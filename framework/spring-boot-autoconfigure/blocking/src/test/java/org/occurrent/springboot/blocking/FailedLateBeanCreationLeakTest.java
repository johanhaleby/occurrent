/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.blocking;

import jakarta.annotation.PostConstruct;
import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Spring calls no post-processor when a bean's initialization throws, so what the annotation post-processor recorded
 * for a bean built after startup has to be released without it. Container-free, with an event store that reports a
 * start boundary, since the boundary is only read and recorded when there is one.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class FailedLateBeanCreationLeakTest {

    private static final int FAILED_CREATIONS = 20;
    private static final long GC_DEADLINE_NANOS = 10_000_000_000L;

    private static final AtomicInteger BOUNDARY_READS = new AtomicInteger();
    private static final List<WeakReference<Object>> CREATED_INSTANCES = new CopyOnWriteArrayList<>();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(LateBeanConfiguration.class);

    @BeforeEach
    void reset() {
        BOUNDARY_READS.set(0);
        CREATED_INSTANCES.clear();
    }

    @Test
    void a_prototype_whose_creation_failed_after_startup_can_be_garbage_collected() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            int readsAtStartup = BOUNDARY_READS.get();
            context.getBean("workingLateSubscriber");
            assertThat(BOUNDARY_READS).describedAs("a late bean of the kind reads the boundary it records, so the failing creations below record one too").hasValue(readsAtStartup + 1);
            verify(context.getBean(Subscriptions.class)).subscribe(eq("working-late-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));

            CREATED_INSTANCES.clear();
            for (int i = 0; i < FAILED_CREATIONS; i++) {
                assertThatThrownBy(() -> context.getBean("failingLateSubscriber")).hasRootCauseMessage("initialization failed");
            }

            assertThat(CREATED_INSTANCES).hasSize(FAILED_CREATIONS);
            assertThat(BOUNDARY_READS).describedAs("every failed creation read a boundary").hasValue(readsAtStartup + 1 + FAILED_CREATIONS);
            assertThat(uncollectedAfterGc()).describedAs("failed instances still reachable").isZero();
        });
    }

    private static long uncollectedAfterGc() {
        long deadline = System.nanoTime() + GC_DEADLINE_NANOS;
        long uncollected;
        while ((uncollected = CREATED_INSTANCES.stream().filter(reference -> reference.get() != null).count()) > 0 && System.nanoTime() < deadline) {
            System.gc();
            byte[] pressure = new byte[1024 * 1024];
            pressure[0] = 1;
        }
        return uncollected;
    }

    interface LateMarker {
    }

    static class WorkingLateSubscriber implements LateMarker {
        WorkingLateSubscriber() {
            CREATED_INSTANCES.add(new WeakReference<>(this));
        }

        @Subscription(id = "working-late-handler")
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class FailingLateSubscriber implements LateMarker {
        FailingLateSubscriber() {
            CREATED_INSTANCES.add(new WeakReference<>(this));
        }

        @PostConstruct
        void failToInitialize() {
            throw new IllegalStateException("initialization failed");
        }

        @Subscription(id = "failing-late-handler")
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LateBeanConfiguration {
        @Bean
        CloudEventConverter<LateBeanAnnotationRegistrationTest.TestEvent> testEventCloudEventConverter() {
            return new LateBeanAnnotationRegistrationTest.NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<LateBeanAnnotationRegistrationTest.TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        EventStoreQueries eventStore() {
            EventStoreQueries store = mock(EventStoreQueries.class, withSettings().extraInterfaces(CatchupReads.class));
            CatchupReadScope scope = mock(CatchupReadScope.class);
            when(scope.liveStart()).thenAnswer(invocation -> {
                BOUNDARY_READS.incrementAndGet();
                return "1";
            });
            when(((CatchupReads) store).openCatchupReadScope(any())).thenReturn(scope);
            return store;
        }

        // Declared as the marker, so the startup scan cannot see the handlers and the beans are built when asked for
        @Bean
        @Scope("prototype")
        LateMarker workingLateSubscriber() {
            return new WorkingLateSubscriber();
        }

        @Bean
        @Scope("prototype")
        LateMarker failingLateSubscriber() {
            return new FailingLateSubscriber();
        }
    }
}
