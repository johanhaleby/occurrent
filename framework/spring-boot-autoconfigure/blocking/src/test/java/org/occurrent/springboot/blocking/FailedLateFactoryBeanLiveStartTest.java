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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.GlobalCheckpoint;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Spring calls no post-processor when a FactoryBean's initialization or its {@code getObject} throws, so the boundary
 * the failed creation read is not taken by anything. That boundary must not reach a later creation, and must not
 * outlive the thread that read it. Container-free, with an event store whose boundary the test moves.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class FailedLateFactoryBeanLiveStartTest {

    private static final long GC_DEADLINE_NANOS = 10_000_000_000L;

    private static final AtomicLong BOUNDARY = new AtomicLong();
    private static final AtomicInteger ATTEMPTS = new AtomicInteger();
    private static final List<WeakReference<String>> BOUNDARIES_READ = new CopyOnWriteArrayList<>();
    private static volatile FailsAt failsAt;

    enum FailsAt {
        INITIALIZATION, GET_OBJECT
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(FailingFactoryConfiguration.class);

    @BeforeEach
    void reset() {
        BOUNDARY.set(1);
        ATTEMPTS.set(0);
        BOUNDARIES_READ.clear();
    }

    @ParameterizedTest
    @EnumSource(FailsAt.class)
    void a_later_creation_under_the_same_name_on_the_same_thread_registers_from_its_own_boundary_not_the_one_the_failed_creation_read(FailsAt point) {
        failsAt = point;
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            assertThatThrownBy(() -> context.getBean("failingOnceFactory")).hasRootCauseMessage("creation failed");
            BOUNDARY.set(2);
            context.getBean("failingOnceFactory");

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "failing-once-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    @ParameterizedTest
    @EnumSource(FailsAt.class)
    void a_creation_that_failed_on_a_thread_that_ended_leaves_no_boundary_for_another_thread_to_take_and_none_reachable(FailsAt point) {
        failsAt = point;
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARIES_READ.clear();
            runOnAThreadThatEnds(() -> assertThatThrownBy(() -> context.getBean("failingOnceFactory")).hasRootCauseMessage("creation failed"));
            assertThat(BOUNDARIES_READ).describedAs("the failed creation read one boundary").hasSize(1);
            assertThat(uncollectedAfterGc()).describedAs("boundaries of the failed creation still reachable after its thread ended").isZero();

            BOUNDARY.set(2);
            context.getBean("failingOnceFactory");
            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "failing-once-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    private static void runOnAThreadThatEnds(Runnable work) throws InterruptedException {
        Thread thread = new Thread(work, "failing-creation");
        thread.start();
        thread.join();
        assertThat(thread.isAlive()).isFalse();
    }

    private static long uncollectedAfterGc() {
        long deadline = System.nanoTime() + GC_DEADLINE_NANOS;
        long uncollected;
        while ((uncollected = BOUNDARIES_READ.stream().filter(reference -> reference.get() != null).count()) > 0 && System.nanoTime() < deadline) {
            System.gc();
            byte[] pressure = new byte[1024 * 1024];
            pressure[0] = 1;
        }
        return uncollected;
    }

    interface LateMarker {
    }

    static class FailingOnceProduct implements LateMarker {
        @Subscription(id = "failing-once-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    // Fails its first creation, at the point the test chose, and every later one succeeds
    static class FailingOnceFactory implements FactoryBean<LateMarker>, InitializingBean {
        private final boolean failsThisCreation = ATTEMPTS.getAndIncrement() == 0;

        @Override
        public void afterPropertiesSet() {
            if (failsThisCreation && failsAt == FailsAt.INITIALIZATION) {
                throw new IllegalStateException("creation failed");
            }
        }

        @Override
        public LateMarker getObject() {
            if (failsThisCreation && failsAt == FailsAt.GET_OBJECT) {
                throw new IllegalStateException("creation failed");
            }
            return new FailingOnceProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return LateMarker.class;
        }

        @Override
        public boolean isSingleton() {
            return false;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FailingFactoryConfiguration {
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
        LiveStartCheckpoints liveStartCheckpoints() {
            return liveStart -> GlobalCheckpoint.of(Long.parseLong(liveStart));
        }

        @Bean
        EventStoreQueries eventStore() {
            EventStoreQueries store = mock(EventStoreQueries.class, withSettings().extraInterfaces(CatchupReads.class));
            CatchupReadScope scope = mock(CatchupReadScope.class);
            when(scope.liveStart()).thenAnswer(invocation -> {
                String liveStart = new String(String.valueOf(BOUNDARY.get()));
                BOUNDARIES_READ.add(new WeakReference<>(liveStart));
                return liveStart;
            });
            when(((CatchupReads) store).openCatchupReadScope(any())).thenReturn(scope);
            return store;
        }

        @Bean
        @Scope("prototype")
        FailingOnceFactory failingOnceFactory() {
            return new FailingOnceFactory();
        }
    }
}
