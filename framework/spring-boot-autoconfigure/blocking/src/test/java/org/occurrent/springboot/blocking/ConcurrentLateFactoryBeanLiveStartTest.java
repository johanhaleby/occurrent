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

import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A FactoryBean built after startup reads the start boundary of its product, and the product takes that boundary as
 * the live start of the subscription it registers. Container-free, with an event store whose boundary the test moves.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ConcurrentLateFactoryBeanLiveStartTest {

    private static final long AWAIT_SECONDS = 20;
    private static final SubscriptionModelContext ANY_MODEL = new SubscriptionModelContext(Object.class);

    private static final AtomicLong BOUNDARY = new AtomicLong();
    private static final AtomicInteger FACTORIES_CREATED = new AtomicInteger();
    private static final CountDownLatch[] REACHED_INIT = latches(3);
    private static final CountDownLatch[] RELEASE_INIT = latches(3);

    private final ExecutorService threads = Executors.newFixedThreadPool(2);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(LateFactoryConfiguration.class);

    @BeforeEach
    void reset() {
        BOUNDARY.set(1);
        FACTORIES_CREATED.set(0);
        for (int i = 0; i < REACHED_INIT.length; i++) {
            REACHED_INIT[i] = new CountDownLatch(1);
            RELEASE_INIT[i] = new CountDownLatch(1);
        }
    }

    @AfterEach
    void releaseEveryone() {
        for (CountDownLatch release : RELEASE_INIT) {
            release.countDown();
        }
        threads.shutdownNow();
    }

    @Test
    void a_prototype_factory_bean_product_registers_from_the_boundary_its_own_factory_read_when_another_creation_read_a_later_one() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            Future<Object> first = threads.submit(() -> context.getBean("gatedPrototypeFactory"));
            assertThat(REACHED_INIT[1].await(AWAIT_SECONDS, TimeUnit.SECONDS)).describedAs("first factory reached its initialization").isTrue();
            BOUNDARY.set(2);
            Future<Object> second = threads.submit(() -> context.getBean("gatedPrototypeFactory"));
            assertThat(REACHED_INIT[2].await(AWAIT_SECONDS, TimeUnit.SECONDS)).describedAs("second factory reached its initialization").isTrue();

            RELEASE_INIT[1].countDown();
            first.get(AWAIT_SECONDS, TimeUnit.SECONDS);
            RELEASE_INIT[2].countDown();
            second.get(AWAIT_SECONDS, TimeUnit.SECONDS);

            assertThat(startOfLateRegistration(context, "gated-prototype-handler")).isEqualTo(GlobalCheckpoint.of(1));
        });
    }

    @Test
    void a_singleton_factory_bean_product_registers_from_the_boundary_its_factory_read_when_another_thread_asks_for_it_later() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            RELEASE_INIT[1].countDown();
            threads.submit(() -> context.getBean("&gatedSingletonFactory")).get(AWAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(FACTORIES_CREATED).describedAs("the singleton factory was created after startup, by the request for the factory").hasValue(1);
            BOUNDARY.set(2);
            threads.submit(() -> context.getBean("gatedSingletonFactory")).get(AWAIT_SECONDS, TimeUnit.SECONDS);

            assertThat(startOfLateRegistration(context, "gated-singleton-handler")).isEqualTo(GlobalCheckpoint.of(1));
        });
    }

    @SuppressWarnings("unchecked")
    static Checkpoint startOfLateRegistration(ApplicationContext context, String subscriptionId) {
        ArgumentCaptor<StartAt> startAt = ArgumentCaptor.forClass(StartAt.class);
        verify(context.getBean(Subscriptions.class)).subscribe(eq(subscriptionId), any(AgnosticSubscriptionFilter.class), startAt.capture(), anyBoolean(), any(Function2.class));
        StartAt resolved = startAt.getValue().get(ANY_MODEL);
        assertThat(resolved).isInstanceOf(StartAt.StartAtCheckpoint.class);
        return ((StartAt.StartAtCheckpoint) resolved).checkpoint;
    }

    private static CountDownLatch[] latches(int count) {
        CountDownLatch[] latches = new CountDownLatch[count];
        for (int i = 0; i < count; i++) {
            latches[i] = new CountDownLatch(1);
        }
        return latches;
    }

    interface LateMarker {
    }

    static class PrototypeProduct implements LateMarker {
        @Subscription(id = "gated-prototype-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class SingletonProduct implements LateMarker {
        @Subscription(id = "gated-singleton-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    // Initialization runs after the post-processor's callback has read the boundary, so it holds the creation there
    static class GatedFactory implements FactoryBean<LateMarker>, InitializingBean {
        private final boolean singleton;
        private int number;

        GatedFactory(boolean singleton) {
            this.singleton = singleton;
        }

        @Override
        public void afterPropertiesSet() throws Exception {
            number = FACTORIES_CREATED.incrementAndGet();
            REACHED_INIT[number].countDown();
            if (!RELEASE_INIT[number].await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("factory " + number + " was never released");
            }
        }

        @Override
        public LateMarker getObject() {
            return singleton ? new SingletonProduct() : new PrototypeProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return LateMarker.class;
        }

        @Override
        public boolean isSingleton() {
            return singleton;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LateFactoryConfiguration {
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
            when(scope.liveStart()).thenAnswer(invocation -> String.valueOf(BOUNDARY.get()));
            when(((CatchupReads) store).openCatchupReadScope(any())).thenReturn(scope);
            return store;
        }

        @Bean
        @Scope("prototype")
        GatedFactory gatedPrototypeFactory() {
            return new GatedFactory(false);
        }

        @Bean
        @Lazy
        GatedFactory gatedSingletonFactory() {
            return new GatedFactory(true);
        }
    }
}
