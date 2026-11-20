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
package org.occurrent.springboot.reactor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.GlobalCheckpoint;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A prototype FactoryBean created before the startup scan ended, pinned by the boundary its product's handler registers
 * from. The context starts at boundary 1.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PrototypeFactoryBuiltBeforeScanLiveStartTest {

    private static final long AWAIT_SECONDS = 20;
    private static final long STARTUP_BOUNDARY = 1;

    private static final AtomicLong BOUNDARY = new AtomicLong();
    private static final AtomicInteger RACING_FACTORIES = new AtomicInteger();
    private static final AtomicInteger HOLDING_PRODUCTS = new AtomicInteger();
    private static final AtomicReference<FutureTask<Object>> REQUEST = new AtomicReference<>();
    // Keeps the product built during startup, and with it its factory, reachable for the whole test
    private static final List<Object> STARTUP_PRODUCTS = new CopyOnWriteArrayList<>();
    private static volatile CountDownLatch factoryInitialized = new CountDownLatch(1);
    private static volatile CountDownLatch releaseFactoryInitialization = new CountDownLatch(1);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(PrototypeFactoryConfiguration.class);

    @BeforeEach
    void reset() {
        BOUNDARY.set(STARTUP_BOUNDARY);
        RACING_FACTORIES.set(0);
        HOLDING_PRODUCTS.set(0);
        REQUEST.set(null);
        STARTUP_PRODUCTS.clear();
        factoryInitialized = new CountDownLatch(1);
        releaseFactoryInitialization = new CountDownLatch(1);
    }

    @AfterEach
    void releaseTheRequestingThread() {
        releaseFactoryInitialization.countDown();
    }

    /**
     * A thread asks for the factory's product while the context starts, and the factory's initialization holds until the
     * context has started. Its getObject then writes, which moves the boundary, and asks for the factory itself, which
     * builds another factory of the name that reads a boundary after that write.
     */
    @Test
    void a_product_whose_prototype_factory_was_created_before_the_scan_ended_registers_from_the_boundary_of_the_context_when_its_getObject_builds_another_factory_of_its_name_after_a_write() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            releaseFactoryInitialization.countDown();
            REQUEST.get().get(AWAIT_SECONDS, TimeUnit.SECONDS);

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "racing-handler")).isEqualTo(GlobalCheckpoint.of(STARTUP_BOUNDARY));
        });
    }

    /**
     * The first product is built on the starting thread before the scan ends and holds its factory. The second is built
     * on the same thread after the context has started, and its factory reads the boundary then.
     */
    @Test
    void a_product_built_after_startup_registers_from_its_own_factorys_boundary_when_an_earlier_product_of_the_name_was_built_before_the_scan_ended_on_the_same_thread() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(STARTUP_PRODUCTS).describedAs("products built during startup").hasSize(1);

            BOUNDARY.set(2);
            context.getBean("holdingFactory");

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "holding-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    interface LateMarker {
    }

    static class RacingHandler implements LateMarker {
        @Subscription(id = "racing-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class HoldingHandler implements LateMarker {
        @Subscription(id = "holding-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class HoldingProduct implements LateMarker {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final Object factory;

        HoldingProduct(Object factory) {
            this.factory = factory;
        }
    }

    // The first factory holds in its initialization, which runs after the post-processors' before-initialization callback
    static class RacingFactory implements FactoryBean<LateMarker>, InitializingBean, BeanFactoryAware {
        private final boolean first = RACING_FACTORIES.getAndIncrement() == 0;
        private BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public void afterPropertiesSet() throws InterruptedException {
            if (first) {
                factoryInitialized.countDown();
                if (!releaseFactoryInitialization.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the factory's initialization was never released");
                }
            }
        }

        @Override
        public LateMarker getObject() {
            BOUNDARY.set(BOUNDARY.get() + 2);
            beanFactory.getBean(BeanFactory.FACTORY_BEAN_PREFIX + "racingFactory");
            return new RacingHandler();
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

    static class HoldingFactory implements FactoryBean<LateMarker> {
        @Override
        public LateMarker getObject() {
            if (HOLDING_PRODUCTS.incrementAndGet() == 1) {
                HoldingProduct product = new HoldingProduct(this);
                STARTUP_PRODUCTS.add(product);
                return product;
            }
            return new HoldingHandler();
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

    // Asks for products while the context starts, one on a thread of its own and one on the starting thread
    static class StartupRequester implements BeanFactoryAware, InitializingBean {
        private BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public void afterPropertiesSet() throws InterruptedException {
            FutureTask<Object> request = new FutureTask<>(() -> beanFactory.getBean("racingFactory"));
            REQUEST.set(request);
            Thread thread = new Thread(request, "startup-requester");
            thread.setDaemon(true);
            thread.start();
            if (!factoryInitialized.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the factory was never created");
            }
            beanFactory.getBean("holdingFactory");
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PrototypeFactoryConfiguration {
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
        EventStore eventStore() {
            EventStore store = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class, CatchupReads.class));
            when(((CatchupReads) store).openCatchupReadScope(any())).thenAnswer(invocation ->
                    Mono.just(mock(CatchupReadScope.class, scopeCall -> switch (scopeCall.getMethod().getName()) {
                        case "liveStart" -> String.valueOf(BOUNDARY.get());
                        case "close" -> Mono.empty();
                        default -> null;
                    })));
            return store;
        }

        @Bean
        @Scope("prototype")
        RacingFactory racingFactory() {
            return new RacingFactory();
        }

        @Bean
        @Scope("prototype")
        HoldingFactory holdingFactory() {
            return new HoldingFactory();
        }

        @Bean
        StartupRequester startupRequester() {
            return new StartupRequester();
        }
    }
}
