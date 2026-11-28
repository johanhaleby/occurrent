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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * A bean that passed the starter's callback before its initialization while the startup gate was closed, and is
 * initialized after the gate opened, makes its write during initialization and then fails its creation. Container-free,
 * with an application service that admits a write the way the starters' write gates do, once the post-processor says so.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StartupWriteAdmissionAfterInitializationTest {

    private static final long AWAIT_SECONDS = 20;

    private static final AtomicInteger CREATIONS = new AtomicInteger();
    private static final CountDownLatch PARKED = new CountDownLatch(1);
    private static final CountDownLatch RELEASE = new CountDownLatch(1);
    private static final CountDownLatch FINISHED = new CountDownLatch(1);
    private static final AtomicReference<Object> OUTCOME = new AtomicReference<>();
    private static final List<Object> STORED_WRITES = new CopyOnWriteArrayList<>();

    @BeforeEach
    void reset() {
        CREATIONS.set(0);
        OUTCOME.set(null);
        STORED_WRITES.clear();
    }

    @AfterEach
    void releaseEveryone() {
        RELEASE.countDown();
    }

    @Test
    void a_bean_initialized_after_the_gate_opened_stores_its_write_and_fails_with_the_refusal_that_says_the_handler_misses_it() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(LateBeanConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(OccurrentBlockingAnnotationBeanPostProcessor.class).admitsWrite()).describedAs("the startup scan opened the gate while the other thread was parked").isTrue();
                    assertThat(PARKED.getCount()).describedAs("the other thread was parked after the starter's callback").isZero();

                    RELEASE.countDown();
                    assertThat(FINISHED.await(AWAIT_SECONDS, TimeUnit.SECONDS)).describedAs("the other thread finished the creation").isTrue();

                    assertThat(OUTCOME.get()).isInstanceOf(Throwable.class);
                    assertThat((Throwable) OUTCOME.get()).rootCause().hasMessageContaining("may already be stored, and the handler won't get that write after the bean is fixed either");
                    assertThat(STORED_WRITES).containsExactly(WRITE);
                    verify(context.getBean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class), never())
                            .subscribe(eq("late-synchronous-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
                });
    }

    private static final LateBeanAnnotationRegistrationTest.TestEvent WRITE = new LateBeanAnnotationRegistrationTest.TestEvent();

    interface LateMarker {
    }

    // Admits a write once the post-processor has opened its gate
    static class ApplicationService {
        private final ObjectProvider<OccurrentBlockingAnnotationBeanPostProcessor> postProcessor;

        ApplicationService(ObjectProvider<OccurrentBlockingAnnotationBeanPostProcessor> postProcessor) {
            this.postProcessor = postProcessor;
        }

        void write(Object event) {
            if (!postProcessor.getObject().admitsWrite()) {
                throw new IllegalStateException("The write was refused, the startup gate is closed");
            }
            STORED_WRITES.add(event);
        }
    }

    static class LateReceiver implements LateMarker {
        private final ApplicationService service;

        LateReceiver(ApplicationService service) {
            this.service = service;
        }

        @PostConstruct
        void writeDuringInitialization() {
            service.write(WRITE);
        }

        @SynchronousSubscription(id = "late-synchronous-handler")
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class PlainShell implements LateMarker {
    }

    // Runs after the starter's callback and before the bean's @PostConstruct method, and holds the creation there
    static class ParksTheLateReceiver implements BeanPostProcessor {
        @Override
        public Object postProcessBeforeInitialization(Object bean, String beanName) {
            if (bean instanceof LateReceiver) {
                PARKED.countDown();
                try {
                    if (!RELEASE.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The creation was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return bean;
        }
    }

    // Created by the startup thread, so it runs while the gate is closed. It starts the creation on another thread,
    // waits until that is parked, and then builds a second instance of another class on the startup thread. The class
    // the starter recorded for the name is then one without a handler, so the startup scan never registers the
    // late receiver's handler.
    static class StartsTheCreation {
        StartsTheCreation(BeanFactory beanFactory) throws InterruptedException {
            Thread creating = new Thread(() -> {
                try {
                    OUTCOME.set(beanFactory.getBean("lateReceiver"));
                } catch (Throwable e) {
                    OUTCOME.set(e);
                }
                FINISHED.countDown();
            }, "late-receiver-creation");
            creating.setDaemon(true);
            creating.start();
            if (!PARKED.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("The creation was never parked");
            }
            beanFactory.getBean("lateReceiver");
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

        @Bean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @SuppressWarnings("unchecked")
        Subscriptions<LateBeanAnnotationRegistrationTest.TestEvent> synchronousSubscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        ApplicationService applicationService(ObjectProvider<OccurrentBlockingAnnotationBeanPostProcessor> postProcessor) {
            return new ApplicationService(postProcessor);
        }

        @Bean
        static ParksTheLateReceiver parksTheLateReceiver() {
            return new ParksTheLateReceiver();
        }

        // Declared as the marker, so the startup scan cannot see the handler and the first instance is the late receiver
        @Bean
        @Scope("prototype")
        LateMarker lateReceiver(ApplicationService service) {
            return CREATIONS.getAndIncrement() == 0 ? new LateReceiver(service) : new PlainShell();
        }

        @Bean
        StartsTheCreation startsTheCreation(BeanFactory beanFactory) throws InterruptedException {
            return new StartsTheCreation(beanFactory);
        }
    }
}
