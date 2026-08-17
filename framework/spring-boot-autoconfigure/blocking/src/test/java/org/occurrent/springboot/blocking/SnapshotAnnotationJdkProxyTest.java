/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.springboot.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Snapshot;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The {@link Snapshot} twin of {@link ProjectionAnnotationJdkProxyTest}: see that class for the mechanism ADR 127
 * section 4 describes. A mocked {@link SnapshotStore} is all a {@code @Snapshot} registration needs, the same
 * container-free setup {@code SnapshotFilterExpansionTest} uses.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotAnnotationJdkProxyTest {

    private static final AtomicInteger FACTORY_INVOCATIONS = new AtomicInteger();

    @Test
    void a_snapshot_factory_on_a_jdk_interface_proxied_lazy_bean_registers_instead_of_failing_startup() {
        FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withPropertyValues("spring.aop.proxy-target-class=false")
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(StoreConfiguration.class, JdkProxyPostProcessorConfiguration.class, LazySnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(FACTORY_INVOCATIONS).hasValue(1);
                    // Confirms the fixture still exercises a real JDK interface proxy rather than degrading into
                    // an ordinary bean that would pass this test for the wrong reason.
                    assertThat(AopUtils.isJdkDynamicProxy(context.getBean("snapshotHolder"))).isTrue();
                });
    }

    // Implemented by the bean the factory method declares to return, and nothing else, so a JDK proxy of it can
    // never carry the @Snapshot method: that mismatch is the whole mechanism under test.
    interface Marker {
    }

    static class SnapshotHolder implements Marker {
        @Snapshot(id = "jdk-proxy-snapshot")
        SnapshotView<TestState, TestEvent> snapshot() {
            FACTORY_INVOCATIONS.incrementAndGet();
            return SnapshotView.<TestState, TestEvent>builder(new TestState())
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazySnapshotConfiguration {
        // @Lazy is what keeps this bean uncreated when the registrar's scan runs, so the scan predicts
        // SnapshotHolder from the factory method's return type rather than seeing an already-created proxy.
        @Lazy
        @Bean
        SnapshotHolder snapshotHolder() {
            return new SnapshotHolder();
        }
    }

    // Wraps any Marker bean in a genuine JDK interface proxy once it is created: an Advised proxy backed by a
    // SingletonTargetSource, implementing only Marker, the same shape a real advisor leaves an interface-proxied
    // bean in under spring.aop.proxy-target-class=false.
    @Configuration(proxyBeanMethods = false)
    static class JdkProxyPostProcessorConfiguration {
        @Bean
        static BeanPostProcessor jdkInterfaceProxyPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof Marker)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setInterfaces(Marker.class);
                    proxyFactory.setProxyTargetClass(false);
                    proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class StoreConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("TestEvent").build();
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new TestEvent();
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }

        @SuppressWarnings("unchecked")
        @Bean
        SnapshotStore<TestState> snapshotStore() {
            return mock(SnapshotStore.class);
        }

        // The redelivery-detection head probe resolves this by type once a snapshot exists to compare against;
        // never reached here since the store mock always reports no existing snapshot for a given key.
        @Bean
        EventStore eventStore() {
            return mock(EventStore.class);
        }

        @Bean
        OccurrentProperties occurrentProperties() {
            return new OccurrentProperties();
        }

        // Default (non-synchronous, non-stream) registration resolves this by type to subscribe.
        @SuppressWarnings("unchecked")
        @Bean
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }
    }

    record TestState() {
    }

    record TestEvent() {
    }
}
