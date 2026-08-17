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
import org.occurrent.annotation.Catchup;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@link Saga} twin of {@link ProjectionAnnotationJdkProxyTest}: see that class for the mechanism ADR 127
 * section 4 describes. {@code source = PUSH, catchup = NONE} is what lets this run without Docker, the same reason
 * {@code SagaAnnotationPushWithoutCatchupTest} can.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationJdkProxyTest {

    @Test
    void a_saga_factory_on_a_jdk_interface_proxied_lazy_bean_registers_instead_of_failing_startup() {
        new ApplicationContextRunner()
                .withPropertyValues("spring.aop.proxy-target-class=false")
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(PushInfrastructureConfiguration.class, JdkProxyPostProcessorConfiguration.class, LazySagaConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasBean(SagaAnnotationRegistrar.sagaInstancesBeanName("jdk-proxy-saga"));
                });
    }

    // Implemented by the bean the factory method declares to return, and nothing else, so a JDK proxy of it can
    // never carry the @Saga method: that mismatch is the whole mechanism under test.
    interface Marker {
    }

    sealed interface OrderEvent {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record OrderState(String orderId) {
    }

    static class SagaHolder implements Marker {
        @Saga(id = "jdk-proxy-saga", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, event) -> new OrderState(event.orderId()))
                    .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.issue(new ShipOrder(event.orderId()))))
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazySagaConfiguration {
        // @Lazy is what keeps this bean uncreated when the registrar's scan runs, so the scan predicts SagaHolder
        // from the factory method's return type rather than seeing an already-created proxy.
        @Lazy
        @Bean
        SagaHolder sagaHolder() {
            return new SagaHolder();
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
    static class PushInfrastructureConfiguration {
        @Bean
        CloudEventConverter<OrderEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                    return CloudEventBuilder.v1()
                            .withId("id")
                            .withSource(URI.create("urn:test"))
                            .withType(domainEvent.getClass().getSimpleName())
                            .withSubject(domainEvent.orderId())
                            .build();
                }

                @Override
                public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new OrderPlaced(cloudEvent.getSubject());
                }

                @Override
                public String getCloudEventType(Class<? extends OrderEvent> type) {
                    return type.getSimpleName();
                }
            };
        }

        @Bean
        OccurrentProperties occurrentProperties() {
            return new OccurrentProperties();
        }

        @Bean
        SagaInstancesRegistryImpl sagaInstancesRegistry() {
            return new SagaInstancesRegistryImpl();
        }

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        SagaStateStore<OrderState> sagaStateStore() {
            return SagaStateStore.inMemory();
        }

        @Bean
        CommandDispatcher<OrderCommand> commandDispatcher() {
            List<OrderCommand> issued = new CopyOnWriteArrayList<>();
            return issued::add;
        }
    }
}
