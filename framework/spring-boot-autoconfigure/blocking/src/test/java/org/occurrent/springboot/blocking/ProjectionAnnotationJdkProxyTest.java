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
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR 127 section 4: a {@code @Projection} factory is invoked on whatever bean the context hands back for its
 * declaring name, which is a JDK interface proxy rather than {@link ProjectionHolder} itself once the bean is
 * {@code @Lazy} and advised under {@code spring.aop.proxy-target-class=false}. The bean post processor's scan runs
 * before the bean is created, so it predicts {@link ProjectionHolder} from the factory method's declared return type
 * and finds the {@code @Projection} method there; only the later {@code getBean} call, which triggers creation,
 * hands back the proxy. Before {@link SubscriptionAnnotations#invokeDescriptorFactory} unwrapped to the proxy's
 * target first, invoking that method on the proxy threw {@link IllegalArgumentException}, because the proxy is
 * never an instance of {@link ProjectionHolder}. Container-free: an empty domain-feed reader is all a
 * {@code source = PUSH} projection needs to register without Docker.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationJdkProxyTest {

    private static final AtomicInteger FACTORY_INVOCATIONS = new AtomicInteger();

    @Test
    void a_projection_factory_on_a_jdk_interface_proxied_lazy_bean_registers_instead_of_failing_startup() {
        FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withPropertyValues("spring.aop.proxy-target-class=false")
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(DomainFeedConfiguration.class, JdkProxyPostProcessorConfiguration.class, LazyProjectionConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(FACTORY_INVOCATIONS).hasValue(1);
                });
    }

    // Implemented by the bean the factory method declares to return, and nothing else, so a JDK proxy of it can
    // never carry the @Projection method: that mismatch is the whole mechanism under test.
    interface Marker {
    }

    static class ProjectionHolder implements Marker {
        @Projection(id = "jdk-proxy-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyProjectionConfiguration {
        // @Lazy is what keeps this bean uncreated when the registrar's scan runs, so the scan predicts
        // ProjectionHolder from the factory method's return type rather than seeing an already-created proxy.
        @Lazy
        @Bean
        ProjectionHolder projectionHolder() {
            return new ProjectionHolder();
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
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DomainFeedConfiguration {
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

        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader emptyReader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.empty();
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(emptyReader, converter, event -> "k");
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }
    }

    record TestEvent() {
    }
}
