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
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ADR 127 section 4 unwraps a descriptor factory's bean to its AOP target before invoking the recorded factory
 * method. A prototype-scoped factory bean that hands back a different implementation on a later call is unwrapped to
 * that other implementation's own class, which is not the class {@link SubscriptionAnnotations#invokeDescriptorFactory}
 * recorded the method from. {@link HandlerHolder} is what makes the divergence possible at all, because without a
 * singleton asking for the prototype during startup, the scan never sees a concrete class to record and there is
 * nothing to diverge from. Both implementations declaring {@code projection()} is deliberate, since the same method
 * name is what let a name-based lookup match the wrong implementation before this was fixed. This test needs no
 * Docker. An empty domain-feed reader is all a {@code source = PUSH} projection needs to register.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DescriptorFactorySwitchingPrototypeTest {

    private static final AtomicInteger CHECKED_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger UNCHECKED_INVOCATIONS = new AtomicInteger();

    @Test
    void a_projection_factory_on_a_prototype_that_switches_implementation_refuses_instead_of_running_the_other_implementations_factory() {
        CHECKED_INVOCATIONS.set(0);
        UNCHECKED_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(DomainFeedConfiguration.class, SwitchingPrototypeConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("@Projection")
                            .hasMessageContaining(CheckedProjectionFactory.class.getName())
                            .hasMessageContaining(UncheckedProjectionFactory.class.getName())
                            .hasMessageContaining("different implementation");
                    // Refused before either implementation's factory ran, since the mismatch is caught before invoking.
                    assertThat(CHECKED_INVOCATIONS).hasValue(0);
                    assertThat(UNCHECKED_INVOCATIONS).hasValue(0);
                });
    }

    interface ProjectionFactory {
    }

    static class CheckedProjectionFactory implements ProjectionFactory {
        @Projection(id = "checked-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            CHECKED_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    static class UncheckedProjectionFactory implements ProjectionFactory {
        @Projection(id = "unchecked-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            UNCHECKED_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    record HandlerHolder(ProjectionFactory factory) {
    }

    @Configuration(proxyBeanMethods = false)
    static class SwitchingPrototypeConfiguration {

        private final AtomicInteger instances = new AtomicInteger();

        // Prototype scope is what makes a second getBean call at registration time build a fresh instance instead
        // of reusing the one HandlerHolder already triggered. Wrapping each instance in its own CGLIB proxy is what
        // makes ultimateTarget unwrap to that instance's own class rather than to the proxy itself, so a second
        // resolution unwraps to a genuinely different class than the one the scan recorded.
        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        ProjectionFactory projectionFactory() {
            ProjectionFactory target = instances.getAndIncrement() == 0 ? new CheckedProjectionFactory() : new UncheckedProjectionFactory();
            ProxyFactory proxyFactory = new ProxyFactory();
            proxyFactory.setTarget(target);
            proxyFactory.setProxyTargetClass(true);
            proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
            return (ProjectionFactory) proxyFactory.getProxy();
        }

        // A singleton depending on the prototype during startup is what creates an instance before the registrar's
        // scan runs, so the scan records a concrete class instead of predicting one from the factory method's
        // declared return type.
        @Bean
        HandlerHolder handlerHolder(ProjectionFactory factory) {
            return new HandlerHolder(factory);
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
