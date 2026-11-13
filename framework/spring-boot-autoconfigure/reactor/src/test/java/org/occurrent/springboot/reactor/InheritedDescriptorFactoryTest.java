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

package org.occurrent.springboot.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

/**
 * A descriptor factory ({@code @Projection} or {@code @Snapshot} on a no-argument method) declared on
 * a superclass and inherited unchanged by the bean's class runs and registers, the way one the class declares itself
 * does (issue #983). {@link SubscriptionAnnotations#invokeDescriptorFactory} refuses a target that overrides the
 * method or is not the scanned class at all, which {@code DescriptorFactorySwitchingPrototypeTest} and
 * {@code SubscriptionAnnotationsTest} cover. Reproduced without a running store (no Docker), each with the
 * container-free setup the corresponding {@code ...JdkProxyTest} uses.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InheritedDescriptorFactoryTest {

    private static final AtomicInteger PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger SNAPSHOT_FACTORY_INVOCATIONS = new AtomicInteger();

    @Test
    void a_projection_factory_declared_on_a_superclass_runs_for_a_bean_that_inherits_it() {
        PROJECTION_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ProjectionInfrastructureConfiguration.class, InheritedProjectionConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(PROJECTION_FACTORY_INVOCATIONS).hasValue(1);
                });
    }

    @Test
    void a_snapshot_factory_declared_on_a_superclass_runs_and_subscribes_for_a_bean_that_inherits_it() {
        SNAPSHOT_FACTORY_INVOCATIONS.set(0);
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SnapshotInfrastructureConfiguration.class, InheritedSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(SNAPSHOT_FACTORY_INVOCATIONS).hasValue(1);
                    assertThat(mockingDetails(context.getBean(Subscriptions.class)).getInvocations())
                            .anyMatch(invocation -> invocation.getMethod().getName().equals("subscribe") && "inherited-snapshot".equals(invocation.getRawArguments()[0]));
                });
    }

    abstract static class BaseProjectionFactory {
        @Projection(id = "inherited-projection-reactive", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            PROJECTION_FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    static class InheritingProjectionFactory extends BaseProjectionFactory {
    }

    abstract static class BaseSnapshotFactory {
        @Snapshot(id = "inherited-snapshot", startAt = StartPosition.NOW)
        SnapshotView<TestState, TestEvent> snapshot() {
            SNAPSHOT_FACTORY_INVOCATIONS.incrementAndGet();
            return SnapshotView.<TestState, TestEvent>builder(new TestState())
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    static class InheritingSnapshotFactory extends BaseSnapshotFactory {
    }

    @Configuration(proxyBeanMethods = false)
    static class InheritedProjectionConfiguration {
        @Bean
        InheritingProjectionFactory projectionFactory() {
            return new InheritingProjectionFactory();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InheritedSnapshotConfiguration {
        @Bean
        InheritingSnapshotFactory snapshotFactory() {
            return new InheritingSnapshotFactory();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ProjectionInfrastructureConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        // The reactive post processor skips annotation processing without a Subscribable bean, and a DomainEventFeed is not one.
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader emptyReader = new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.empty();
                }

                @Override
                public Mono<Long> currentPosition() {
                    return Mono.just(0L);
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
            return ViewStateRepository.create(store::get, (id, value) -> store.put(id, value));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SnapshotInfrastructureConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        @SuppressWarnings("unchecked")
        @Bean
        ReactiveSnapshotStore<TestState> reactiveSnapshotStore() {
            return mock(ReactiveSnapshotStore.class);
        }

        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        EventStore eventStore() {
            return mock(EventStore.class);
        }

        @Bean
        OccurrentProperties occurrentProperties() {
            return new OccurrentProperties();
        }

        @SuppressWarnings("unchecked")
        @Bean
        Subscriptions<TestEvent> subscriptions() {
            Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
            org.occurrent.subscription.api.reactor.Subscription subscription = mock(org.occurrent.subscription.api.reactor.Subscription.class);
            when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
            when(subscriptions.subscribe(any(String.class), any(org.occurrent.subscription.AgnosticSubscriptionFilter.class),
                    any(org.occurrent.subscription.StartAt.class), any(kotlin.jvm.functions.Function2.class))).thenReturn(subscription);
            return subscriptions;
        }
    }

    static class TestEventConverter implements CloudEventConverter<TestEvent> {
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
    }

    record TestState() {
    }

    record TestEvent() {
    }
}
