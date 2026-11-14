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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Catchup;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A {@code @Projection}, {@code @Snapshot} or {@code @Saga} factory on a scoped bean runs once, on the instance the
 * scope held when the scan read it, never through the scoped proxy (issue #1492). A proxy of
 * {@code ScopedProxyMode.INTERFACES} has no factory the class alone declares, so a factory reached through it would be
 * refused. The descriptor is read once at startup, so a later refresh runs it no more. Each descriptor kind reads its
 * instance at its own call site, so each has a test of its own: the target-class mode is covered for
 * {@code @Projection} only, and the interfaces mode for all three. This test needs no Docker, an empty domain-feed
 * reader is all a {@code source = PUSH} projection needs to register, a mocked store all a {@code @Snapshot} needs, and
 * {@code source = PUSH, catchup = NONE} lets a {@code @Saga} register.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ScopedDescriptorFactoryTest {

    private static final List<String> FACTORY_RAN_ON = new CopyOnWriteArrayList<>();
    private static final AtomicInteger INSTANCES = new AtomicInteger();

    private final RefreshableScope scope = new RefreshableScope();
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(DomainFeedConfiguration.class)
            .withInitializer(scope::registerIn);

    @Test
    void a_projection_factory_on_a_target_class_scoped_bean_runs_once_on_the_instance_the_scope_held_at_startup() {
        reset();
        runner.withUserConfiguration(TargetClassConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            scope.refresh();
            context.getBean(TargetClassProjectionHolder.class).toString();

            assertThat(FACTORY_RAN_ON).containsExactly("instance-1");
        });
    }

    @Test
    void a_projection_factory_only_the_class_declares_on_an_interfaces_scoped_bean_runs_on_the_target_instead_of_through_the_proxy() {
        reset();
        runner.withUserConfiguration(InterfacesConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            scope.refresh();
            context.getBean(Marker.class).toString();

            assertThat(FACTORY_RAN_ON).containsExactly("instance-1");
        });
    }

    @Test
    void a_snapshot_factory_only_the_class_declares_on_an_interfaces_scoped_bean_runs_on_the_target_instead_of_through_the_proxy() {
        reset();
        runner.withUserConfiguration(SnapshotInfrastructureConfiguration.class, InterfacesSnapshotConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            scope.refresh();
            context.getBean(Marker.class).toString();

            assertThat(FACTORY_RAN_ON).containsExactly("instance-1");
        });
    }

    @Test
    void a_saga_factory_only_the_class_declares_on_an_interfaces_scoped_bean_runs_on_the_target_instead_of_through_the_proxy() {
        reset();
        runner.withUserConfiguration(SagaInfrastructureConfiguration.class, InterfacesSagaConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            scope.refresh();
            context.getBean(Marker.class).toString();

            assertThat(FACTORY_RAN_ON).containsExactly("instance-1");
        });
    }

    private static void reset() {
        FACTORY_RAN_ON.clear();
        INSTANCES.set(0);
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projectionFrom(String instance) {
        FACTORY_RAN_ON.add(instance);
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    interface Marker {
    }

    public static class TargetClassProjectionHolder {
        private final String instance = "instance-" + INSTANCES.incrementAndGet();

        @Projection(id = "scoped-target-class-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return projectionFrom(instance);
        }
    }

    static class InterfacesProjectionHolder implements Marker {
        private final String instance = "instance-" + INSTANCES.incrementAndGet();

        @Projection(id = "scoped-interfaces-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return projectionFrom(instance);
        }
    }

    static class InterfacesSnapshotHolder implements Marker {
        private final String instance = "instance-" + INSTANCES.incrementAndGet();

        @Snapshot(id = "scoped-interfaces-snapshot")
        SnapshotView<TestState, TestEvent> snapshot() {
            FACTORY_RAN_ON.add(instance);
            return SnapshotView.<TestState, TestEvent>builder(new TestState())
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    static class InterfacesSagaHolder implements Marker {
        private final String instance = "instance-" + INSTANCES.incrementAndGet();

        @Saga(id = "scoped-interfaces-saga", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> saga() {
            FACTORY_RAN_ON.add(instance);
            return org.occurrent.dsl.saga.Saga.<TestEvent, TestState, TestCommand>builder(null)
                    .correlateAll(event -> "k")
                    .startsOn(TestEvent.class)
                    .evolve(TestEvent.class, (state, event) -> new TestState())
                    .react(TestEvent.class, (state, event) -> List.of(SagaEffect.issue(new TestCommand())))
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InterfacesSnapshotConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.INTERFACES)
        InterfacesSnapshotHolder interfacesSnapshotHolder() {
            return new InterfacesSnapshotHolder();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InterfacesSagaConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.INTERFACES)
        InterfacesSagaHolder interfacesSagaHolder() {
            return new InterfacesSagaHolder();
        }
    }

    // A mocked store, event store and Subscriptions are all a @Snapshot registration needs, the container-free setup
    // SnapshotAnnotationJdkProxyTest uses.
    @Configuration(proxyBeanMethods = false)
    static class SnapshotInfrastructureConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        SnapshotStore<TestState> snapshotStore() {
            return mock(SnapshotStore.class);
        }

        @Bean
        EventStore eventStore() {
            return mock(EventStore.class);
        }

        @SuppressWarnings("unchecked")
        @Bean
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }
    }

    // source = PUSH with catchup = NONE is what lets a @Saga register without Docker, as in SagaAnnotationJdkProxyTest.
    @Configuration(proxyBeanMethods = false)
    static class SagaInfrastructureConfiguration {
        @Bean
        SagaInstancesRegistryImpl sagaInstancesRegistry() {
            return new SagaInstancesRegistryImpl();
        }

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        SagaStateStore<TestState> sagaStateStore() {
            return SagaStateStore.inMemory();
        }

        @Bean
        CommandDispatcher<TestCommand> commandDispatcher() {
            List<TestCommand> issued = new CopyOnWriteArrayList<>();
            return issued::add;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TargetClassConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.TARGET_CLASS)
        TargetClassProjectionHolder targetClassProjectionHolder() {
            return new TargetClassProjectionHolder();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InterfacesConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.INTERFACES)
        InterfacesProjectionHolder interfacesProjectionHolder() {
            return new InterfacesProjectionHolder();
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

    record TestState() {
    }

    record TestCommand() {
    }
}
