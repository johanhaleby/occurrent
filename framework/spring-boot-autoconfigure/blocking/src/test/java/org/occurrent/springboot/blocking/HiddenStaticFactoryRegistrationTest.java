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
import org.junit.jupiter.api.BeforeEach;
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
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * A static {@code @Snapshot}, {@code @Projection} or {@code @Saga} factory that hides the static factory of its
 * superclass with the same name and parameters and repeats the annotation is a factory of its own, so both register,
 * and an id they share fails startup with {@link DuplicateSubscriptionIdException}. The tests use {@code public} and {@code protected} factories, since
 * the tests of the scan in {@code SubscriptionAnnotationsTest} use package-private ones. The projections and sagas
 * use {@code source = PUSH}, and the sagas {@code catchup = NONE}, which register without Docker.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class HiddenStaticFactoryRegistrationTest {

    private static final List<String> RUN = new CopyOnWriteArrayList<>();

    private final ApplicationContextRunner postProcessorRunner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new);
    private final ApplicationContextRunner runner = postProcessorRunner.withUserConfiguration(InfrastructureConfiguration.class);
    private final ApplicationContextRunner projectionRunner = postProcessorRunner.withUserConfiguration(ProjectionInfrastructureConfiguration.class);
    private final ApplicationContextRunner sagaRunner = postProcessorRunner.withUserConfiguration(SagaInfrastructureConfiguration.class);

    @BeforeEach
    void clearRun() {
        RUN.clear();
    }

    @Test
    void the_same_id_on_two_public_static_factories_one_hiding_the_other_fails_startup() {
        runner.withBean(PublicHidingWithSameId.class, PublicHidingWithSameId::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-id");
        });
    }

    @Test
    void two_public_static_factories_one_hiding_the_other_both_run_when_their_ids_differ() {
        runner.withBean(PublicHidingWithOtherId.class, PublicHidingWithOtherId::new).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(RUN).containsExactlyInAnyOrder("PublicBase", "PublicHidingWithOtherId");
            assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactlyInAnyOrder("base-id", "hiding-id");
        });
    }

    @Test
    void the_same_id_on_two_protected_static_factories_one_hiding_the_other_fails_startup() {
        runner.withBean(ProtectedHidingWithSameId.class, ProtectedHidingWithSameId::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-id");
        });
    }

    @Test
    void two_protected_static_factories_one_hiding_the_other_both_run_when_their_ids_differ() {
        runner.withBean(ProtectedHidingWithOtherId.class, ProtectedHidingWithOtherId::new).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(RUN).containsExactlyInAnyOrder("ProtectedBase", "ProtectedHidingWithOtherId");
            assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactlyInAnyOrder("base-id", "hiding-id");
        });
    }

    @Test
    void the_same_id_on_two_public_static_projection_factories_one_hiding_the_other_fails_startup() {
        projectionRunner.withBean(PublicProjectionHidingWithSameId.class, PublicProjectionHidingWithSameId::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-id");
        });
    }

    @Test
    void two_public_static_projection_factories_one_hiding_the_other_both_run_when_their_ids_differ() {
        projectionRunner.withBean(PublicProjectionHidingWithOtherId.class, PublicProjectionHidingWithOtherId::new).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(RUN).containsExactlyInAnyOrder("PublicProjectionBase", "PublicProjectionHidingWithOtherId");
        });
    }

    @Test
    void the_same_id_on_two_protected_static_projection_factories_one_hiding_the_other_fails_startup() {
        projectionRunner.withBean(ProtectedProjectionHidingWithSameId.class, ProtectedProjectionHidingWithSameId::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-id");
        });
    }

    @Test
    void two_protected_static_projection_factories_one_hiding_the_other_both_run_when_their_ids_differ() {
        projectionRunner.withBean(ProtectedProjectionHidingWithOtherId.class, ProtectedProjectionHidingWithOtherId::new).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(RUN).containsExactlyInAnyOrder("ProtectedProjectionBase", "ProtectedProjectionHidingWithOtherId");
        });
    }

    @Test
    void the_same_id_on_two_public_static_saga_factories_one_hiding_the_other_fails_startup() {
        sagaRunner.withBean(PublicSagaHidingWithSameId.class, PublicSagaHidingWithSameId::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-id");
        });
    }

    @Test
    void two_public_static_saga_factories_one_hiding_the_other_both_run_when_their_ids_differ() {
        sagaRunner.withBean(PublicSagaHidingWithOtherId.class, PublicSagaHidingWithOtherId::new).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(RUN).containsExactlyInAnyOrder("PublicSagaBase", "PublicSagaHidingWithOtherId");
            assertThat(context).hasBean(SagaAnnotationRegistrar.sagaInstancesBeanName("base-id"));
            assertThat(context).hasBean(SagaAnnotationRegistrar.sagaInstancesBeanName("hiding-id"));
        });
    }

    @Test
    void the_same_id_on_two_protected_static_saga_factories_one_hiding_the_other_fails_startup() {
        sagaRunner.withBean(ProtectedSagaHidingWithSameId.class, ProtectedSagaHidingWithSameId::new).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-id");
        });
    }

    @Test
    void two_protected_static_saga_factories_one_hiding_the_other_both_run_when_their_ids_differ() {
        sagaRunner.withBean(ProtectedSagaHidingWithOtherId.class, ProtectedSagaHidingWithOtherId::new).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(RUN).containsExactlyInAnyOrder("ProtectedSagaBase", "ProtectedSagaHidingWithOtherId");
            assertThat(context).hasBean(SagaAnnotationRegistrar.sagaInstancesBeanName("base-id"));
            assertThat(context).hasBean(SagaAnnotationRegistrar.sagaInstancesBeanName("hiding-id"));
        });
    }

    private static List<String> subscribedIds(Subscriptions<?> subscriptions) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe"))
                .map(invocation -> (String) invocation.getRawArguments()[0])
                .toList();
    }

    private static SnapshotView<TestState, TestEvent> view() {
        return SnapshotView.<TestState, TestEvent>builder(new TestState())
                .on(TestEvent.class, (state, event) -> state)
                .build();
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    private static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
        return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, event) -> new OrderState(event.orderId()))
                .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.issue(new ShipOrder(event.orderId()))))
                .build();
    }

    static class PublicBase {
        @Snapshot(id = "base-id")
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicBase");
            return view();
        }
    }

    static class PublicHidingWithSameId extends PublicBase {
        @Snapshot(id = "base-id")
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicHidingWithSameId");
            return view();
        }
    }

    static class PublicHidingWithOtherId extends PublicBase {
        @Snapshot(id = "hiding-id")
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicHidingWithOtherId");
            return view();
        }
    }

    static class ProtectedBase {
        @Snapshot(id = "base-id")
        protected static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("ProtectedBase");
            return view();
        }
    }

    static class ProtectedHidingWithSameId extends ProtectedBase {
        @Snapshot(id = "base-id")
        protected static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("ProtectedHidingWithSameId");
            return view();
        }
    }

    static class ProtectedHidingWithOtherId extends ProtectedBase {
        @Snapshot(id = "hiding-id")
        protected static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("ProtectedHidingWithOtherId");
            return view();
        }
    }

    static class PublicProjectionBase {
        @Projection(id = "base-id", source = Source.PUSH, subscriptionModelName = "base-feed")
        public static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> factory() {
            RUN.add("PublicProjectionBase");
            return projection();
        }
    }

    static class PublicProjectionHidingWithSameId extends PublicProjectionBase {
        @Projection(id = "base-id", source = Source.PUSH, subscriptionModelName = "hiding-feed")
        public static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> factory() {
            RUN.add("PublicProjectionHidingWithSameId");
            return projection();
        }
    }

    static class PublicProjectionHidingWithOtherId extends PublicProjectionBase {
        @Projection(id = "hiding-id", source = Source.PUSH, subscriptionModelName = "hiding-feed")
        public static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> factory() {
            RUN.add("PublicProjectionHidingWithOtherId");
            return projection();
        }
    }

    static class ProtectedProjectionBase {
        @Projection(id = "base-id", source = Source.PUSH, subscriptionModelName = "base-feed")
        protected static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> factory() {
            RUN.add("ProtectedProjectionBase");
            return projection();
        }
    }

    static class ProtectedProjectionHidingWithSameId extends ProtectedProjectionBase {
        @Projection(id = "base-id", source = Source.PUSH, subscriptionModelName = "hiding-feed")
        protected static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> factory() {
            RUN.add("ProtectedProjectionHidingWithSameId");
            return projection();
        }
    }

    static class ProtectedProjectionHidingWithOtherId extends ProtectedProjectionBase {
        @Projection(id = "hiding-id", source = Source.PUSH, subscriptionModelName = "hiding-feed")
        protected static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> factory() {
            RUN.add("ProtectedProjectionHidingWithOtherId");
            return projection();
        }
    }

    static class PublicSagaBase {
        @Saga(id = "base-id", source = Source.PUSH, catchup = Catchup.NONE, subscriptionModelName = "base-feed")
        public static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> factory() {
            RUN.add("PublicSagaBase");
            return saga();
        }
    }

    static class PublicSagaHidingWithSameId extends PublicSagaBase {
        @Saga(id = "base-id", source = Source.PUSH, catchup = Catchup.NONE, subscriptionModelName = "hiding-feed")
        public static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> factory() {
            RUN.add("PublicSagaHidingWithSameId");
            return saga();
        }
    }

    static class PublicSagaHidingWithOtherId extends PublicSagaBase {
        @Saga(id = "hiding-id", source = Source.PUSH, catchup = Catchup.NONE, subscriptionModelName = "hiding-feed")
        public static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> factory() {
            RUN.add("PublicSagaHidingWithOtherId");
            return saga();
        }
    }

    static class ProtectedSagaBase {
        @Saga(id = "base-id", source = Source.PUSH, catchup = Catchup.NONE, subscriptionModelName = "base-feed")
        protected static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> factory() {
            RUN.add("ProtectedSagaBase");
            return saga();
        }
    }

    static class ProtectedSagaHidingWithSameId extends ProtectedSagaBase {
        @Saga(id = "base-id", source = Source.PUSH, catchup = Catchup.NONE, subscriptionModelName = "hiding-feed")
        protected static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> factory() {
            RUN.add("ProtectedSagaHidingWithSameId");
            return saga();
        }
    }

    static class ProtectedSagaHidingWithOtherId extends ProtectedSagaBase {
        @Saga(id = "hiding-id", source = Source.PUSH, catchup = Catchup.NONE, subscriptionModelName = "hiding-feed")
        protected static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> factory() {
            RUN.add("ProtectedSagaHidingWithOtherId");
            return saga();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ProjectionInfrastructureConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        @Bean("base-feed")
        DomainEventFeed<TestEvent> baseFeed(CloudEventConverter<TestEvent> converter) {
            return emptyFeed(converter);
        }

        @Bean("hiding-feed")
        DomainEventFeed<TestEvent> hidingFeed(CloudEventConverter<TestEvent> converter) {
            return emptyFeed(converter);
        }

        private static DomainEventFeed<TestEvent> emptyFeed(CloudEventConverter<TestEvent> converter) {
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

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SagaInfrastructureConfiguration {
        @Bean
        CloudEventConverter<OrderEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                    return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test"))
                            .withType(domainEvent.getClass().getSimpleName()).withSubject(domainEvent.orderId()).build();
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
        SagaInstancesRegistryImpl sagaInstancesRegistry() {
            return new SagaInstancesRegistryImpl();
        }

        @Bean("base-feed")
        PushSubscriptionModel basePushModel() {
            return new PushSubscriptionModel();
        }

        @Bean("hiding-feed")
        PushSubscriptionModel hidingPushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        SagaStateStore<OrderState> sagaStateStore() {
            return SagaStateStore.inMemory();
        }

        @Bean
        CommandDispatcher<OrderCommand> commandDispatcher() {
            return command -> {
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InfrastructureConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        SnapshotStore<TestState> snapshotStore() {
            return mock(SnapshotStore.class);
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
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
}
