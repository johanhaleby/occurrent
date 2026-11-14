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
import kotlin.jvm.functions.Function2;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * A static {@code @Snapshot} or {@code @Projection} factory that hides the static factory of its superclass with the same name and
 * parameters and repeats the annotation is a factory of its own, so both register, and an id they share fails startup
 * with {@link DuplicateSubscriptionIdException}. The tests use {@code public} and {@code protected} factories, since
 * the tests of the scan in {@code SubscriptionAnnotationsTest} use package-private ones. The projections use
 * {@code source = PUSH}, which registers without Docker. There is no {@code @Saga} test here, since the reactor starter
 * doesn't support {@code @Saga}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class HiddenStaticFactoryRegistrationTest {

    private static final List<String> RUN = new CopyOnWriteArrayList<>();

    private final ApplicationContextRunner postProcessorRunner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new);
    private final ApplicationContextRunner runner = postProcessorRunner.withUserConfiguration(InfrastructureConfiguration.class);
    private final ApplicationContextRunner projectionRunner = postProcessorRunner.withUserConfiguration(ProjectionInfrastructureConfiguration.class);

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

    static class PublicBase {
        @Snapshot(id = "base-id", startAt = StartPosition.NOW)
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicBase");
            return view();
        }
    }

    static class PublicHidingWithSameId extends PublicBase {
        @Snapshot(id = "base-id", startAt = StartPosition.NOW)
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicHidingWithSameId");
            return view();
        }
    }

    static class PublicHidingWithOtherId extends PublicBase {
        @Snapshot(id = "hiding-id", startAt = StartPosition.NOW)
        public static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("PublicHidingWithOtherId");
            return view();
        }
    }

    static class ProtectedBase {
        @Snapshot(id = "base-id", startAt = StartPosition.NOW)
        protected static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("ProtectedBase");
            return view();
        }
    }

    static class ProtectedHidingWithSameId extends ProtectedBase {
        @Snapshot(id = "base-id", startAt = StartPosition.NOW)
        protected static SnapshotView<TestState, TestEvent> factory() {
            RUN.add("ProtectedHidingWithSameId");
            return view();
        }
    }

    static class ProtectedHidingWithOtherId extends ProtectedBase {
        @Snapshot(id = "hiding-id", startAt = StartPosition.NOW)
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

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ProjectionInfrastructureConfiguration {
        // A DomainEventFeed isn't a Subscribable, and without a Subscribable bean the reactive post-processor doesn't
        // look for @Projection or @Snapshot factories
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

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
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InfrastructureConfiguration {
        @SuppressWarnings("unchecked")
        @Bean
        ReactiveSnapshotStore<TestState> snapshotStore() {
            return mock(ReactiveSnapshotStore.class);
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new TestEventConverter();
        }

        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        EventStore eventStore() {
            return mock(EventStore.class);
        }

        @SuppressWarnings("unchecked")
        @Bean
        Subscriptions<TestEvent> subscriptions() {
            Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
            org.occurrent.subscription.api.reactor.Subscription subscription = mock(org.occurrent.subscription.api.reactor.Subscription.class);
            when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
            when(subscriptions.subscribe(any(String.class), any(AgnosticSubscriptionFilter.class),
                    any(StartAt.class), any(Function2.class))).thenReturn(subscription);
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
