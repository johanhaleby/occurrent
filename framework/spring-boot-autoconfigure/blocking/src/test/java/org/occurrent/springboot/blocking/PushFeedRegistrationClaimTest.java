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
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.SubscriptionMode;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A {@code PushSubscriptionModel} delivers to one subscription only, so one feed bean takes one push
 * {@code @Projection} or {@code @Saga}. These tests check that a second registration on the same feed instance fails
 * context refresh and names both registrations, that feeds are told apart by identity rather than {@code equals}, and
 * that the {@code occurrentCatchupThenPushSubscriptionModelsByLiveFeed} map a broker source reads resolves each feed
 * to its own wrapper.
 * <p>
 * They start no containers. They use an empty in-memory reader, a mocked checkpoint storage and the real
 * {@code PushSubscriptionModel}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PushFeedRegistrationClaimTest {

    private static final String WRAPPERS_BY_LIVE_FEED = "occurrentCatchupThenPushSubscriptionModelsByLiveFeed";

    @Test
    void two_catch_up_push_projections_on_one_feed_fail_a_manual_start_context_refresh() {
        runner(SubscriptionMode.MANUAL, SharedFeedFixture.class, TwoCatchupProjections.class).run(context ->
                assertRefusedOnSharedFeed(context, "@Projection '(?:first|second)'", "@Projection 'first'", "@Projection 'second'"));
    }

    @Test
    void a_push_projection_without_catch_up_and_a_catch_up_push_projection_on_one_feed_fail_a_manual_start_context_refresh() {
        runner(SubscriptionMode.MANUAL, SharedFeedFixture.class, NoCatchupAndCatchupProjections.class).run(context ->
                assertRefusedOnSharedFeed(context, "@Projection '(?:first|second)'", "@Projection 'first'", "@Projection 'second'"));
    }

    @Test
    void two_push_projections_on_one_feed_fail_an_auto_start_context_refresh_by_naming_both() {
        runner(SubscriptionMode.AUTO, SharedFeedFixture.class, TwoCatchupProjections.class).run(context ->
                assertRefusedOnSharedFeed(context, "@Projection '(?:first|second)'", "@Projection 'first'", "@Projection 'second'"));
    }

    @Test
    void a_push_saga_and_a_push_projection_on_one_feed_fail_context_refresh_naming_both() {
        runner(SubscriptionMode.MANUAL, SharedFeedFixture.class, SagaFixture.class, SagaAndProjection.class).run(context ->
                assertRefusedOnSharedFeed(context, "@Saga 'the-saga'|@Projection 'the-projection'", "@Saga 'the-saga'", "@Projection 'the-projection'"));
    }

    @Test
    void feeds_that_are_equal_but_distinct_each_resolve_to_the_wrapper_of_their_own_projection() {
        runner(SubscriptionMode.AUTO, EqualFeedsFixture.class, ProjectionsOnEqualFeeds.class).run(context -> {
            assertThat(context).hasNotFailed();
            PushSubscriptionModel firstFeed = context.getBean("firstFeed", PushSubscriptionModel.class);
            PushSubscriptionModel secondFeed = context.getBean("secondFeed", PushSubscriptionModel.class);
            CatchupThenPushSubscriptionModel firstWrapper = context.getBean("catchupThenPushSubscriptionModel-first", CatchupThenPushSubscriptionModel.class);
            CatchupThenPushSubscriptionModel secondWrapper = context.getBean("catchupThenPushSubscriptionModel-second", CatchupThenPushSubscriptionModel.class);
            Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel> wrappersByLiveFeed = wrappersByLiveFeed(context);

            assertThat(firstWrapper).isNotSameAs(secondWrapper);
            assertThat(wrappersByLiveFeed.get(firstFeed)).as("wrapper for the first feed").isSameAs(firstWrapper);
            assertThat(wrappersByLiveFeed.get(secondFeed)).as("wrapper for the second feed").isSameAs(secondWrapper);
        });
    }

    @Test
    void a_single_push_projection_under_manual_start_resolves_its_feed_to_its_wrapper_and_subscribes_once_started() {
        runner(SubscriptionMode.MANUAL, SharedFeedFixture.class, SingleProjection.class).run(context -> {
            assertThat(context).hasNotFailed();
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);

            context.getBean(ManualStartPushSources.class).start("only");

            assertThat(wrappersByLiveFeed(context).get(feed))
                    .isSameAs(context.getBean("catchupThenPushSubscriptionModel-only", CatchupThenPushSubscriptionModel.class));
            assertThat(feed.subscriptionIds()).containsExactly("only");
        });
    }

    @Test
    void a_single_push_projection_under_auto_start_resolves_its_feed_to_its_wrapper_and_is_subscribed() {
        runner(SubscriptionMode.AUTO, SharedFeedFixture.class, SingleProjection.class).run(context -> {
            assertThat(context).hasNotFailed();
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);

            assertThat(wrappersByLiveFeed(context).get(feed))
                    .isSameAs(context.getBean("catchupThenPushSubscriptionModel-only", CatchupThenPushSubscriptionModel.class));
            assertThat(feed.subscriptionIds()).containsExactly("only");
        });
    }

    private static ApplicationContextRunner runner(SubscriptionMode mode, Class<?>... configurations) {
        OccurrentProperties properties = new OccurrentProperties();
        properties.getSubscription().setMode(mode);
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withBean(ManualStartPushSources.class, ManualStartPushSources::new)
                .withBean(OccurrentProperties.class, () -> properties)
                .withUserConfiguration(configurations);
    }

    // Which of the two registrations is refused depends on the order Spring processes them, so either one may be named first.
    private static void assertRefusedOnSharedFeed(AssertableApplicationContext context, String refusedRegistrantRegex, String... bothRegistrants) {
        assertThat(context).hasFailed();
        assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("(?s)Cannot register (?:" + refusedRegistrantRegex + ") on push feed bean 'pushModel'\\..*")
                .hasMessageContainingAll(bothRegistrants);
    }

    @SuppressWarnings("unchecked")
    private static Map<PushSubscriptionModel, CatchupThenPushSubscriptionModel> wrappersByLiveFeed(AssertableApplicationContext context) {
        return context.getBean(WRAPPERS_BY_LIVE_FEED, Map.class);
    }

    // --- Fixtures ---

    sealed interface OrderEvent {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record OrderState(String orderId) {
    }

    private static org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> countingProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, OrderEvent, String>builder(0)
                .id(OrderEvent::orderId)
                .on(OrderPlaced.class, (state, event) -> state + 1)
                .build();
    }

    // What a catch-up needs whichever feed the projection is on.
    static class CatchupSupport {
        @Bean
        PositionOrderedReader reader() {
            return new PositionOrderedReader() {
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
        }

        @Bean
        CheckpointStorage checkpointStorage() {
            return mock(CheckpointStorage.class);
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }

        @Bean
        CloudEventConverter<OrderEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                    return CloudEventBuilder.v1()
                            .withId(domainEvent.eventId())
                            .withSource(URI.create("urn:test"))
                            .withType(domainEvent.getClass().getSimpleName())
                            .withSubject(domainEvent.orderId())
                            .build();
                }

                @Override
                public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new OrderPlaced(cloudEvent.getId(), Objects.requireNonNull(cloudEvent.getSubject()));
                }

                @Override
                public String getCloudEventType(Class<? extends OrderEvent> type) {
                    return type.getSimpleName();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SharedFeedFixture extends CatchupSupport {
        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SagaFixture {
        @Bean
        SagaInstancesRegistryImpl sagaInstancesRegistry() {
            return new SagaInstancesRegistryImpl();
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

    // Two instances that always compare equal, so a map keyed with equals would treat the two feeds as one.
    static final class AlwaysEqualPushSubscriptionModel extends PushSubscriptionModel {
        @Override
        public boolean equals(Object other) {
            return other instanceof AlwaysEqualPushSubscriptionModel;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EqualFeedsFixture extends CatchupSupport {
        @Bean
        PushSubscriptionModel firstFeed() {
            return new AlwaysEqualPushSubscriptionModel();
        }

        @Bean
        PushSubscriptionModel secondFeed() {
            return new AlwaysEqualPushSubscriptionModel();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoCatchupProjections {
        @Bean
        TwoCatchupProjectionsHolder twoCatchupProjections() {
            return new TwoCatchupProjectionsHolder();
        }
    }

    static class TwoCatchupProjectionsHolder {
        @Projection(id = "first", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> first() {
            return countingProjection();
        }

        @Projection(id = "second", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> second() {
            return countingProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NoCatchupAndCatchupProjections {
        @Bean
        NoCatchupAndCatchupProjectionsHolder noCatchupAndCatchupProjections() {
            return new NoCatchupAndCatchupProjectionsHolder();
        }
    }

    static class NoCatchupAndCatchupProjectionsHolder {
        @Projection(id = "first", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> first() {
            return countingProjection();
        }

        @Projection(id = "second", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> second() {
            return countingProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SagaAndProjection {
        @Bean
        SagaAndProjectionHolder sagaAndProjection() {
            return new SagaAndProjectionHolder();
        }
    }

    static class SagaAndProjectionHolder {
        @Saga(id = "the-saga", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, event) -> new OrderState(event.orderId()))
                    .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.issue(new ShipOrder(event.orderId()))))
                    .build();
        }

        @Projection(id = "the-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> projection() {
            return countingProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ProjectionsOnEqualFeeds {
        @Bean
        ProjectionsOnEqualFeedsHolder projectionsOnEqualFeeds() {
            return new ProjectionsOnEqualFeedsHolder();
        }
    }

    static class ProjectionsOnEqualFeedsHolder {
        @Projection(id = "first", source = Source.PUSH, subscriptionModelName = "firstFeed")
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> first() {
            return countingProjection();
        }

        @Projection(id = "second", source = Source.PUSH, subscriptionModelName = "secondFeed")
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> second() {
            return countingProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SingleProjection {
        @Bean
        SingleProjectionHolder singleProjection() {
            return new SingleProjectionHolder();
        }
    }

    static class SingleProjectionHolder {
        @Projection(id = "only", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> only() {
            return countingProjection();
        }
    }
}
