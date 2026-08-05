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
import org.occurrent.annotation.Mode;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.BackgroundCatchupFailures;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Characterizes the {@code @Projection} validation branches that fail fast before any subscription model or store is
 * consulted, so they reproduce without a running store (no Docker): the {@code source=PUSH} guards (no synchronous
 * mode, no catch-up start knobs, no DcbProjection, a feed bean that is neither a {@code PushSubscriptionModel} nor a
 * {@code DomainEventFeed}, and two projections resolving the same push sink, see ADR 90), and the convention-based
 * store resolution failing when the factory return type carries no concrete state type and no store bean exists. Each
 * must fail fast at context startup with the exact message. One test is deliberately the mirror image of that theme:
 * a domain-feed push projection with {@code startupMode = BACKGROUND} must be accepted, not rejected, now that ADR 90
 * makes a domain-push feed unambiguously one projection's own.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationValidationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(ConverterConfiguration.class);

    @Test
    void push_projection_with_synchronous_mode_fails_fast() {
        runner.withUserConfiguration(FeedConfiguration.class, PushSynchronousConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot combine source=PUSH with mode=SYNCHRONOUS");
        });
    }

    @Test
    void push_projection_that_sets_a_start_position_fails_fast_and_points_at_startup_mode() {
        // Deliberately the PushSubscriptionModel feed, not the DomainEventFeed one: this message tells the reader to
        // use startupMode = BACKGROUND, and only that feed honours it.
        runner.withUserConfiguration(PushModelFeedConfiguration.class, PushStartKnobsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot set startAt, startAtGlobalPosition or resumeBehavior")
                    // startupMode is no longer in that list, and the message has to say so, since keeping a large
                    // replay off the startup path is the reason someone reaches for it here.
                    .hasMessageContaining("startupMode = BACKGROUND");
        });
    }


    @Test
    void a_domain_feed_push_projection_that_sets_startup_mode_is_accepted_rather_than_rejected() {
        // The feed is a DomainEventFeed, not a PushSubscriptionModel, but startupMode = BACKGROUND is honoured here
        // too: the catch-up runs on a thread the registrar owns instead of holding up the refresh. See
        // DomainEventFeedProjectionPushStartupModeTest for the fuller proof (parked replay, context refreshes,
        // released replay fills the store). This test only checks that the context does not fail fast, which is
        // what the rejection used to do before ADR 90 made a domain-push feed unambiguously one projection's own.
        runner.withUserConfiguration(DomainFeedBackgroundConfiguration.class).run(context ->
                assertThat(context).hasNotFailed());
    }

    @Test
    void two_push_projections_on_the_same_push_subscription_model_fail_the_context_naming_both_ids() {
        runner.withUserConfiguration(SharedPushModelConfiguration.class, TwoPushSinkProjectionsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("push-sink-a")
                    .hasMessageContaining("push-sink-b")
                    .hasMessageContaining("a push sink feeds exactly one consumer")
                    .hasMessageContaining("Declare one sink per projection");
        });
    }

    @Test
    void two_push_projections_on_the_same_domain_event_feed_fail_the_context_naming_both_ids() {
        runner.withUserConfiguration(SharedDomainEventFeedConfiguration.class, TwoDomainFeedSinkProjectionsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("domain-feed-sink-a")
                    .hasMessageContaining("domain-feed-sink-b")
                    .hasMessageContaining("a push sink feeds exactly one consumer")
                    .hasMessageContaining("Declare one sink per projection");
        });
    }

    @Test
    void push_projection_returning_a_dcb_projection_fails_fast() {
        runner.withUserConfiguration(FeedConfiguration.class, PushDcbConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("with source=PUSH must return a Projection. A DcbProjection push source is not supported");
        });
    }

    @Test
    void push_projection_whose_feed_bean_is_neither_a_push_model_nor_a_domain_feed_fails_fast() {
        runner.withUserConfiguration(WrongFeedConfiguration.class, PushWrongFeedConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("which is neither a PushSubscriptionModel nor a DomainEventFeed");
        });
    }

    @Test
    void an_event_store_projection_that_sets_catchup_fails_fast_and_points_at_start_at() {
        runner.withUserConfiguration(EventStoreCatchupConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sets catchup, which only applies to source=PUSH")
                    .hasMessageContaining("startAt = NOW to skip it");
        });
    }

    @Test
    void a_push_projection_with_catchup_none_that_sets_a_start_position_fails_fast_without_the_startup_mode_hint() {
        runner.withUserConfiguration(PushModelFeedConfiguration.class, PushCatchupNoneStartKnobsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot set startAt, startAtGlobalPosition or resumeBehavior")
                    .hasMessageContaining("With catchup=NONE it takes live events only")
                    .hasMessageNotContaining("startupMode = BACKGROUND");
        });
    }

    @Test
    void a_push_projection_with_catchup_none_that_sets_startup_mode_fails_fast() {
        runner.withUserConfiguration(PushModelFeedConfiguration.class, PushCatchupNoneStartupModeConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("replays nothing and there is no startup work for startupMode to decide about")
                    .hasMessageContaining("drop catchup=NONE if you meant the projection to catch up first");
        });
    }

    @Test
    void projection_without_a_concrete_state_type_and_no_store_bean_fails_fast() {
        runner.withUserConfiguration(RawReturnTypeConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("needs a read-model store");
        });
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> countProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterConfiguration {
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
    }

    @Configuration(proxyBeanMethods = false)
    static class FeedConfiguration {
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            return new DomainEventFeed<>(mock(PositionOrderedReader.class), converter, event -> "k");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WrongFeedConfiguration {
        @Bean
        String wrongFeed() {
            return "not-a-feed";
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushSynchronousConfiguration {
        @Bean
        PushSynchronousProjection pushSynchronousProjection() {
            return new PushSynchronousProjection();
        }
    }

    static class PushSynchronousProjection {
        @Projection(id = "push-sync", source = Source.PUSH, mode = Mode.SYNCHRONOUS)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushStartKnobsConfiguration {
        @Bean
        PushStartKnobsProjection pushStartKnobsProjection() {
            return new PushStartKnobsProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushModelFeedConfiguration {
        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DomainFeedBackgroundConfiguration {
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            // A real (empty) reader rather than a mock: this test lets the catch-up actually run in the background,
            // unlike the other FeedConfiguration-based tests above which fail validation before the feed is ever used.
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

        @Bean
        BackgroundCatchupFailures backgroundCatchupFailures() {
            return new BackgroundCatchupFailures();
        }

        @Bean
        DomainFeedBackgroundProjection domainFeedBackgroundProjection() {
            return new DomainFeedBackgroundProjection();
        }
    }

    static class DomainFeedBackgroundProjection {
        @Projection(id = "domain-feed-background", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SharedPushModelConfiguration {
        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
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
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoPushSinkProjectionsConfiguration {
        @Bean
        TwoPushSinkProjections twoPushSinkProjections() {
            return new TwoPushSinkProjections();
        }
    }

    // Both factory methods resolve the same shared PushSubscriptionModel bean by type (neither names it explicitly),
    // so the second registration collides with the first on the underlying sink.
    static class TwoPushSinkProjections {
        @Projection(id = "push-sink-a", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> first() {
            return countProjection();
        }

        @Projection(id = "push-sink-b", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> second() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SharedDomainEventFeedConfiguration {
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            // A real (empty) reader rather than a mock: DomainEventFeed.register validates writesPosition() before
            // the single-consumer check even runs, and a Mockito mock's boolean default (false) trips that first.
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
    static class TwoDomainFeedSinkProjectionsConfiguration {
        @Bean
        TwoDomainFeedSinkProjections twoDomainFeedSinkProjections() {
            return new TwoDomainFeedSinkProjections();
        }
    }

    // Both factory methods resolve the same shared DomainEventFeed bean by type, so the second register() call
    // collides with the first: the feed is claimed synchronously, before any catch-up runs, so the mocked reader is
    // never touched.
    static class TwoDomainFeedSinkProjections {
        @Projection(id = "domain-feed-sink-a", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> first() {
            return countProjection();
        }

        @Projection(id = "domain-feed-sink-b", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> second() {
            return countProjection();
        }
    }

    static class PushStartKnobsProjection {
        @Projection(id = "push-knobs", source = Source.PUSH, startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EventStoreCatchupConfiguration {
        @Bean
        EventStoreCatchupProjection eventStoreCatchupProjection() {
            return new EventStoreCatchupProjection();
        }
    }

    static class EventStoreCatchupProjection {
        @Projection(id = "event-store-catchup", catchup = org.occurrent.annotation.Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushCatchupNoneStartKnobsConfiguration {
        @Bean
        PushCatchupNoneStartKnobsProjection pushCatchupNoneStartKnobsProjection() {
            return new PushCatchupNoneStartKnobsProjection();
        }
    }

    static class PushCatchupNoneStartKnobsProjection {
        @Projection(id = "push-none-start-knobs", source = Source.PUSH, catchup = org.occurrent.annotation.Catchup.NONE, startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushCatchupNoneStartupModeConfiguration {
        @Bean
        PushCatchupNoneStartupModeProjection pushCatchupNoneStartupModeProjection() {
            return new PushCatchupNoneStartupModeProjection();
        }
    }

    static class PushCatchupNoneStartupModeProjection {
        @Projection(id = "push-none-startup-mode", source = Source.PUSH, catchup = org.occurrent.annotation.Catchup.NONE, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushDcbConfiguration {
        @Bean
        PushDcbProjection pushDcbProjection() {
            return new PushDcbProjection();
        }
    }

    static class PushDcbProjection {
        @Projection(id = "push-dcb", source = Source.PUSH)
        DcbProjection<Integer, TestEvent, String> projection() {
            return new DcbProjection<>(countProjection(), DcbCriteria.all());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushWrongFeedConfiguration {
        @Bean
        PushWrongFeedProjection pushWrongFeedProjection() {
            return new PushWrongFeedProjection();
        }
    }

    static class PushWrongFeedProjection {
        @Projection(id = "push-wrong-feed", source = Source.PUSH, subscriptionModelName = "wrongFeed")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RawReturnTypeConfiguration {
        @Bean
        RawReturnTypeProjection rawReturnTypeProjection() {
            return new RawReturnTypeProjection();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static class RawReturnTypeProjection {
        // Raw return type on purpose: reflectStateType cannot read a concrete state type from it, so with no store
        // bean the convention-based resolution must fail fast instead of using the zero-config default.
        @Projection(id = "raw-return")
        org.occurrent.dsl.projection.Projection projection() {
            return countProjection();
        }
    }

    record TestEvent() {
    }
}
