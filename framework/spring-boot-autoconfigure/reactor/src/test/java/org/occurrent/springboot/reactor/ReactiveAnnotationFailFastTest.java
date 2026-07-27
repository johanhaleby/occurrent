/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.Mode;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Container-free {@link ApplicationContextRunner} fail-fast tests for the reactive annotation bean post-processor's
 * error/validation branches that the Docker-gated {@code Reactive*MongoTest}s in the store starter don't exercise (those are the CI gate for
 * happy paths and Docker-dependent branches; these run with no Testcontainers, characterizing behavior that must
 * survive the {@code OccurrentReactiveAnnotationBeanPostProcessor} decomposition into package-private registrars).
 * Modeled on {@code DcbSubscriptionMalformedTagAnnotationTest} in the blocking stack's test package.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveAnnotationFailFastTest {

    @Test
    void more_than_one_subscription_annotation_on_a_method_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TwoAnnotationsConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .cause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("more than one of @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription");
                });
    }

    @Test
    void a_dcb_subscription_with_no_event_parameter_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(NoEventParamDcbConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .cause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("must declare an event parameter");
                });
    }

    @Test
    void a_stream_subscription_with_a_specific_start_time_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterConfiguration.class, SpecificTimeStreamSubscriberConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .cause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("cannot honor a specific historical start time");
                });
    }

    @Test
    void a_beginning_of_time_stream_subscription_fails_fast_when_history_replay_is_not_supported() {
        // No bean that is both a PositionOrderedReader and an EventStore, so streamHistoryReplaySupported() is false
        // without needing a store at all.
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterConfiguration.class, BeginningOfTimeStreamSubscriberConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .cause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("does not support reactive stream history replay");
                });
    }

    @Test
    void a_push_projection_cannot_combine_source_push_with_mode_synchronous() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterSubscribableAndPushFeedConfiguration.class, PushSynchronousProjectionConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("cannot combine source=PUSH with mode=SYNCHRONOUS");
                });
    }

    @Test
    void a_push_projection_does_not_support_the_catchup_start_knobs() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterSubscribableAndPushFeedConfiguration.class, PushWithStartKnobProjectionConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("does not support the catch-up start knobs");
                });
    }

    @Test
    void a_push_projection_must_return_a_projection_not_a_dcb_projection() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterSubscribableAndPushFeedConfiguration.class, PushDcbProjectionConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("DcbProjection push source is not supported");
                });
    }

    @Test
    void a_push_projection_feed_bean_must_be_a_push_subscription_model_or_a_domain_event_feed() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterAndSubscribableConfiguration.class, PushWrongFeedTypeProjectionConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("neither a PushSubscriptionModel nor a DomainEventFeed");
                });
    }

    @Test
    void a_snapshot_with_no_store_bean_and_no_concrete_state_type_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterAndSubscribableConfiguration.class, RawSnapshotViewConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("needs a snapshot store");
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoAnnotationsConfiguration {
        @Bean
        TwoAnnotationsSubscriber twoAnnotationsSubscriber() {
            return new TwoAnnotationsSubscriber();
        }
    }

    static class TwoAnnotationsSubscriber {
        @StreamSubscription(id = "twoAnnotationsStream")
        @Subscription(id = "twoAnnotationsAgnostic")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NoEventParamDcbConfiguration {
        @Bean
        NoEventParamDcbSubscriber noEventParamDcbSubscriber() {
            return new NoEventParamDcbSubscriber();
        }
    }

    static class NoEventParamDcbSubscriber {
        @DcbSubscription(id = "noEventParam")
        void on() {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return testEventConverter();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SpecificTimeStreamSubscriberConfiguration {
        @Bean
        SpecificTimeStreamSubscriber specificTimeStreamSubscriber() {
            return new SpecificTimeStreamSubscriber();
        }
    }

    static class SpecificTimeStreamSubscriber {
        @StreamSubscription(id = "specificTime", startAtISO8601 = "2024-01-01T00:00:00Z")
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class BeginningOfTimeStreamSubscriberConfiguration {
        @Bean
        BeginningOfTimeStreamSubscriber beginningOfTimeStreamSubscriber() {
            return new BeginningOfTimeStreamSubscriber();
        }
    }

    static class BeginningOfTimeStreamSubscriber {
        @StreamSubscription(id = "beginningOfTime", startAt = StartPosition.BEGINNING_OF_TIME)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterAndSubscribableConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return testEventConverter();
        }

        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterSubscribableAndPushFeedConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return testEventConverter();
        }

        // A PushSubscriptionModel is itself a Subscribable, so this single mock satisfies both the
        // afterSingletonsInstantiated early-return guard and the source=PUSH feed-bean resolution, with no ambiguity.
        @Bean
        PushSubscriptionModel pushSubscriptionModel() {
            return mock(PushSubscriptionModel.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushSynchronousProjectionConfiguration {
        @Bean
        PushSynchronousProjectionHolder pushSynchronousProjectionHolder() {
            return new PushSynchronousProjectionHolder();
        }
    }

    static class PushSynchronousProjectionHolder {
        @org.occurrent.annotation.Projection(id = "pushSynchronous", source = Source.PUSH, mode = Mode.SYNCHRONOUS)
        Object factory() {
            return null;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushWithStartKnobProjectionConfiguration {
        @Bean
        PushWithStartKnobProjectionHolder pushWithStartKnobProjectionHolder() {
            return new PushWithStartKnobProjectionHolder();
        }
    }

    static class PushWithStartKnobProjectionHolder {
        @org.occurrent.annotation.Projection(id = "pushWithStartKnob", source = Source.PUSH, startAt = org.occurrent.annotation.StartPosition.BEGINNING)
        Object factory() {
            return null;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushDcbProjectionConfiguration {
        @Bean
        PushDcbProjectionHolder pushDcbProjectionHolder() {
            return new PushDcbProjectionHolder();
        }
    }

    static class PushDcbProjectionHolder {
        @org.occurrent.annotation.Projection(id = "pushDcb", source = Source.PUSH)
        org.occurrent.dsl.projection.DcbProjection<Object, TestEvent, String> factory() {
            org.occurrent.dsl.projection.Projection<Object, TestEvent, String> projection =
                    org.occurrent.dsl.projection.Projection.<Object, TestEvent>singletonBuilder(new Object()).build();
            return new org.occurrent.dsl.projection.DcbProjection<>(projection, new DcbCriteria.MatchAll());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushWrongFeedTypeProjectionConfiguration {
        @Bean(name = "wrongFeed")
        String wrongFeed() {
            return "not a push feed";
        }

        @Bean
        PushWrongFeedTypeProjectionHolder pushWrongFeedTypeProjectionHolder() {
            return new PushWrongFeedTypeProjectionHolder();
        }
    }

    static class PushWrongFeedTypeProjectionHolder {
        @org.occurrent.annotation.Projection(id = "pushWrongFeedType", source = Source.PUSH, subscriptionModelName = "wrongFeed")
        Object factory() {
            return null;
        }
    }

    @Test
    void a_snapshot_with_an_ambiguous_store_type_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterAndSubscribableConfiguration.class, AmbiguousStoreSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("Disambiguate with storeName");
                });
    }

    @Test
    void a_snapshot_with_an_unresolvable_store_name_fails_fast() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterAndSubscribableConfiguration.class, MissingNamedStoreSnapshotConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("could not resolve a store bean named 'missingSnapshotStore'");
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class RawSnapshotViewConfiguration {
        @Bean
        RawSnapshotViewHolder rawSnapshotViewHolder() {
            return new RawSnapshotViewHolder();
        }
    }

    static class RawSnapshotViewHolder {
        @SuppressWarnings("rawtypes")
        @Snapshot(id = "rawSnapshotView")
        SnapshotView factory() {
            return mock(SnapshotView.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AmbiguousStoreSnapshotConfiguration {
        @Bean
        AmbiguousStoreSnapshotHolder ambiguousStoreSnapshotHolder() {
            return new AmbiguousStoreSnapshotHolder();
        }

        // Two beans of the referenced store type so resolution cannot pick one.
        @Bean
        String storeA() {
            return "a";
        }

        @Bean
        String storeB() {
            return "b";
        }
    }

    static class AmbiguousStoreSnapshotHolder {
        @SuppressWarnings("rawtypes")
        @Snapshot(id = "ambiguousStoreSnapshot", store = String.class)
        SnapshotView factory() {
            return mock(SnapshotView.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MissingNamedStoreSnapshotConfiguration {
        @Bean
        MissingNamedStoreSnapshotHolder missingNamedStoreSnapshotHolder() {
            return new MissingNamedStoreSnapshotHolder();
        }
    }

    static class MissingNamedStoreSnapshotHolder {
        @SuppressWarnings("rawtypes")
        @Snapshot(id = "missingNamedStoreSnapshot", storeName = "missingSnapshotStore")
        SnapshotView factory() {
            return mock(SnapshotView.class);
        }
    }

    private static CloudEventConverter<TestEvent> testEventConverter() {
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

    record TestEvent() {
    }
}
