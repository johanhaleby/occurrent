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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.springboot.reactor.LateRegistrationOnANonBlockingThreadTest.BaseConfiguration;
import org.occurrent.springboot.reactor.LateRegistrationOnANonBlockingThreadTest.ParkingCheckpointStorage;
import org.occurrent.springboot.reactor.LateRegistrationOnANonBlockingThreadTest.RecordingDelegate;
import org.occurrent.springboot.reactor.LateRegistrationOnANonBlockingThreadTest.TestEvent;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code startAt = NOW} annotation with the default {@code resumeBehavior} is the subscription model default, so over
 * {@link ReactorDurableSubscriptionModel} it records where the feed is when it registers. When the model it wraps
 * cannot say where that is, because the read fails or answers nothing, the registration is refused. A subscription
 * registered at startup that waits until it has started then fails the application context, which is the default setup.
 * {@code startWhenNoStartPositionCanBeRecorded} starts it anyway, and {@code startupMode = BACKGROUND} leaves it to be
 * refused without anything waiting for it. A {@code resumeBehavior} of {@code SAME_AS_START_AT} makes the start NOW, a
 * start of its own that is never refused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class NowStartRefusedAtStartupTest {

    private static final String NOW_ID = "now-start-refused-at-startup";

    // The two ways the wrapped model fails to say where its feed is, and what the refusal says when it is a result of that
    enum FeedPosition {
        ANSWERS_NOTHING(Mono::empty, "answered nothing when asked for the current position for subscription " + NOW_ID),
        FAILS(() -> Mono.error(new IllegalStateException("The feed position cannot be read")), "The feed position cannot be read"),
        ;

        private final Supplier<Mono<Checkpoint>> answer;
        private final String refusal;

        FeedPosition(Supplier<Mono<Checkpoint>> answer, String refusal) {
            this.answer = answer;
            this.refusal = refusal;
        }
    }

    @ParameterizedTest
    @EnumSource(FeedPosition.class)
    void the_application_context_does_not_start_when_a_default_resume_behavior_now_subscription_cannot_have_its_start_position_recorded(FeedPosition feedPosition) {
        // Given
        ApplicationContextRunner runner = runner(feedPosition).withBean(NowHolder.class);

        // When
        runner.run(context -> {
            // Then
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure())).hasMessageContaining(feedPosition.refusal);
        });
    }

    @ParameterizedTest
    @EnumSource(FeedPosition.class)
    void the_application_context_does_not_start_when_a_default_resume_behavior_now_dcb_subscription_cannot_have_its_start_position_recorded(FeedPosition feedPosition) {
        // Given
        ApplicationContextRunner runner = runner(feedPosition).withBean(DcbNowHolder.class);

        // When
        runner.run(context -> {
            // Then
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure())).hasMessageContaining(feedPosition.refusal);
        });
    }

    @Test
    void the_subscription_is_subscribed_when_the_model_may_start_without_a_recorded_start_position() {
        // Given
        ApplicationContextRunner runner = runner(FeedPosition.ANSWERS_NOTHING)
                .withBean(ReactorDurableSubscriptionModelConfig.class, () -> new ReactorDurableSubscriptionModelConfig(event -> false).startWhenNoStartPositionCanBeRecorded(true))
                .withBean(NowHolder.class);

        // When
        runner.run(context -> {
            // Then
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = context.getBean(RecordingDelegate.Holder.class).delegate();
            awaitUntil(() -> delegate.isSubscribed(NOW_ID));
            assertThat(delegate.isSubscribed(NOW_ID)).isTrue();
        });
    }

    @ParameterizedTest
    @EnumSource(FeedPosition.class)
    void the_subscription_is_subscribed_when_the_resume_behavior_is_same_as_start_at(FeedPosition feedPosition) {
        // Given
        ApplicationContextRunner runner = runner(feedPosition).withBean(NowSameAsStartAtHolder.class);

        // When
        runner.run(context -> {
            // Then
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = context.getBean(RecordingDelegate.Holder.class).delegate();
            awaitUntil(() -> delegate.isSubscribed(NOW_ID));
            assertThat(delegate.isSubscribed(NOW_ID)).isTrue();
        });
    }

    // Nothing waits for the refusal in the background, so the position read being asked is what shows the registration
    // reached the point where it is either refused or started. A subscription that was wrongly started would be
    // subscribed in the wrapped model by then or shortly after, which is why the absence is observed for a while.
    // Only ANSWERS_NOTHING is covered here. A failing read is retried with backoff and never subscribes, so from outside
    // a retry and a refusal look the same within the window and the test would pass either way.
    @Test
    void the_application_context_starts_without_subscribing_a_background_default_resume_behavior_now_subscription_that_cannot_have_its_start_position_recorded() {
        // Given
        ApplicationContextRunner runner = runner(FeedPosition.ANSWERS_NOTHING).withBean(BackgroundNowHolder.class);

        // When
        runner.run(context -> {
            // Then
            assertThat(context).hasNotFailed();
            UnreadableFeedDelegate delegate = (UnreadableFeedDelegate) context.getBean(RecordingDelegate.Holder.class).delegate();
            assertThat(delegate.positionAsked.await(5, TimeUnit.SECONDS)).describedAs("the feed position was asked for").isTrue();
            assertThat(becomesTrueWithin(() -> delegate.isSubscribed(NOW_ID), 500)).describedAs("subscribed in the wrapped model").isFalse();
        });
    }

    // In manual mode nothing waits for the subscription to start, so the context starts whatever the feed answers. The
    // registration is still made, so the feed position is asked for and the refusal it gets does not fail the context.
    @Test
    void the_application_context_starts_without_subscribing_a_default_resume_behavior_now_subscription_in_manual_mode_that_cannot_have_its_start_position_recorded() {
        // Given
        ApplicationContextRunner runner = runner(FeedPosition.ANSWERS_NOTHING)
                .withPropertyValues("occurrent.subscription.mode=manual")
                .withBean(NowHolder.class);

        // When
        runner.run(context -> {
            // Then
            assertThat(context).hasNotFailed();
            UnreadableFeedDelegate delegate = (UnreadableFeedDelegate) context.getBean(RecordingDelegate.Holder.class).delegate();
            assertThat(delegate.positionAsked.await(5, TimeUnit.SECONDS)).describedAs("the feed position was asked for").isTrue();
            assertThat(becomesTrueWithin(() -> delegate.isSubscribed(NOW_ID), 500)).describedAs("subscribed in the wrapped model").isFalse();
        });
    }

    private static ApplicationContextRunner runner(FeedPosition feedPosition) {
        return new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(BaseConfiguration.class, DurableModelConfiguration.class)
                .withBean(UnreadableFeedDelegate.class, () -> new UnreadableFeedDelegate(feedPosition.answer));
    }

    private static void awaitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("The condition did not hold within 5 seconds");
            }
            Thread.sleep(10);
        }
    }

    // Whether the condition holds at some point within the time given
    private static boolean becomesTrueWithin(BooleanSupplier condition, long millis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }

    // A wrapped model that cannot say where its feed is
    static class UnreadableFeedDelegate extends RecordingDelegate {
        final CountDownLatch positionAsked = new CountDownLatch(1);
        private final Supplier<Mono<Checkpoint>> position;

        UnreadableFeedDelegate(Supplier<Mono<Checkpoint>> position) {
            this.position = position;
        }

        // globalCheckpointAsOfNow() of the recording delegate answers this
        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.defer(() -> {
                positionAsked.countDown();
                return position.get();
            });
        }
    }

    // The composition the reactive MongoDB starter builds, with a delegate that cannot be read in place of the MongoDB one
    @Configuration(proxyBeanMethods = false)
    static class DurableModelConfiguration {
        @Bean
        RecordingDelegate.Holder recordingDelegate(UnreadableFeedDelegate delegate) {
            return new RecordingDelegate.Holder(delegate);
        }

        @Bean
        ParkingCheckpointStorage checkpointStorage() {
            return new ParkingCheckpointStorage();
        }

        @Bean(destroyMethod = "shutdown")
        ReactorDurableSubscriptionModel durableSubscriptionModel(RecordingDelegate.Holder delegate, ParkingCheckpointStorage storage,
                                                                 ObjectProvider<ReactorDurableSubscriptionModelConfig> config) {
            return new ReactorDurableSubscriptionModel(delegate.delegate(), storage, config.getIfAvailable(() -> new ReactorDurableSubscriptionModelConfig(event -> false)));
        }

        @Bean
        Subscriptions<TestEvent> subscriptions(ReactorDurableSubscriptionModel model, CloudEventConverter<TestEvent> converter) {
            return new Subscriptions<>(model, converter);
        }

        @Bean
        DcbSubscriptions<TestEvent> dcbSubscriptions(ReactorDurableSubscriptionModel model, CloudEventConverter<TestEvent> converter) {
            return new DcbSubscriptions<>(model, converter);
        }
    }

    static class NowHolder {
        @Subscription(id = NOW_ID, startAt = StartPosition.NOW)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    static class DcbNowHolder {
        @DcbSubscription(id = NOW_ID, startAt = StartPosition.NOW)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    static class NowSameAsStartAtHolder {
        @Subscription(id = NOW_ID, startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    static class BackgroundNowHolder {
        @Subscription(id = NOW_ID, startAt = StartPosition.NOW, startupMode = StartupMode.BACKGROUND)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }
}
