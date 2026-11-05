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

package org.occurrent.subscription.reactor.durable.catchup;

import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReads;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDcbCatchupSubscriptionModelTest {

    @Test
    void a_replay_start_fails_loudly_when_the_model_reports_no_resume_token() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore());

        // Without a resume token the handover from the replay to live cannot be guaranteed loss-free, so the catch-up
        // errors instead of replaying. The event store is never read, the failure happens before the first replay read.
        StepVerifier.create(catchup.subscribe(DcbCriteria.tags(Tag.parse("name:1")), DcbStartAt.beginning()))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void a_start_after_a_dcb_position_above_zero_is_refused_before_anything_is_read() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore());

        StepVerifier.create(catchup.subscribe(DcbCriteria.tags(Tag.parse("name:1")), DcbStartAt.afterPosition(2)))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(CatchupCannotResumeException.class)
                        .hasMessageContaining("after position 2")
                        .hasMessageContaining("EventMetadata.getCheckpoint()"))
                .verify();
    }

    @Test
    void a_live_start_does_not_require_a_resume_token() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore());

        // A non-replay start goes straight to live through the facade, so it neither needs a resume token nor reads
        // history. The fail-loud rule is scoped to replay starts only.
        StepVerifier.create(catchup.subscribe(DcbCriteria.tags(Tag.parse("name:1")), DcbStartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_with_a_dcb_filter_goes_live_for_a_non_replay_start() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore());

        StepVerifier.create(catchup.subscribe(DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), StartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_uses_the_default_query_when_no_filter_is_given() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore(), DcbCriteria.all());

        StepVerifier.create(catchup.subscribe(null, StartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_without_a_filter_or_default_query_fails() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore());

        StepVerifier.create(catchup.subscribe(null, StartAt.now()))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void generic_subscribe_rejects_a_non_dcb_filter() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore());

        StepVerifier.create(catchup.subscribe(StreamSubscriptionFilter.filter(Filter.all()), StartAt.now()))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void a_pause_or_a_resume_after_shutdown_throws_SubscriptionModelShutdownException() {
        ShutDownLikeMongo wrapped = new ShutDownLikeMongo();
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(wrapped, new UnusedDcbEventStore());
        catchup.shutdown();

        Throwable pause = catchThrowable(() -> catchup.pauseSubscription("x"));
        Throwable resume = catchThrowable(() -> catchup.resumeSubscription("x"));

        assertAll(
                () -> assertThat(pause).as("what a pause throws").isExactlyInstanceOf(SubscriptionModelShutdownException.class),
                () -> assertThat(resume).as("what a resume throws").isExactlyInstanceOf(SubscriptionModelShutdownException.class),
                () -> assertThat(wrapped.callsAfterShutdown).as("calls the wrapped model got after its shutdown").isEmpty()
        );
    }

    // "live" went straight to live, "replayed" replayed the history, which is empty here, and handed over
    @Test
    void a_pause_or_a_resume_of_a_subscription_after_shutdown_throws_SubscriptionModelShutdownException() {
        ShutDownLikeMongo wrapped = new ShutDownLikeMongo();
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(wrapped, new EmptyDcbEventStore());
        DcbSubscriptionFilter filter = DcbSubscriptionFilter.filter(DcbCriteria.all());
        catchup.subscribe("live", filter, StartAt.now(), cloudEvent -> Mono.empty());
        catchup.subscribe("replayed", filter, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(5));
        assertThat(wrapped.subscribed).as("the subscriptions the wrapped model holds before the shutdown").containsExactlyInAnyOrder("live", "replayed");
        catchup.shutdown();

        assertAll(
                () -> assertThat(catchThrowable(() -> catchup.pauseSubscription("live"))).as("what a pause of the subscription that went straight to live throws")
                        .isExactlyInstanceOf(SubscriptionModelShutdownException.class),
                () -> assertThat(catchThrowable(() -> catchup.resumeSubscription("live"))).as("what a resume of the subscription that went straight to live throws")
                        .isExactlyInstanceOf(SubscriptionModelShutdownException.class),
                () -> assertThat(catchThrowable(() -> catchup.pauseSubscription("replayed"))).as("what a pause of the subscription that replayed throws")
                        .isExactlyInstanceOf(SubscriptionModelShutdownException.class),
                () -> assertThat(catchThrowable(() -> catchup.resumeSubscription("replayed"))).as("what a resume of the subscription that replayed throws")
                        .isExactlyInstanceOf(SubscriptionModelShutdownException.class),
                () -> assertThat(wrapped.callsAfterShutdown).as("calls the wrapped model got after its shutdown").isEmpty()
        );
    }

    // After shutdown() it throws a plain IllegalStateException from a pause or a resume, as ReactorMongoSubscriptionModel
    // did in 0.34.0, and records every call it gets
    private static final class ShutDownLikeMongo implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        private static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> callsAfterShutdown = new CopyOnWriteArrayList<>();
        final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        private volatile boolean shutdown;

        private void called(String call) {
            if (shutdown) {
                callsAfterShutdown.add(call);
            }
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(TOKEN);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.just(TOKEN);
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used"));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            called("subscribe:" + subscriptionId);
            if (shutdown) {
                throw new IllegalStateException("ShutDownLikeMongo is shutdown");
            }
            subscribed.add(subscriptionId);
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            called("cancel:" + subscriptionId);
            return Mono.empty();
        }

        @Override
        public void stop() {
            called("stop");
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            called("start");
        }

        @Override
        public void shutdown() {
            called("shutdown");
            shutdown = true;
            subscribed.clear();
        }

        @Override
        public boolean isRunning() {
            return !shutdown;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscribed.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            called("resume:" + subscriptionId);
            throw shutdown ? new IllegalStateException("ShutDownLikeMongo is shutdown") : new UnknownSubscriptionException(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            called("pause:" + subscriptionId);
            throw shutdown ? new IllegalStateException("ShutDownLikeMongo is shutdown") : new UnknownSubscriptionException(subscriptionId);
        }
    }

    private static final class NoTokenSubscriptionModel implements CheckpointAwareSubscriptionModel {
        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.empty();
        }

        @Override
        // No position, as globalCheckpoint() answers
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.empty();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }
    }

    // No events, with the head at position 0
    private static final class EmptyDcbEventStore implements DcbEventStore, DcbCatchupReads {
        @Override
        public Mono<DcbCatchupReadScope> openDcbCatchupReadScope(@Nullable String notBefore) {
            return DcbCatchupReads.unscoped(this).openDcbCatchupReadScope(notBefore);
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.just(new DcbEventStream(List.of(), 0, DcbConsistencyToken.of(criteria, 0)));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return Mono.error(new AssertionError("append must not be called"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return Mono.error(new AssertionError("append must not be called"));
        }
    }

    private static final class UnusedDcbEventStore implements DcbEventStore, DcbCatchupReads {
        @Override
        public Mono<DcbCatchupReadScope> openDcbCatchupReadScope(@Nullable String notBefore) {
            return DcbCatchupReads.unscoped(this).openDcbCatchupReadScope(notBefore);
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.error(new AssertionError("read must not be called when the catch-up fails loudly"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return Mono.error(new AssertionError("append must not be called"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return Mono.error(new AssertionError("append must not be called"));
        }
    }
}
