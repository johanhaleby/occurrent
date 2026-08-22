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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelTest {

    @Test
    void cancelling_a_named_subscription_before_its_replay_hands_over_fails_the_started_signal_instead_of_completing_it() {
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        // The reader never finishes its first window, so the replay is still in flight (no handover) when cancel()
        // runs below, which is exactly the race NamedCatchupSupport.cancelSubscription's "not yet handed over" branch covers.
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new StuckPositionOrderedReader());

        SubscriptionHandle subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        catchup.cancelSubscription("sub");

        StepVerifier.create(subscription.waitUntilStarted())
                .verifyErrorSatisfies(throwable -> assertThat(throwable)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("sub")
                        .hasMessageContaining("was cancelled before it started"));
        assertThat(wrapped.subscribeCalls)
                .as("the id never reached the wrapped model, so cancelling here must not either")
                .isEmpty();
    }

    // The contract a recording projection is told, rather than one it reads per delivery. The start arrives before
    // anything this catch-up delivers, the boundary arrives after the history that was already there and before the
    // events written since the catch-up started, and both name the same catch-up.
    @Test
    void tells_a_listener_when_a_catch_up_starts_and_when_its_history_has_been_read() {
        GrowingPositionOrderedReader reader = new GrowingPositionOrderedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NamedRecordingSubscriptionModel(), reader);

        List<String> signals = new CopyOnWriteArrayList<>();
        List<Object> episodes = new CopyOnWriteArrayList<>();
        boolean sendsThem = catchup.listenForCatchup("sub", new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                signals.add("started");
                episodes.add(episode);
            }

            @Override
            public void historyRead(Object episode) {
                signals.add("historyRead");
                episodes.add(episode);
            }
        });
        assertThat(sendsThem).isTrue();

        SubscriptionHandle subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> signals.add("delivered")));
        StepVerifier.create(subscription.waitUntilStarted()).verifyComplete();

        assertThat(signals).containsExactly("started", "delivered", "historyRead", "delivered");
        assertThat(episodes).hasSize(2);
        assertThat(episodes.get(0)).isSameAs(episodes.get(1));
    }

    @Test
    void a_replay_start_fails_loudly_when_the_model_reports_no_resume_token() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        // Without a resume token the handover from the replay to live cannot be guaranteed loss-free, so the catch-up
        // errors instead of replaying. The store is never read, the failure happens before the first replay read.
        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.checkpoint(GlobalCheckpoint.of(0))))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void a_live_start_does_not_require_a_resume_token() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        // A non-replay start goes straight to live through the facade, so it neither needs a resume token nor reads
        // history. The fail-loud rule is scoped to replay starts only.
        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_with_a_stream_filter_goes_live_for_a_non_replay_start() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        StepVerifier.create(catchup.subscribe(StreamSubscriptionFilter.filter(Filter.all()), StartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_uses_the_default_filter_when_no_filter_is_given() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader(), Filter.all());

        StepVerifier.create(catchup.subscribe((SubscriptionFilter) null, StartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_without_a_filter_or_default_filter_fails() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        StepVerifier.create(catchup.subscribe((SubscriptionFilter) null, StartAt.now()))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void generic_subscribe_rejects_a_non_stream_filter() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        StepVerifier.create(catchup.subscribe(DcbSubscriptionFilter.filter(DcbCriteria.all()), StartAt.now()))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    // One event already there and one more written while the history is being read, so the catch-up has a history to
    // read and something to deliver afterwards. The head grows on the second read, which is what the reconciliation
    // sees.
    private static final class GrowingPositionOrderedReader implements PositionOrderedReader {
        private final java.util.concurrent.atomic.AtomicInteger headReads = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = range.upToPosition().orElse(0L);
            return Flux.fromStream(java.util.stream.LongStream.rangeClosed(from, to).boxed()
                    .map(position -> io.cloudevents.core.builder.CloudEventBuilder.v1()
                            .withId("e" + position)
                            .withSource(java.net.URI.create("urn:test"))
                            .withType("type")
                            .build()));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.fromSupplier(() -> headReads.incrementAndGet() == 1 ? 1L : 2L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private static final class NoTokenSubscriptionModel implements CheckpointAwareSubscriptionModel {
        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.empty();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }
    }

    private static final class UnusedPositionOrderedReader implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("readInPositionOrder must not be called when the catch-up fails loudly"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.error(new AssertionError("currentPosition must not be called when the catch-up fails loudly"));
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Head sits ahead of the replay's start position, so there is a window to read, and that window never completes:
    // Flux.never() registers a subscriber and then neither emits nor terminates. subscribe() itself still returns
    // immediately (registering a subscriber is not blocking), so the calling thread is never stuck, and the replay
    // is simply left in flight, exactly as if a slow store had not answered the first page yet.
    private static final class StuckPositionOrderedReader implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.never();
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(10L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // A named subscription model with a resolvable checkpoint, so a catch-up wrapping it can capture a live token
    // and start replaying. Records every named subscribe/cancel it is asked to do, so a test can assert the wrapped
    // model was never told about a subscription whose replay never handed over.
    private static final class NamedRecordingSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        final List<String> subscribeCalls = new CopyOnWriteArrayList<>();
        final List<String> cancelCalls = new CopyOnWriteArrayList<>();

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("token"));
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            subscribeCalls.add(subscriptionId);
            return new SubscriptionHandle() {
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
        public void cancelSubscription(String subscriptionId) {
            cancelCalls.add(subscriptionId);
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            throw new AssertionError("resumeSubscription must not be called in this test");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }
    }
}
