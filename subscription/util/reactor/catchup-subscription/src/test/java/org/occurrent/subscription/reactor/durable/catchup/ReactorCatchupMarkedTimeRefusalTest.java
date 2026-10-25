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
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReads;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Only a blocking catch-up of a stream subscription stores the marked form of a time replay, and a reactor catch-up
 * replays only by position. Every reactor catch-up given that form refuses it before it reads, subscribes or delivers
 * anything, rather than taking it for a global position.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupMarkedTimeRefusalTest {

    private static final Checkpoint MARKED_TIME = new StringBasedCheckpoint(
            CatchupCheckpoints.format("2026-10-01T12:00:00Z", "2026-10-01T11:00:00Z", new StringBasedCheckpoint("live-start")));
    private static final DcbCriteria CRITERIA = DcbCriteria.tags(Tag.parse("name:1"));

    private final List<String> touched = new CopyOnWriteArrayList<>();
    private final List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
    private final Function<CloudEvent, Mono<Void>> action = event -> Mono.fromRunnable(() -> delivered.add(event));

    @Test
    void a_named_stream_catchup_refuses_a_marked_time_synchronously() {
        // Given
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NamedModel(), new Reader(), Filter.all());

        // When
        // Then
        assertThatThrownBy(() -> catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(MARKED_TIME), action))
                .as("what the named subscribe throws")
                .isInstanceOf(CatchupCannotResumeException.class)
                .hasMessageContainingAll("sub", MARKED_TIME.asString(), "replays by time", "StreamCatchupSubscriptionModel", "delete the checkpoint");
        assertNothingReadSubscribedOrDelivered();
    }

    @Test
    void an_unnamed_stream_catchup_refuses_a_marked_time_with_an_error_signal() {
        // Given
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NamedModel(), new Reader(), Filter.all());

        // When
        // Then
        StepVerifier.create(unnamed(() -> catchup.subscribe(StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(MARKED_TIME))))
                .expectErrorSatisfies(error -> assertThat(error).as("the error signal")
                        .isInstanceOf(CatchupCannotResumeException.class).hasMessageContainingAll(MARKED_TIME.asString(), "replays by time", "StreamCatchupSubscriptionModel", "delete the checkpoint"))
                .verify();
        assertNothingReadSubscribedOrDelivered();
    }

    @Test
    void a_named_dcb_catchup_refuses_a_marked_time_synchronously() {
        // Given
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NamedModel(), new Store(), CRITERIA);

        // When
        // Then
        assertThatThrownBy(() -> catchup.subscribe("sub", DcbSubscriptionFilter.filter(CRITERIA), StartAt.checkpoint(MARKED_TIME), action))
                .as("what the named subscribe throws")
                .isInstanceOf(CatchupCannotResumeException.class)
                .hasMessageContainingAll("sub", MARKED_TIME.asString(), "replays by time", "StreamCatchupSubscriptionModel", "delete the checkpoint");
        assertNothingReadSubscribedOrDelivered();
    }

    @Test
    void an_unnamed_dcb_catchup_refuses_a_marked_time_with_an_error_signal() {
        // Given
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NamedModel(), new Store(), CRITERIA);

        // When
        // Then
        StepVerifier.create(unnamed(() -> catchup.subscribe(DcbSubscriptionFilter.filter(CRITERIA), StartAt.checkpoint(MARKED_TIME))))
                .expectErrorSatisfies(error -> assertThat(error).as("the error signal")
                        .isInstanceOf(CatchupCannotResumeException.class).hasMessageContainingAll(MARKED_TIME.asString(), "replays by time", "StreamCatchupSubscriptionModel", "delete the checkpoint"))
                .verify();
        assertNothingReadSubscribedOrDelivered();
    }

    @Test
    void a_dual_mode_catchup_refuses_a_marked_time_without_a_filter_both_named_and_unnamed() {
        // Given
        ReactorCatchupSubscriptionModel catchup = new ReactorCatchupSubscriptionModel(new NamedModel(), new Reader(), new Store(), CRITERIA, Filter.all());

        // When
        // Then
        assertThatThrownBy(() -> catchup.subscribe("sub", null, StartAt.checkpoint(MARKED_TIME), action))
                .as("what the named subscribe throws")
                .isInstanceOf(CatchupCannotResumeException.class)
                .hasMessageContainingAll("sub", MARKED_TIME.asString(), "replays by time", "StreamCatchupSubscriptionModel", "delete the checkpoint");
        StepVerifier.create(unnamed(() -> catchup.subscribe(null, StartAt.checkpoint(MARKED_TIME))))
                .expectErrorSatisfies(error -> assertThat(error).as("the error signal")
                        .isInstanceOf(CatchupCannotResumeException.class).hasMessageContainingAll(MARKED_TIME.asString(), "replays by time", "StreamCatchupSubscriptionModel", "delete the checkpoint"))
                .verify();
        assertNothingReadSubscribedOrDelivered();
        assertThat(catchup.isRunning("sub")).as("whether the refused subscription runs").isFalse();
    }

    // The unnamed subscribe reports a refusal through its publisher, so it returns one rather than throwing
    private static Flux<CloudEvent> unnamed(Supplier<Flux<CloudEvent>> subscribe) {
        AtomicReference<Flux<CloudEvent>> publisher = new AtomicReference<>();
        assertThatCode(() -> publisher.set(subscribe.get())).as("what the unnamed subscribe throws").doesNotThrowAnyException();
        return publisher.get();
    }

    private void assertNothingReadSubscribedOrDelivered() {
        assertThat(touched).as("what the refusal read or subscribed").isEmpty();
        assertThat(delivered).as("what the refusal delivered").isEmpty();
    }

    private final class NamedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            touched.add("subscribe");
            return Flux.never();
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            touched.add("globalCheckpoint");
            return Mono.just(new StringBasedCheckpoint("live-start"));
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            touched.add("globalCheckpointAsOfNow");
            return Mono.just(new StringBasedCheckpoint("live-start"));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            touched.add("subscribe " + subscriptionId);
            throw new AssertionError("a refused subscription must not reach the wrapped model");
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            return Mono.empty();
        }

        @Override
        public void stop() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
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
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }
    }

    private final class Reader implements PositionOrderedReader, CatchupReads {
        @Override
        public Mono<CatchupReadScope> openCatchupReadScope(@Nullable String notBefore) {
            touched.add("openCatchupReadScope");
            return CatchupReads.unscoped(this).openCatchupReadScope(notBefore);
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            touched.add("readInPositionOrder");
            return Flux.empty();
        }

        @Override
        public Mono<Long> currentPosition() {
            touched.add("currentPosition");
            return Mono.just(0L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private final class Store implements DcbEventStore, DcbCatchupReads {
        @Override
        public Mono<DcbCatchupReadScope> openDcbCatchupReadScope(@Nullable String notBefore) {
            touched.add("openDcbCatchupReadScope");
            return DcbCatchupReads.unscoped(this).openDcbCatchupReadScope(notBefore);
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            touched.add("read");
            return Mono.error(new AssertionError("a refused subscription must not read"));
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
