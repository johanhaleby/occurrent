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
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Pins what {@link ReactorCatchupSubscriptionModel} does with a subscription id it did not create.
 * <p>
 * The dispatcher holds three inner catch-up models and routes each named subscription to one of them, so a
 * per-subscription life-cycle call has to reach the right one. For an id it never created there is no routing decision
 * to look up, and the answer chosen here is to forward rather than to refuse: every inner model delegates to the same
 * wrapped model, so one forward through any of them is that model's own answer. Refusing would break the documented
 * behaviour of the calls that are allowed to be asked about an unknown id, such as cancelling one.
 * <p>
 * The forward has to happen exactly once. Fanning the call out to all three inner models, which is what the blocking
 * {@code CatchupSubscriptionModel} does for its two predicates, would reach the wrapped model three times, and pausing
 * an already paused subscription throws.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupSubscriptionModelOwnershipTest {

    private static final String NEVER_CREATED_HERE = "an-id-this-dispatcher-never-created";

    @Test
    void a_life_cycle_call_for_an_id_it_did_not_create_reaches_the_wrapped_model_exactly_once() {
        RecordingSubscriptionModel wrapped = new RecordingSubscriptionModel();
        ReactorCatchupSubscriptionModel catchup = dualModeOver(wrapped);

        catchup.isRunning(NEVER_CREATED_HERE);
        catchup.isPaused(NEVER_CREATED_HERE);
        catchup.pauseSubscription(NEVER_CREATED_HERE);
        catchup.resumeSubscription(NEVER_CREATED_HERE);
        catchup.cancelSubscription(NEVER_CREATED_HERE);

        // One call each, not one per inner model: the three inner models share this wrapped model, so a fan-out would
        // ask it the same question three times and pause it twice over.
        assertThat(wrapped.calls).containsExactly(
                "isRunning:" + NEVER_CREATED_HERE,
                "isPaused:" + NEVER_CREATED_HERE,
                "pauseSubscription:" + NEVER_CREATED_HERE,
                "resumeSubscription:" + NEVER_CREATED_HERE,
                "cancelSubscription:" + NEVER_CREATED_HERE);
    }

    @Test
    void it_does_not_refuse_an_id_it_did_not_create() {
        ReactorCatchupSubscriptionModel catchup = dualModeOver(new RecordingSubscriptionModel());

        // The dispatcher answers from the wrapped model rather than throwing on the grounds that it has no routing
        // record for the id. Whether a given call is legal for an unknown id is the wrapped model's contract, not this
        // model's, and cancelling one is explicitly an idempotent no-op.
        assertThatCode(() -> {
            catchup.isRunning(NEVER_CREATED_HERE);
            catchup.isPaused(NEVER_CREATED_HERE);
            catchup.cancelSubscription(NEVER_CREATED_HERE);
        }).doesNotThrowAnyException();
    }

    @Test
    void an_id_it_did_create_is_answered_by_the_model_it_was_routed_to() {
        RecordingSubscriptionModel wrapped = new RecordingSubscriptionModel();
        ReactorCatchupSubscriptionModel catchup = dualModeOver(wrapped);

        // A live start hands straight over to the wrapped model, so the routed inner model has no replay of its own to
        // answer from and the question reaches the wrapped model, once, exactly as for an id the dispatcher never saw.
        catchup.subscribe("routed-subscription", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        wrapped.calls.clear();

        catchup.isRunning("routed-subscription");

        assertThat(wrapped.calls).containsExactly("isRunning:routed-subscription");
    }

    private static ReactorCatchupSubscriptionModel dualModeOver(RecordingSubscriptionModel wrapped) {
        return new ReactorCatchupSubscriptionModel(wrapped, new UnusedPositionOrderedReader(), new UnusedDcbEventStore(), DcbCriteria.all(), Filter.all());
    }

    // Records every per-subscription life-cycle call it receives, so the test can count the forwards rather than only
    // observe their result.
    private static final class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        private final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.empty();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            calls.add("subscribe:" + subscriptionId);
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            calls.add("stop");
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            calls.add("start");
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            calls.add("isRunning:" + subscriptionId);
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            calls.add("isPaused:" + subscriptionId);
            return false;
        }

        @Override
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            calls.add("resumeSubscription:" + subscriptionId);
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            calls.add("pauseSubscription:" + subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            calls.add("cancelSubscription:" + subscriptionId);
        }
    }

    private record StartedSubscription(String id) implements SubscriptionHandle {
        @Override
        public Mono<Void> waitUntilStarted() {
            return Mono.empty();
        }
    }

    private static final class UnusedPositionOrderedReader implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("readInPositionOrder must not be called when nothing replays"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.error(new AssertionError("currentPosition must not be called when nothing replays"));
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private static final class UnusedDcbEventStore implements DcbEventStore {
        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.error(new AssertionError("read must not be called when nothing replays"));
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
