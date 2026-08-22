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

/**
 * Guards which subscription model type a caller's {@link StartAt#dynamic(Function)} is shown when the subscription goes
 * through the {@link ReactorCatchupSubscriptionModel} dispatcher.
 * <p>
 * The dispatcher routes to a mode-specific inner model, and each inner model used to resolve the caller's
 * {@code StartAt} against its own class. A caller that branches on {@code ReactorCatchupSubscriptionModel}, which is the
 * type it holds and the only one of the three the dispatcher's own constructors expose, therefore never matched. The
 * blocking {@code CatchupSubscriptionModel} has always reported its own class to callers for this reason, so the two
 * stacks disagreed on the same question.
 * <p>
 * The stubs are enough because the context is resolved before anything is read: the wrapped model reports no resume
 * token and neither the event store nor the position reader is ever touched.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupSubscriptionModelStartAtContextTest {

    @Test
    void the_cold_primitive_reports_the_dispatcher_type_for_every_filter_it_routes() {
        RecordingStartAt startAt = new RecordingStartAt();
        ReactorCatchupSubscriptionModel catchup = dualMode();

        StepVerifier.create(catchup.subscribe(StreamSubscriptionFilter.filter(Filter.all()), startAt.startAt())).verifyComplete();
        StepVerifier.create(catchup.subscribe(DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), startAt.startAt())).verifyComplete();
        StepVerifier.create(catchup.subscribe(AgnosticSubscriptionFilter.filter(Filter.all()), startAt.startAt())).verifyComplete();

        // Every one of the three inner models the dispatcher can route to, including the capability-agnostic one that
        // shares its class with the stream model, has to report the dispatcher rather than itself.
        assertThat(startAt.observedTypes).isNotEmpty().containsOnly(ReactorCatchupSubscriptionModel.class);
    }

    @Test
    void the_named_path_reports_the_dispatcher_type_for_every_filter_it_routes() {
        RecordingStartAt startAt = new RecordingStartAt();
        ReactorCatchupSubscriptionModel catchup = dualMode();

        catchup.subscribe("stream-subscription", StreamSubscriptionFilter.filter(Filter.all()), startAt.startAt(), __ -> Mono.empty());
        catchup.subscribe("dcb-subscription", DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), startAt.startAt(), __ -> Mono.empty());
        catchup.subscribe("agnostic-subscription", AgnosticSubscriptionFilter.filter(Filter.all()), startAt.startAt(), __ -> Mono.empty());

        assertThat(startAt.observedTypes).isNotEmpty().containsOnly(ReactorCatchupSubscriptionModel.class);
    }

    @Test
    void a_dynamic_start_position_branching_on_the_dispatcher_type_takes_the_catch_up_branch() {
        // What a caller actually writes: replay from the beginning when this is the catch-up dispatcher, go live
        // otherwise. Before the fix the inner model reported its own class, so this always took the live branch and the
        // catch-up never ran. Reaching the "no resume token" IllegalStateException is what proves the replay branch was
        // taken, since a live start over this wrapped model completes empty instead.
        StartAt replayWhenDispatched = StartAt.dynamic(context -> context.hasSubscriptionModelType(ReactorCatchupSubscriptionModel.class)
                ? StartAt.checkpoint(GlobalCheckpoint.of(0))
                : StartAt.subscriptionModelDefault());

        StepVerifier.create(dualMode().subscribe(StreamSubscriptionFilter.filter(Filter.all()), replayWhenDispatched))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void an_inner_model_used_on_its_own_still_reports_its_own_type() {
        // The injected type is the dispatcher's only when the dispatcher builds the inner model. A stream catch-up
        // model wired directly, which is the DCB-free variant its javadoc offers, is still the type a caller holds.
        RecordingStartAt startAt = new RecordingStartAt();
        ReactorStreamCatchupSubscriptionModel streamCatchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenCheckpointModel(), new UnusedPositionOrderedReader(), Filter.all());

        StepVerifier.create(streamCatchup.subscribe(StreamSubscriptionFilter.filter(Filter.all()), startAt.startAt())).verifyComplete();

        assertThat(startAt.observedTypes).containsOnly(ReactorStreamCatchupSubscriptionModel.class);
    }

    private static ReactorCatchupSubscriptionModel dualMode() {
        return new ReactorCatchupSubscriptionModel(new NoTokenCheckpointModel(), new UnusedPositionOrderedReader(), new UnusedDcbEventStore(), null, Filter.all());
    }

    // A dynamic StartAt that records every subscription model type it is resolved against and always starts live, so a
    // subscription over the stubs below completes instead of replaying.
    private static final class RecordingStartAt {
        private final List<Class<?>> observedTypes = new CopyOnWriteArrayList<>();

        private StartAt startAt() {
            return StartAt.dynamic(context -> {
                observedTypes.add(context.subscriptionModelType());
                return StartAt.subscriptionModelDefault();
            });
        }
    }

    // Named so the catch-up models take their delegating path rather than refusing, and empty everywhere else so a live
    // subscription completes at once.
    private static final class NoTokenCheckpointModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
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
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void stop() {
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
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
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
            return Flux.error(new AssertionError("readInPositionOrder must not be called when the subscription starts live"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.error(new AssertionError("currentPosition must not be called when the subscription starts live"));
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private static final class UnusedDcbEventStore implements DcbEventStore {
        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.error(new AssertionError("read must not be called when the subscription starts live"));
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
