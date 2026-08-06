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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

/**
 * Fast, no-Mongo regression guard for the dual-mode {@link ReactorCatchupSubscriptionModel} routing decision. Before
 * the fix, {@code routesToDcb} used a position-only heuristic, so a stream subscription with an
 * {@link StreamSubscriptionFilter} and a {@link GlobalCheckpoint} start was misrouted to the DCB inner
 * model, which then rejected it with "only supports a DcbSubscriptionFilter". The fix routes by filter type first: a
 * {@link DcbSubscriptionFilter} always goes to DCB, an {@link StreamSubscriptionFilter} always goes to stream, and
 * only a {@code null} filter falls back to the position heuristic.
 * <p>
 * Both inner models validate the filter type before doing anything else (see their {@code subscribe} methods), so a
 * stub {@link CheckpointAwareSubscriptionModel} reporting no resume token is enough to observe routing: reaching the
 * inner model's "no resume token" {@link IllegalStateException} proves the filter check was passed, i.e. the
 * subscription was routed to the model that accepts that filter type. Reaching the other model's
 * {@link IllegalArgumentException} would prove misrouting.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupSubscriptionModelRoutingTest {

    @Test
    void a_stream_filter_with_a_global_position_start_routes_to_the_stream_model_not_dcb() {
        ReactorCatchupSubscriptionModel catchup = dualMode();

        // Routed to the stream model: passes the stream filter-type check, then fails loud on the missing resume
        // token. Before the fix this scenario produced the DCB model's IllegalArgumentException instead.
        StepVerifier.create(catchup.subscribe(StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0))))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void a_dcb_filter_routes_to_the_dcb_model() {
        ReactorCatchupSubscriptionModel catchup = dualMode();

        // Routed to the DCB model: passes the DCB filter-type check, then fails loud on the missing resume token.
        StepVerifier.create(catchup.subscribe(DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), StartAt.checkpoint(GlobalCheckpoint.of(0))))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void an_agnostic_filter_routes_to_the_neutral_unscoped_model_not_stream_or_dcb() {
        ReactorCatchupSubscriptionModel catchup = dualMode();

        // A null/agnostic capability scope: routed to the neutral inner model, which accepts any SubscriptionFilter
        // (including AgnosticSubscriptionFilter) and fails loud on the missing resume token, same as the scoped models.
        // Reaching either scoped model's filter-type IllegalArgumentException would prove misrouting.
        StepVerifier.create(catchup.subscribe(AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0))))
                .expectErrorSatisfies(error -> {
                    if (error instanceof IllegalArgumentException illegalArgumentException) {
                        throw new AssertionError("Agnostic subscription was misrouted to a capability-scoped model", illegalArgumentException);
                    }
                })
                .verify();
    }

    @Test
    void the_pre_fix_misrouting_cannot_recur_a_stream_filter_with_a_global_position_start_never_produces_the_dcb_rejection() {
        ReactorCatchupSubscriptionModel catchup = dualMode();

        // The regression was specifically the DCB model's "only supports a DcbSubscriptionFilter" rejection leaking
        // through for a stream subscription. Assert the error is not that IllegalArgumentException, of any message.
        StepVerifier.create(catchup.subscribe(StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0))))
                .expectErrorSatisfies(error -> {
                    if (error instanceof IllegalArgumentException illegalArgumentException) {
                        throw new AssertionError("Stream subscription was misrouted to the DCB model", illegalArgumentException);
                    }
                })
                .verify();
    }

    private static ReactorCatchupSubscriptionModel dualMode() {
        return new ReactorCatchupSubscriptionModel(new NoTokenCheckpointModel(), new UnusedPositionOrderedReader(), new UnusedDcbEventStore(), null, null);
    }

    private static final class NoTokenCheckpointModel implements CheckpointAwareSubscriptionModel {
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

    private static final class UnusedDcbEventStore implements DcbEventStore {
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
