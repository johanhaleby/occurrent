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
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

/**
 * Each catch-up model answers {@code globalCheckpointAsOfNow()} with the wrapped model's answer to the same method, not
 * with its {@code globalCheckpoint()}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupGlobalCheckpointAsOfNowTest {

    private static final Checkpoint AS_OF_NOW = new StringBasedCheckpoint("as of now");

    private final CheckpointAwareSubscriptionModel wrapped = new AnswersDifferentlyAsOfNow();

    @Test
    void the_stream_catch_up_model_asks_the_wrapped_model_as_of_now() {
        StepVerifier.create(new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedPositionOrderedReader()).globalCheckpointAsOfNow())
                .expectNext(AS_OF_NOW).verifyComplete();
    }

    @Test
    void the_dcb_catch_up_model_asks_the_wrapped_model_as_of_now() {
        StepVerifier.create(new ReactorDcbCatchupSubscriptionModel(wrapped, new UnusedDcbEventStore()).globalCheckpointAsOfNow())
                .expectNext(AS_OF_NOW).verifyComplete();
    }

    @Test
    void the_catch_up_model_for_streams_asks_the_wrapped_model_as_of_now() {
        StepVerifier.create(new ReactorCatchupSubscriptionModel(wrapped, new UnusedPositionOrderedReader(), null).globalCheckpointAsOfNow())
                .expectNext(AS_OF_NOW).verifyComplete();
    }

    @Test
    void the_catch_up_model_for_dcb_asks_the_wrapped_model_as_of_now() {
        StepVerifier.create(new ReactorCatchupSubscriptionModel(wrapped, new UnusedDcbEventStore(), null).globalCheckpointAsOfNow())
                .expectNext(AS_OF_NOW).verifyComplete();
    }

    @Test
    void the_catch_up_model_for_both_asks_the_wrapped_model_as_of_now() {
        StepVerifier.create(new ReactorCatchupSubscriptionModel(wrapped, new UnusedPositionOrderedReader(), new UnusedDcbEventStore(), null, null).globalCheckpointAsOfNow())
                .expectNext(AS_OF_NOW).verifyComplete();
    }

    private static final class AnswersDifferentlyAsOfNow implements CheckpointAwareSubscriptionModel {
        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("when subscribed to"));
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.just(AS_OF_NOW);
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }
    }

    private static final class UnusedPositionOrderedReader implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("readInPositionOrder must not be called"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.error(new AssertionError("currentPosition must not be called"));
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private static final class UnusedDcbEventStore implements DcbEventStore {
        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.error(new AssertionError("read must not be called"));
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
