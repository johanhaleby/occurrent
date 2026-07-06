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
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelTest {

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
}
