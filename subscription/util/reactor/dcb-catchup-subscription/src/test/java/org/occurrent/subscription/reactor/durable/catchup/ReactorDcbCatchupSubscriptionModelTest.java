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
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDcbCatchupSubscriptionModelTest {

    @Test
    void a_replay_start_fails_loudly_when_the_model_reports_no_resume_token() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore());

        // Without a resume token the handover from the replay to live cannot be guaranteed loss-free, so the catch-up
        // errors instead of replaying. The event store is never read, the failure happens before the first replay read.
        StepVerifier.create(catchup.subscribe(DcbQuery.tags("name:1"), DcbStartAt.beginning()))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void a_live_start_does_not_require_a_resume_token() {
        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedDcbEventStore());

        // A non-replay start goes straight to live through the facade, so it neither needs a resume token nor reads
        // history. The fail-loud rule is scoped to replay starts only.
        StepVerifier.create(catchup.subscribe(DcbQuery.tags("name:1"), DcbStartAt.now()))
                .verifyComplete();
    }

    private static final class NoTokenSubscriptionModel implements PositionAwareSubscriptionModel {
        @Override
        public Mono<SubscriptionPosition> globalSubscriptionPosition() {
            return Mono.empty();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }
    }

    private static final class UnusedDcbEventStore implements DcbEventStore {
        @Override
        public Mono<DcbEventStream> read(DcbQuery query, DcbReadOptions options) {
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
