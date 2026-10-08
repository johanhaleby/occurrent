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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;
import org.occurrent.subscription.api.reactor.QuietPositionReportingSubscriptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ReactorStreamCatchupSubscriptionModel} answers the quiet position capability with the one of the model it
 * wraps, and answers every other capability for itself.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelQuietPositionCapabilityTest {

    @Test
    void the_quiet_position_capability_is_the_one_of_the_wrapped_model() {
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();

        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedPositionOrderedReader());

        assertThat(QuietPositionReportingSubscriptions.findIn(catchup)).as("quiet position capability of a stream catch-up model").containsSame(wrapped);
    }

    @Test
    void there_is_no_quiet_position_capability_when_the_wrapped_model_has_none() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new PlainModel(), new UnusedPositionOrderedReader());

        assertThat(QuietPositionReportingSubscriptions.findIn(catchup)).as("quiet position capability of a stream catch-up model over a model without it").isEmpty();
    }

    @Test
    void no_other_capability_is_answered_with_the_wrapped_model() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new QuietPositionReportingModel(), new UnusedPositionOrderedReader());

        assertThat(catchup.capability(IntrospectableSubscriptions.class)).as("introspection capability of a stream catch-up model over a model that has it").isEmpty();
    }

    // A wrapped model with the quiet position capability, and with introspection so a test can tell it isn't passed on
    private static final class QuietPositionReportingModel extends PlainModel implements QuietPositionReportingSubscriptions, IntrospectableSubscriptions {
        @Override
        public void addQuietPositionListener(QuietPositionListener listener) {
        }

        @Override
        public void removeQuietPositionListener(QuietPositionListener listener) {
        }

        @Override
        public Set<String> subscriptionIds() {
            return Set.of();
        }
    }

    private static class PlainModel implements CheckpointAwareSubscriptionModel {
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
}
