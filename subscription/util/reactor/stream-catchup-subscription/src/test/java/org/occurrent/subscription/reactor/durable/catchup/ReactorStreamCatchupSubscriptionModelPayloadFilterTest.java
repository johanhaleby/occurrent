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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.occurrent.condition.Condition.eq;

/**
 * This model re-checks the filter in memory on its live tail, so that a backend which does not honor the filter
 * server-side still only delivers matching events. It has no reader for an event's payload, so a condition on a
 * {@code data} field is treated as already satisfied and the store is trusted for it (ADR 92). Before that, the
 * re-check threw on the first event delivered to any subscription filtering on a payload field.
 * <p>
 * The two branches are tested separately on purpose. Straight-to-live and the post-catch-up handover are reached by
 * different start positions and each has its own copy of the predicate, so one test would leave the other unproven.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelPayloadFilterTest {

    private static final Filter BIG_AMOUNTS = Filter.data("amount", eq(42));

    @Test
    void a_payload_filter_delivers_on_the_straight_to_live_branch() {
        LiveOnlySubscriptionModel live = new LiveOnlySubscriptionModel(event("live", 1));
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(live, new UnusedPositionOrderedReader());

        // StartAt.now() is not a catch-up position, so this takes the branch that subscribes and filters in process.
        StepVerifier.create(catchup.subscribe(BIG_AMOUNTS, StartAt.now()).map(CloudEvent::getId))
                .expectNext("live")
                .verifyComplete();
    }

    @Test
    void a_payload_filter_delivers_on_the_post_catchup_handover_branch() {
        LiveOnlySubscriptionModel live = new LiveOnlySubscriptionModel(event("live", 2));
        ReplayingPositionOrderedReader reader = new ReplayingPositionOrderedReader(event("replayed", 1));
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(live, reader);

        // A global checkpoint start goes through the replay pipeline, whose live predicate is a separate copy.
        StepVerifier.create(catchup.subscribe(BIG_AMOUNTS, StartAt.checkpoint(GlobalCheckpoint.of(0))).map(CloudEvent::getId))
                .expectNext("replayed", "live")
                .verifyComplete();
    }

    @Test
    void an_attribute_condition_alongside_a_payload_condition_is_still_enforced_in_process() {
        // The store is trusted for the payload, not for everything: a backend that ignores the filter entirely is
        // still held to the part this model can check for itself.
        LiveOnlySubscriptionModel live = new LiveOnlySubscriptionModel(event("wrong-type", 1, "SomethingElseHappened"));
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(live, new UnusedPositionOrderedReader());

        StepVerifier.create(catchup.subscribe(Filter.type("SomethingHappened").and(BIG_AMOUNTS), StartAt.now()))
                .verifyComplete();
    }

    private static CloudEvent event(String id, long position) {
        return event(id, position, "SomethingHappened");
    }

    private static CloudEvent event(String id, long position, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .withDataContentType("application/json")
                .withData("{\"amount\":42}".getBytes(StandardCharsets.UTF_8))
                .withExtension(OccurrentCloudEventExtension.POSITION, position)
                .build();
    }

    // Emits the given events on the live path and reports a resume token, so a replay start is allowed to proceed.
    private static final class LiveOnlySubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final CloudEvent[] events;

        private LiveOnlySubscriptionModel(CloudEvent... events) {
            this.events = events;
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(GlobalCheckpoint.of(0));
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.just(events);
        }
    }

    private static final class ReplayingPositionOrderedReader implements PositionOrderedReader {
        private final CloudEvent[] history;

        private ReplayingPositionOrderedReader(CloudEvent... history) {
            this.history = history;
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.just(history);
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(1L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private static final class UnusedPositionOrderedReader implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("readInPositionOrder must not be called on the straight-to-live branch"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.error(new AssertionError("currentPosition must not be called on the straight-to-live branch"));
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }
}
