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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReplayThenPushSubscriptionModelTest {

    @Test
    void bootstraps_history_then_delivers_the_live_feed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> delivered = new ArrayList<>();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")), 3);

        ReplayThenPushSubscriptionModel model = new ReplayThenPushSubscriptionModel(reader, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(delivered));

        assertThat(delivered).containsExactly("1", "2", "3");

        feed.accept(cloudEvent("4", "Updated")).block();
        assertThat(delivered).containsExactly("1", "2", "3", "4");
    }

    @Test
    void an_event_both_replayed_and_delivered_live_during_bootstrap_is_delivered_once() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> delivered = new ArrayList<>();
        CloudEvent e1 = cloudEvent("1", "Created");
        CloudEvent e2 = cloudEvent("2", "Updated");
        CloudEvent e3 = cloudEvent("3", "Updated");
        // While the replay streams, e2 also arrives live on the feed.
        PositionOrderedReader reader = reader(() -> Flux.just(e1, e2, e3).doOnNext(ce -> {
            if (ce == e2) {
                feed.accept(e2).subscribe();
            }
        }), 3);

        ReplayThenPushSubscriptionModel model = new ReplayThenPushSubscriptionModel(reader, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(delivered));

        assertThat(delivered).containsExactly("1", "2", "3");
    }

    @Test
    void a_late_committing_event_not_in_the_replay_arrives_via_the_feed_and_is_not_lost() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        List<String> delivered = new ArrayList<>();
        CloudEvent e1 = cloudEvent("1", "Created");
        CloudEvent e2 = cloudEvent("2", "Updated");
        CloudEvent late = cloudEvent("late", "Updated");
        boolean[] pushed = {false};
        PositionOrderedReader reader = reader(() -> Flux.just(e1, e2).doOnNext(ce -> {
            if (!pushed[0]) {
                pushed[0] = true;
                feed.accept(late).subscribe();
            }
        }), 2);

        ReplayThenPushSubscriptionModel model = new ReplayThenPushSubscriptionModel(reader, feed, null);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(delivered));

        assertThat(delivered).containsExactly("1", "2", "late");
    }

    @Test
    void a_restart_skips_the_replay_when_the_bootstrap_marker_exists() {
        InMemoryReactiveCheckpointStorage marker = new InMemoryReactiveCheckpointStorage();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1", "Created"), cloudEvent("2", "Updated")), 2);

        PushSubscriptionModel feed1 = new PushSubscriptionModel();
        List<String> firstRun = new ArrayList<>();
        new ReplayThenPushSubscriptionModel(reader, feed1, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(firstRun));
        assertThat(firstRun).containsExactly("1", "2");

        // Restart: fresh feed and model, same reader and marker. The replay is skipped.
        PushSubscriptionModel feed2 = new PushSubscriptionModel();
        List<String> secondRun = new ArrayList<>();
        new ReplayThenPushSubscriptionModel(reader, feed2, marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), recordInto(secondRun));
        assertThat(secondRun).isEmpty();

        feed2.accept(cloudEvent("3", "Updated")).block();
        assertThat(secondRun).containsExactly("3");
    }

    @Test
    void overflowing_the_live_buffer_during_replay_fails_loud() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CloudEvent e1 = cloudEvent("1", "Created");
        List<Throwable> ackErrors = new ArrayList<>();
        boolean[] pushed = {false};
        // On the first replayed element, three live events arrive but the buffer cap is two.
        PositionOrderedReader reader = reader(() -> Flux.just(e1).doOnNext(ce -> {
            if (!pushed[0]) {
                pushed[0] = true;
                for (String id : List.of("l1", "l2", "l3")) {
                    feed.accept(cloudEvent(id, "Updated")).subscribe(v -> {
                    }, ackErrors::add);
                }
            }
        }), 1);

        ReplayThenPushSubscriptionModel model = new ReplayThenPushSubscriptionModel(reader, feed, null, 10, 2);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());

        // The event that overflowed the buffer reports the failure to its caller (the listener), which can nack it.
        assertThat(ackErrors).hasSize(1);
        assertThat(ackErrors.get(0)).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed");
    }

    @Test
    void a_dcb_subscription_filter_cannot_be_replayed() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(Flux::empty, 0);

        ReplayThenPushSubscriptionModel model = new ReplayThenPushSubscriptionModel(reader, feed, null);
        Throwable thrown = catchThrowable(() ->
                model.subscribe("proj", DcbSubscriptionFilter.filter(DcbCriteria.all()), StartAt.subscriptionModelDefault(), ce -> Mono.empty()));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Cannot bootstrap-replay");
    }

    // --- helpers ---

    private static java.util.function.Function<CloudEvent, Mono<Void>> recordInto(List<String> delivered) {
        return ce -> Mono.fromRunnable(() -> delivered.add(ce.getId()));
    }

    private static PositionOrderedReader reader(java.util.function.Supplier<Flux<CloudEvent>> flux, long head) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.defer(flux::get);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(head);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }

    private static final class InMemoryReactiveCheckpointStorage implements CheckpointStorage {
        private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.justOrEmpty(checkpoints.get(subscriptionId));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
            return Mono.just(checkpoint);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.fromRunnable(() -> checkpoints.remove(subscriptionId));
        }
    }
}
