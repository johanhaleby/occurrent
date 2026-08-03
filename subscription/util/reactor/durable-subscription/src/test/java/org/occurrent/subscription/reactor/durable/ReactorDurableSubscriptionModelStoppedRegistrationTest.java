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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins where a subscription starts from when it was registered while the model was stopped. Registering reads the
 * current position and holds it, starting writes it if nothing is stored, so waiting withholds events rather than
 * losing them. Uses hand-rolled fakes rather than MongoDB, because what matters is exactly when the position is read
 * and written, which a real database makes harder to see rather than easier.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelStoppedRegistrationTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void registering_while_stopped_stores_nothing() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(storage.checkpoints).isEmpty();
        assertThat(delegate.startedAt).isEmpty();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void starting_it_later_begins_where_it_was_registered_rather_than_where_the_feed_has_reached() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        delegate.globalCheckpoint = new StringCheckpoint("much-later");
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("at-registration");
    }

    @Test
    void a_subscription_that_already_has_a_stored_position_keeps_it() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.checkpoints.put(SUBSCRIPTION_ID, new StringCheckpoint("from-a-previous-run"));
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("from-a-previous-run");
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("from-a-previous-run");
    }

    @Test
    void a_position_that_cannot_be_read_at_registration_is_read_again_when_the_subscription_starts() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        delegate.failGlobalCheckpoint = false;
        delegate.globalCheckpoint = new StringCheckpoint("read-again-at-start");
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("read-again-at-start");
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void a_dynamic_start_position_is_evaluated_when_the_subscription_starts_not_when_it_is_registered() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        AtomicInteger evaluations = new AtomicInteger();
        StartAt dynamic = StartAt.dynamic(() -> {
            evaluations.incrementAndGet();
            return StartAt.subscriptionModelDefault();
        });

        model.subscribe(SUBSCRIPTION_ID, null, dynamic, __ -> Mono.empty());
        assertThat(evaluations).hasValue(0);

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(evaluations.get()).isPositive();
        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_dynamic_start_position_that_opts_out_still_starts_without_storing_a_position() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        StartAt optOut = StartAt.dynamic(() -> null);

        model.subscribe(SUBSCRIPTION_ID, null, optOut, __ -> Mono.empty());
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints).isEmpty();
        assertThat(delegate.startedAt).isNotEmpty();
    }

    @Test
    void registering_while_running_behaves_as_it_did_before() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
    }

    private static String startedAtCheckpoint(RecordingSubscriptionModel delegate) {
        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst()).isInstanceOf(StartAt.StartAtCheckpoint.class);
        return ((StartAt.StartAtCheckpoint) delegate.startedAt.getFirst()).checkpoint.asString();
    }

    private record StringCheckpoint(String value) implements Checkpoint {
        @Override
        public String asString() {
            return value;
        }
    }

    // Records what start position it is asked to read from, and hands back no events, since these tests are about the
    // position rather than delivery.
    private static final class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel {
        final List<StartAt> startedAt = new CopyOnWriteArrayList<>();
        Checkpoint globalCheckpoint;
        boolean failGlobalCheckpoint = false;

        private RecordingSubscriptionModel(String initialGlobalCheckpoint) {
            this.globalCheckpoint = new StringCheckpoint(initialGlobalCheckpoint);
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            startedAt.add(startAt);
            return Flux.never();
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return failGlobalCheckpoint
                    ? Mono.error(new IllegalStateException("Cannot read the position right now"))
                    : Mono.just(globalCheckpoint);
        }
    }

    private static final class InMemoryCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

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
