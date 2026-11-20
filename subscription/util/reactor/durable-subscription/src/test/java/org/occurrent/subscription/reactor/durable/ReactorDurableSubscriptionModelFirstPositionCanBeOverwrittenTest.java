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

import ch.qos.logback.classic.Level;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A first position is recorded for a subscription started from the model default with nothing stored. A storage that
 * answers {@code false} from {@code evaluatesWriteConditionsFor(id)} can only write it unconditionally, so two nodes
 * recording it at the same moment can overwrite each other's and lose the events between the two positions. The model
 * refuses such a registration on {@code waitUntilStarted()} before anything is written, unless
 * {@code startWhenFirstPositionCanBeOverwritten} is set. A registration that records no first position is never
 * refused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelFirstPositionCanBeOverwrittenTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final String OTHER_SUBSCRIPTION_ID = "otherSubscription";
    private static final String WRITE_ONLY_UNCONDITIONALLY = "startWhenFirstPositionCanBeOverwritten";

    @Test
    void a_default_start_is_refused_before_anything_is_written_when_the_storage_cannot_evaluate_write_conditions() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY)
                .hasMessageContaining(SUBSCRIPTION_ID);
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
        assertThat(delegate.startedAt).as("the start positions the wrapped model was asked for").isEmpty();
    }

    @Test
    void a_default_start_handed_to_a_wrapped_model_that_manages_named_subscriptions_is_refused_before_anything_is_written() {
        NamedRecordingSubscriptionModel delegate = new NamedRecordingSubscriptionModel("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY)
                .hasMessageContaining(SUBSCRIPTION_ID);
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
        assertThat(delegate.isRunning(SUBSCRIPTION_ID)).as("the wrapped model holds the subscription").isFalse();
    }

    @Test
    void a_dynamic_start_that_answers_the_model_default_is_refused_the_same_way() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::subscriptionModelDefault), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY);
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(delegate.startedAt).as("the start positions the wrapped model was asked for").isEmpty();
    }

    @Test
    void only_the_subscription_id_the_storage_cannot_evaluate_write_conditions_for_is_refused() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        Subscription refused = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        Subscription started = model.subscribe(OTHER_SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> refused.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY);
        started.waitUntilStarted().block(TIMEOUT);
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
        assertThat(storage.read(OTHER_SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("position-at-subscribe");
        assertThat(storage.saves).extracting(save -> save.subscriptionId()).containsExactly(OTHER_SUBSCRIPTION_ID);
        assertThat(storage.saves).extracting(save -> save.condition()).containsExactly(CheckpointWriteCondition.ifAbsent());
        assertThat(delegate.startedAt).hasSize(1);
    }

    @Test
    void a_start_position_of_the_callers_own_is_not_refused_since_no_first_position_is_recorded() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(new StringBasedCheckpoint("replay-from-here")), __ -> Mono.empty());

        subscription.waitUntilStarted().block(TIMEOUT);
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("replay-from-here");
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
    }

    @Test
    void a_default_start_with_a_checkpoint_already_stored_starts_from_it_whatever_the_storage_evaluates() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        storage.writeWithoutRecording(SUBSCRIPTION_ID, new StringBasedCheckpoint("from-a-previous-run"));
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        subscription.waitUntilStarted().block(TIMEOUT);
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("from-a-previous-run");
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
    }

    @Test
    void the_opt_in_writes_the_first_position_unconditionally_and_warns_as_before_the_refusal_existed() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage,
                new ReactorDurableSubscriptionModelConfig(1).startWhenFirstPositionCanBeOverwritten(true));

        try (LoggedByTheModel logged = new LoggedByTheModel()) {
            Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
            subscription.waitUntilStarted().block(TIMEOUT);

            assertThat(logged.at(Level.WARN)).hasSize(1).first().asString()
                    .contains("does not evaluate write conditions for subscription " + SUBSCRIPTION_ID)
                    .contains("written unconditionally");
        }
        assertThat(storage.saves).hasSize(1);
        assertThat(storage.saves.getFirst().subscriptionId()).isEqualTo(SUBSCRIPTION_ID);
        assertThat(storage.saves.getFirst().position()).isEqualTo("position-at-subscribe");
        assertThat(storage.saves.getFirst().condition()).isEqualTo(CheckpointWriteCondition.any());
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("position-at-subscribe");
    }

    private static String startedAtCheckpoint(RecordingSubscriptionModel delegate) {
        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst()).isInstanceOf(StartAt.StartAtCheckpoint.class);
        return ((StartAt.StartAtCheckpoint) delegate.startedAt.getFirst()).checkpoint.asString();
    }

    private record Save(String subscriptionId, String position, CheckpointWriteCondition condition) {
    }

    /**
     * Answers {@code false} from {@code evaluatesWriteConditionsFor} for the ids it is given, and {@code true} for every
     * other, as a storage that reserves a shape of id it cannot evaluate a condition for does. Records every save.
     */
    private static final class RecordingCheckpointStorage extends InMemoryCheckpointStorage {

        final List<Save> saves = new CopyOnWriteArrayList<>();
        private final Set<String> idsWithoutConditions;

        RecordingCheckpointStorage(Set<String> idsWithoutConditions) {
            this.idsWithoutConditions = idsWithoutConditions;
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            saves.add(new Save(subscriptionId, checkpoint.asString(), condition));
            return super.save(subscriptionId, checkpoint, condition);
        }

        void writeWithoutRecording(String subscriptionId, Checkpoint checkpoint) {
            super.save(subscriptionId, checkpoint, CheckpointWriteCondition.any()).block(TIMEOUT);
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return !idsWithoutConditions.contains(subscriptionId);
        }
    }
}
