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

package org.occurrent.subscription.inmemory;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StringBasedCheckpoint;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * {@code storeWriteVersion}, which stores the version with the checkpoint stored and leaves the checkpoint as it is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemoryCheckpointStorageStoreWriteVersionTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();

    @Test
    void answers_empty_and_writes_nothing_when_nothing_is_stored() {
        Optional<Checkpoint> answer = storage.storeWriteVersion(SUBSCRIPTION_ID, 2);

        assertAll(
                () -> assertThat(answer).as("answer").isEmpty(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored position").isNull(),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty()
        );
    }

    @Test
    void stores_the_version_and_answers_the_checkpoint_when_a_checkpoint_with_no_version_is_stored() {
        storage.save(SUBSCRIPTION_ID, checkpoint("stored"));

        Optional<Checkpoint> answer = storage.storeWriteVersion(SUBSCRIPTION_ID, 2);

        assertAll(
                () -> assertThat(answer).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void stores_the_version_and_answers_the_checkpoint_when_a_checkpoint_with_an_older_version_is_stored() {
        storage.save(SUBSCRIPTION_ID, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> answer = storage.storeWriteVersion(SUBSCRIPTION_ID, 2);

        assertAll(
                () -> assertThat(answer).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void answers_the_checkpoint_and_keeps_the_version_when_a_checkpoint_with_the_same_version_is_stored() {
        storage.save(SUBSCRIPTION_ID, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(2));

        Optional<Checkpoint> answer = storage.storeWriteVersion(SUBSCRIPTION_ID, 2);

        assertAll(
                () -> assertThat(answer).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void refuses_a_version_older_than_the_stored_one_with_that_version_and_writes_nothing() {
        storage.save(SUBSCRIPTION_ID, checkpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));

        assertAll(
                () -> assertThatThrownBy(() -> storage.storeWriteVersion(SUBSCRIPTION_ID, 2))
                        .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class, e -> assertAll(
                                () -> assertThat(e.storedVersion).as("stored version in the exception").hasValue(3L),
                                () -> assertThat(e.condition).as("condition in the exception").isEqualTo(CheckpointWriteCondition.notOlderThan(2)))),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("newer-nodes-position"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void a_negative_version_is_refused_and_writes_nothing() {
        storage.save(SUBSCRIPTION_ID, checkpoint("stored"));

        assertAll(
                () -> assertThatThrownBy(() -> storage.storeWriteVersion(SUBSCRIPTION_ID, -1)).isInstanceOf(IllegalArgumentException.class),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty()
        );
    }

    private static Checkpoint checkpoint(String value) {
        return new StringBasedCheckpoint(value);
    }
}
