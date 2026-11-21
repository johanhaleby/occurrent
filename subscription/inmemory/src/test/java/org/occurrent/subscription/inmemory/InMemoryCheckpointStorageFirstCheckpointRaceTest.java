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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The three-argument {@code resolveFirstCheckpointRace}, which settles the first position and stores the write version
 * in one step under the lock the conditional writes take.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemoryCheckpointStorageFirstCheckpointRaceTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();

    @Test
    void stores_the_candidate_and_the_version_when_nothing_is_stored() {
        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("candidate")),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("candidate"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void keeps_and_answers_a_stored_checkpoint_with_an_older_version_and_stores_the_new_version() {
        storage.save(SUBSCRIPTION_ID, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(1));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void keeps_and_answers_a_stored_checkpoint_with_no_version_and_stores_the_version() {
        storage.save(SUBSCRIPTION_ID, checkpoint("stored"));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("stored"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void accepts_a_version_equal_to_the_stored_one() {
        storage.save(SUBSCRIPTION_ID, checkpoint("stored"), CheckpointWriteCondition.notOlderThan(2));

        Optional<Checkpoint> resolution = storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, checkpoint("candidate"), 2);

        assertAll(
                () -> assertThat(resolution).as("answer").hasValueSatisfying(c -> assertThat(c.asString()).isEqualTo("stored")),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void refuses_a_version_older_than_the_stored_one_and_writes_nothing() {
        storage.save(SUBSCRIPTION_ID, checkpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));

        assertAll(
                () -> assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, checkpoint("candidate"), 2))
                        .isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class, e -> assertThat(e.storedVersion).as("stored version in the exception").hasValue(3L)),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("newer-nodes-position"),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void concurrent_callers_with_the_same_version_are_all_answered_the_one_checkpoint_that_is_stored() throws Exception {
        int callers = 8;
        List<String> candidates = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            candidates.add("candidate-" + i);
        }
        CyclicBarrier startTogether = new CyclicBarrier(callers);
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            List<Future<Optional<Checkpoint>>> answers = new ArrayList<>();
            for (String candidate : candidates) {
                answers.add(executor.submit(() -> {
                    startTogether.await(10, TimeUnit.SECONDS);
                    return storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, checkpoint(candidate), 2);
                }));
            }
            List<Optional<String>> answered = new ArrayList<>();
            for (Future<Optional<Checkpoint>> answer : answers) {
                answered.add(answer.get(10, TimeUnit.SECONDS).map(Checkpoint::asString));
            }

            assertAll(
                    () -> assertThat(candidates).as("the stored position is one of the candidates").contains(storage.read(SUBSCRIPTION_ID).asString()),
                    () -> assertThat(answered).as("answers").hasSize(callers).containsOnly(Optional.of(storage.read(SUBSCRIPTION_ID).asString())),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
            );
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void a_negative_version_is_refused() {
        assertThatThrownBy(() -> storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, checkpoint("candidate"), -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(storage.read(SUBSCRIPTION_ID)).as("stored position").isNull();
    }

    private static Checkpoint checkpoint(String value) {
        return new StringBasedCheckpoint(value);
    }
}
