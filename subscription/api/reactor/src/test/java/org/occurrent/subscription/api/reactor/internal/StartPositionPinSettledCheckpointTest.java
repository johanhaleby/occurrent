/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.api.reactor.internal;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The checkpoint {@link StartPositionPin#pin} answers is the one the subscription goes on from, for every way the first
 * position can be settled.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StartPositionPinSettledCheckpointTest {

    private static final String ID = "sub";
    private static final Checkpoint POSITION = GlobalCheckpoint.of(10);
    private static final Checkpoint EARLIER = GlobalCheckpoint.of(5);
    private static final Checkpoint LATER = GlobalCheckpoint.of(20);

    @Test
    void answers_the_position_when_the_conditional_write_succeeds() {
        FakeStorage storage = new FakeStorage();

        StepVerifier.create(StartPositionPin.pin(storage, ID, false, POSITION)).expectNext(POSITION).verifyComplete();

        assertThat(storage.stored).isEqualTo(POSITION);
        assertThat(storage.lastCondition).isEqualTo(CheckpointWriteCondition.ifAbsent());
    }

    @Test
    void answers_what_the_storage_resolves_when_the_conditional_write_loses_and_the_storage_orders_the_two() {
        FakeStorage storage = new FakeStorage();
        storage.stored = LATER;
        storage.resolved = EARLIER;

        StepVerifier.create(StartPositionPin.pin(storage, ID, false, POSITION)).expectNext(EARLIER).verifyComplete();
    }

    @Test
    void answers_what_the_storage_resolves_also_when_a_checkpoint_existed_when_the_position_was_read() {
        FakeStorage storage = new FakeStorage();
        storage.stored = LATER;
        storage.resolved = POSITION;

        StepVerifier.create(StartPositionPin.pin(storage, ID, true, POSITION)).expectNext(POSITION).verifyComplete();
    }

    @Test
    void answers_the_stored_checkpoint_when_the_storage_cannot_order_the_two_and_the_stored_one_is_the_position_read() {
        FakeStorage storage = new FakeStorage();
        Checkpoint samePositionStoredByAnotherNode = GlobalCheckpoint.of(10);
        storage.stored = samePositionStoredByAnotherNode;

        StepVerifier.create(StartPositionPin.pin(storage, ID, false, POSITION)).expectNext(samePositionStoredByAnotherNode).verifyComplete();
    }

    @Test
    void refuses_when_the_storage_cannot_order_the_two_and_another_position_arrived_after_the_read() {
        FakeStorage storage = new FakeStorage();
        storage.stored = LATER;

        StepVerifier.create(StartPositionPin.pin(storage, ID, false, POSITION)).expectError(StartPositionAlreadyPinnedException.class).verify();
    }

    @Test
    void answers_nothing_when_a_checkpoint_existed_before_the_read_and_the_storage_cannot_order_the_two() {
        FakeStorage storage = new FakeStorage();
        storage.stored = LATER;

        StepVerifier.create(StartPositionPin.pin(storage, ID, true, POSITION)).verifyComplete();

        assertThat(storage.stored).as("the stored checkpoint is kept").isEqualTo(LATER);
    }

    @Test
    void answers_the_position_after_an_unconditional_write_when_the_storage_does_not_evaluate_write_conditions() {
        FakeStorage storage = new FakeStorage();
        storage.evaluatesConditions = false;

        StepVerifier.create(StartPositionPin.pin(storage, ID, false, POSITION)).expectNext(POSITION).verifyComplete();

        assertThat(storage.stored).isEqualTo(POSITION);
        assertThat(storage.lastCondition).isEqualTo(CheckpointWriteCondition.any());
    }

    @Test
    void answers_nothing_and_writes_nothing_when_the_storage_does_not_evaluate_write_conditions_and_a_checkpoint_existed() {
        FakeStorage storage = new FakeStorage();
        storage.evaluatesConditions = false;
        storage.stored = LATER;

        StepVerifier.create(StartPositionPin.pin(storage, ID, true, POSITION)).verifyComplete();

        assertThat(storage.stored).isEqualTo(LATER);
        assertThat(storage.lastCondition).as("condition of the last save").isNull();
    }

    private static final class FakeStorage implements CheckpointStorage {
        @Nullable Checkpoint stored;
        @Nullable Checkpoint resolved;
        @Nullable CheckpointWriteCondition lastCondition;
        boolean evaluatesConditions = true;

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> Mono.justOrEmpty(stored));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                lastCondition = condition;
                if (condition.equals(CheckpointWriteCondition.ifAbsent()) && stored != null) {
                    return Mono.error(new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition));
                }
                stored = checkpoint;
                return Mono.just(checkpoint);
            });
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return Mono.defer(() -> Mono.justOrEmpty(resolved));
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return evaluatesConditions;
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.fromRunnable(() -> stored = null);
        }
    }
}
