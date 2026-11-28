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

package org.occurrent.springboot.reactor;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.springboot.common.SubscriptionResumePoints;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.springboot.common.internal.SubscriptionAnnotations.StreamSubscriptionDefinition;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Mono;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A live start that pins the start boundary as the first checkpoint starts at the checkpoint the pin settled, for the
 * stream, capability-agnostic and DCB starts at NOW and DEFAULT. Reading storage again when the subscription model
 * subscribes could find the checkpoint of another node that delivers from a later position. When the pin kept a stored
 * checkpoint it can't order against the boundary, the start is the model default, which resumes from it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StartPositionSupportSettledCheckpointTest {

    private static final String ID = "settled";
    private static final String LIVE_START = "7";
    private static final Checkpoint BOUNDARY = GlobalCheckpoint.of(7);
    private static final Checkpoint EARLIER = GlobalCheckpoint.of(3);
    private static final Checkpoint LATER = GlobalCheckpoint.of(9);
    private static final SubscriptionModelContext ANY_MODEL = new SubscriptionModelContext(Object.class);

    private enum Api {STREAM, AGNOSTIC, DCB}

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_at_the_boundary_when_the_pin_wrote_it(Api api, StartPosition startPosition) {
        // Given
        StubStorage storage = new StubStorage();

        // When
        StartAt start = startOf(api, startPosition, storage);

        // Then
        assertThat(checkpointOf(start)).isEqualTo(BOUNDARY.asString());
        assertThat(storage.stored).isEqualTo(BOUNDARY);
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_at_the_earlier_checkpoint_when_another_node_pinned_a_later_one_first_and_the_storage_orders_the_two(Api api, StartPosition startPosition) {
        // Given
        StubStorage storage = new StubStorage();
        storage.storedByAnotherNodeAfterTheRead = LATER;
        storage.resolved = BOUNDARY;

        // When
        StartAt start = startOf(api, startPosition, storage);

        // Then
        assertThat(checkpointOf(start)).isEqualTo(BOUNDARY.asString());
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_at_what_the_storage_resolves_when_it_is_earlier_than_the_boundary(Api api, StartPosition startPosition) {
        // Given
        StubStorage storage = new StubStorage();
        storage.storedByAnotherNodeAfterTheRead = EARLIER;
        storage.resolved = EARLIER;

        // When
        StartAt start = startOf(api, startPosition, storage);

        // Then
        assertThat(checkpointOf(start)).isEqualTo(EARLIER.asString());
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_at_the_model_default_when_a_checkpoint_was_stored_already_and_the_storage_cannot_order_it_against_the_boundary(Api api, StartPosition startPosition) {
        // Given
        StubStorage storage = new StubStorage();
        storage.stored = LATER;
        storage.readAnswer = LATER;

        // When
        StartAt start = startOf(api, startPosition, storage);

        // Then
        assertThat(start.isDefault()).as("start is the model default").isTrue();
        assertThat(storage.stored).as("the stored checkpoint is kept").isEqualTo(LATER);
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_at_the_boundary_when_a_checkpoint_was_stored_already_and_the_storage_proved_it_later_and_wrote_the_boundary_over_it(Api api, StartPosition startPosition) {
        // Given
        StubStorage storage = new StubStorage();
        storage.stored = LATER;
        storage.readAnswer = LATER;
        storage.resolved = BOUNDARY;

        // When
        StartAt start = startOf(api, startPosition, storage);

        // Then
        assertThat(checkpointOf(start)).isEqualTo(BOUNDARY.asString());
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_at_the_model_default_and_pins_nothing_when_the_live_start_checkpoints_cannot_order_the_storage(Api api, StartPosition startPosition) {
        // Given a storage the store starter does not know to order the boundary. A pin over it would keep a checkpoint
        // another node stored after the boundary was read, so the start is the 0.34.0 one and nothing is read or written
        StubStorage storage = new StubStorage();
        storage.storedByAnotherNodeAfterTheRead = LATER;

        // When
        StartAt start = startOf(api, startPosition, storage, false);

        // Then
        assertThat(start.isDefault()).as("start is the model default").isTrue();
        assertThat(storage.stored).as("nothing is stored by a pin, and storage is not read").isNull();
    }

    private static StartAt startOf(Api api, StartPosition startPosition, StubStorage storage) {
        return startOf(api, startPosition, storage, true);
    }

    private static StartAt startOf(Api api, StartPosition startPosition, StubStorage storage, boolean liveStartCheckpointsOrderThem) {
        StartPositionSupport support = support(storage, liveStartCheckpointsOrderThem);
        support.startAtLateBoundary(ID, LIVE_START);
        return switch (api) {
            case STREAM -> support.generateStreamStartAt(new StreamSubscriptionDefinition(ID, new Class<?>[0], "", -1, StreamSubscription.StartPosition.valueOf(startPosition.name()), ResumeBehavior.DEFAULT, StartupMode.BACKGROUND, "@StreamSubscription"), true);
            case AGNOSTIC -> support.generateAgnosticStartAt(ID, startPosition, -1, ResumeBehavior.DEFAULT);
            case DCB -> support.generateDcbStartAt(ID, startPosition, -1, ResumeBehavior.DEFAULT).toStartAt();
        };
    }

    private static String checkpointOf(StartAt start) {
        StartAt resolved = start.get(ANY_MODEL);
        assertThat(resolved).as("start resolved for any model").isInstanceOf(StartAtCheckpoint.class);
        return ((StartAtCheckpoint) resolved).checkpoint.asString();
    }

    @SuppressWarnings("unchecked")
    private static StartPositionSupport support(CheckpointStorage storage, boolean liveStartCheckpointsOrderThem) {
        ApplicationContext applicationContext = ContextWithoutBoundaryStore.mocked();
        ObjectProvider<CheckpointStorage> storages = mock(ObjectProvider.class);
        when(storages.getIfUnique()).thenReturn(storage);
        when(applicationContext.getBeanProvider(CheckpointStorage.class)).thenReturn(storages);
        ObjectProvider<LiveStartCheckpoints> liveStartCheckpoints = mock(ObjectProvider.class);
        LiveStartCheckpoints checkpoints = new LiveStartCheckpoints() {
            @Override
            public Checkpoint checkpointOf(String liveStart) {
                return GlobalCheckpoint.of(Long.parseLong(liveStart));
            }

            @Override
            public boolean ordersCheckpointsIn(Object checkpointStorage) {
                return liveStartCheckpointsOrderThem;
            }
        };
        when(liveStartCheckpoints.getIfUnique()).thenReturn(checkpoints);
        when(applicationContext.getBeanProvider(LiveStartCheckpoints.class)).thenReturn(liveStartCheckpoints);
        ObjectProvider<SubscriptionResumePoints> resumePoints = mock(ObjectProvider.class);
        when(applicationContext.getBeanProvider(SubscriptionResumePoints.class)).thenReturn(resumePoints);
        return new StartPositionSupport(applicationContext);
    }

    /**
     * Evaluates {@code ifAbsent()}. {@code readAnswer} is what a read answers, apart from what is stored, so the
     * checkpoint of another node can arrive after the read and before the write.
     */
    private static final class StubStorage implements CheckpointStorage {
        @Nullable Checkpoint stored;
        @Nullable Checkpoint readAnswer;
        @Nullable Checkpoint storedByAnotherNodeAfterTheRead;
        @Nullable Checkpoint resolved;

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                @Nullable Checkpoint answer = readAnswer;
                if (storedByAnotherNodeAfterTheRead != null) {
                    stored = storedByAnotherNodeAfterTheRead;
                }
                return Mono.justOrEmpty(answer);
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                if (stored != null) {
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
            return true;
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
