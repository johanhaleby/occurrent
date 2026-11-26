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

package org.occurrent.springboot.blocking;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.springboot.blocking.StartPositionSupport.StartPositionToUse.StartAtStartPosition;
import org.occurrent.springboot.common.SubscriptionResumePoints;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;

import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A live start that pins the start boundary as the first checkpoint starts at the checkpoint the pin settled, for the
 * stream, capability-agnostic and DCB starts at NOW and DEFAULT, except for a competing consumer model. That one, and
 * every model that asks after it, gets the model default, which resumes from the checkpoint stored by the time the node
 * holds the lease and stores this node's write version with it. When the pin kept a stored checkpoint it can't order
 * against the boundary, the start is the one it was before the boundary was pinned.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StartPositionSupportSettledCheckpointTest {

    private static final String ID = "settled";
    private static final String LIVE_START = "7";
    private static final Checkpoint BOUNDARY = GlobalCheckpoint.of(7);
    private static final Checkpoint EARLIER = GlobalCheckpoint.of(3);
    private static final Checkpoint LATER = GlobalCheckpoint.of(9);
    private static final SubscriptionModelContext ANY_MODEL = new SubscriptionModelContext(Object.class);
    private static final SubscriptionModelContext COMPETING = new SubscriptionModelContext(CompetingConsumerSubscriptionModel.class);
    private static final SubscriptionModelContext CATCHUP = new SubscriptionModelContext(CatchupSubscriptionModel.class);
    private static final SubscriptionModelContext DURABLE = new SubscriptionModelContext(DurableSubscriptionModel.class);

    private enum Api {STREAM, AGNOSTIC, DCB}

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_at_the_boundary_when_the_pin_wrote_it(Api api, StartPosition startPosition) {
        // Given
        StubStorage storage = new StubStorage();

        // When
        StartAt start = startOf(api, startPosition, storage);

        // Then
        assertThat(checkpointOf(start.get(ANY_MODEL))).isEqualTo(BOUNDARY.asString());
        assertThat(storage.stored).isEqualTo(BOUNDARY);
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_at_the_boundary_when_another_node_pinned_a_later_one_first_and_the_storage_orders_the_two(Api api, StartPosition startPosition) {
        // Given
        StubStorage storage = new StubStorage();
        storage.storedByAnotherNodeAfterTheRead = LATER;
        storage.resolved = BOUNDARY;

        // When
        StartAt start = startOf(api, startPosition, storage);

        // Then
        assertThat(checkpointOf(start.get(ANY_MODEL))).isEqualTo(BOUNDARY.asString());
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
        assertThat(checkpointOf(start.get(ANY_MODEL))).isEqualTo(EARLIER.asString());
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void starts_as_before_when_a_checkpoint_was_stored_already_and_the_storage_cannot_order_it_against_the_boundary(Api api, StartPosition startPosition) {
        // Given
        StubStorage storage = new StubStorage();
        storage.stored = LATER;

        // When
        StartAt start = startOf(api, startPosition, storage);

        // Then
        assertThat(startPosition == StartPosition.NOW ? start.isNow() : start.isDefault()).as("start is the one it was before the boundary was pinned").isTrue();
        assertThat(storage.stored).as("the stored checkpoint is kept").isEqualTo(LATER);
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void a_competing_consumer_model_starts_at_the_model_default_and_so_does_every_model_that_asks_after_it(Api api, StartPosition startPosition) {
        // Given
        StartAt start = startOf(api, startPosition, new StubStorage());

        // When
        StartAt competing = start.get(COMPETING);
        StartAt catchup = start.get(CATCHUP);
        StartAt durable = start.get(DURABLE);

        // Then
        assertThat(competing).as("the competing consumer model").matches(StartAt::isDefault);
        assertThat(catchup).as("the catch-up model asking after it").matches(StartAt::isDefault);
        assertThat(durable).as("the durable model asking after it").matches(StartAt::isDefault);
    }

    @ParameterizedTest
    @CsvSource({"STREAM,NOW", "STREAM,DEFAULT", "AGNOSTIC,NOW", "AGNOSTIC,DEFAULT", "DCB,NOW", "DCB,DEFAULT"})
    void a_catchup_and_a_durable_model_without_a_competing_consumer_model_start_at_the_settled_checkpoint(Api api, StartPosition startPosition) {
        // Given
        StartAt start = startOf(api, startPosition, new StubStorage());

        // When
        StartAt catchup = start.get(CATCHUP);
        StartAt durable = start.get(DURABLE);

        // Then
        assertThat(checkpointOf(catchup)).isEqualTo(BOUNDARY.asString());
        assertThat(checkpointOf(durable)).isEqualTo(BOUNDARY.asString());
    }

    @ParameterizedTest
    @EnumSource(Api.class)
    void the_competing_consumer_answer_is_remembered_by_each_start_separately(Api api) {
        // Given
        StartAt competes = startOf(api, StartPosition.DEFAULT, new StubStorage());
        StartAt plain = startOf(api, StartPosition.DEFAULT, new StubStorage());
        competes.get(COMPETING);

        // When
        StartAt answeredToTheOtherStart = plain.get(DURABLE);

        // Then
        assertThat(checkpointOf(answeredToTheOtherStart)).isEqualTo(BOUNDARY.asString());
    }

    private static StartAt startOf(Api api, StartPosition startPosition, StubStorage storage) {
        StartPositionSupport support = support(storage);
        support.startAtLateBoundary(ID, LIVE_START);
        return switch (api) {
            case STREAM -> support.generateStartAt(ID, new StartAtStartPosition(switch (startPosition) {
                case NOW -> org.occurrent.annotation.StreamSubscription.StartPosition.NOW;
                default -> org.occurrent.annotation.StreamSubscription.StartPosition.DEFAULT;
            }), ResumeBehavior.DEFAULT);
            case AGNOSTIC -> support.generateAgnosticStartAt(ID, startPosition, -1, ResumeBehavior.DEFAULT);
            case DCB -> support.generateDcbStartAt(ID, startPosition, -1, ResumeBehavior.DEFAULT).toStartAt();
        };
    }

    private static String checkpointOf(@Nullable StartAt resolved) {
        assertThat(resolved).as("start resolved").isInstanceOf(StartAtCheckpoint.class);
        return ((StartAtCheckpoint) resolved).checkpoint.asString();
    }

    @SuppressWarnings("unchecked")
    private static StartPositionSupport support(CheckpointStorage storage) {
        ApplicationContext applicationContext = ContextWithoutBoundaryStore.mocked();
        ObjectProvider<CheckpointStorage> storages = mock(ObjectProvider.class);
        when(storages.getIfUnique()).thenReturn(storage);
        when(applicationContext.getBeanProvider(CheckpointStorage.class)).thenReturn(storages);
        ObjectProvider<LiveStartCheckpoints> liveStartCheckpoints = mock(ObjectProvider.class);
        LiveStartCheckpoints checkpoints = liveStart -> GlobalCheckpoint.of(Long.parseLong(liveStart));
        when(liveStartCheckpoints.getIfUnique()).thenReturn(checkpoints);
        when(applicationContext.getBeanProvider(LiveStartCheckpoints.class)).thenReturn(liveStartCheckpoints);
        ObjectProvider<SubscriptionResumePoints> resumePoints = mock(ObjectProvider.class);
        when(applicationContext.getBeanProvider(SubscriptionResumePoints.class)).thenReturn(resumePoints);
        return new StartPositionSupport(applicationContext);
    }

    /**
     * Evaluates {@code ifAbsent()}. {@code storedByAnotherNodeAfterTheRead} is stored by the existence check, after it
     * answered, so the checkpoint of another node can arrive between the check and the write.
     */
    private static final class StubStorage implements CheckpointStorage {
        @Nullable Checkpoint stored;
        @Nullable Checkpoint storedByAnotherNodeAfterTheRead;
        @Nullable Checkpoint resolved;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return stored;
        }

        @Override
        public boolean exists(String subscriptionId) {
            boolean existed = stored != null;
            if (storedByAnotherNodeAfterTheRead != null) {
                stored = storedByAnotherNodeAfterTheRead;
            }
            return existed;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (stored != null) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
            }
            stored = checkpoint;
            return checkpoint;
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return Optional.ofNullable(resolved);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            stored = null;
        }
    }
}
