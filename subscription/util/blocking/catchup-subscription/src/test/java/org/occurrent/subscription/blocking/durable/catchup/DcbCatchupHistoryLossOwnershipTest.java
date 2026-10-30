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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A live start the live model has no history back to is refused by the live branch of the DCB catch-up, before anything
 * is subscribed or stored.
 */
@DisplayName("DcbCatchupHistoryLossOwnership")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DcbCatchupHistoryLossOwnershipTest {

    private static final String ID = "subscription";
    private static final Checkpoint TOKEN = new StringBasedCheckpoint("token-1");
    private static final Checkpoint LATER_TOKEN = new StringBasedCheckpoint("later-token");

    private final CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private InMemoryCheckpointStorage storage;
    private HistoryLosingLiveModel liveModel;
    private DcbCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                saved.add(id + "=" + checkpoint.asString());
                return super.save(id, checkpoint, condition);
            }
        };
        liveModel = new HistoryLosingLiveModel();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchup = new DcbCatchupSubscriptionModel(liveModel, new InMemoryEventStore(inMemorySubscriptionModel), DcbCriteria.tags(Tag.parse("name:1")), config);
    }

    @AfterEach
    void shutdown() {
        catchup.shutdown();
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void throws_CatchupCannotResumeException_when_a_live_start_checkpoint_is_older_than_the_history_of_the_live_model() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);

        // When
        Throwable thrown = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.subscribeCalls).as("subscribes on the live model").isEmpty(),
                () -> assertThat(saved).as("positions stored").isEmpty(),
                () -> assertThat(storage.read(ID)).as("stored position").isNull(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse()
        );
    }

    @Test
    void a_live_start_checkpoint_the_live_model_has_the_history_back_to_is_subscribed_on_the_live_model() {
        // When
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());

        // Then
        assertAll(
                () -> assertThat(liveModel.subscribeCalls).as("subscribes on the live model").containsExactly(ID),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue()
        );
    }

    @Test
    void a_subscribe_of_the_id_after_a_refused_live_start_succeeds_from_a_checkpoint_the_live_model_has_the_history_back_to() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        assertThatThrownBy(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action())).isInstanceOf(CatchupCannotResumeException.class);

        // When
        catchup.subscribe(ID, StartAt.checkpoint(LATER_TOKEN), action());

        // Then
        assertThat(catchup.isRunning(ID)).as("running").isTrue();
    }

    private Consumer<CloudEvent> action() {
        return received::add;
    }
}
