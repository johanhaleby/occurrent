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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.awaitility.core.ConditionTimeoutException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * A plain {@link DurableSubscriptionModel#resumeSubscription(String)} of a paused subscription whose wrapped model
 * restarts at some checkpoints and, for others, goes on from its own position, as the MongoDB models do for a
 * {@link GlobalCheckpoint}. The resume counts as a reposition only when the wrapped model restarts at the stored
 * checkpoint, since only then are the deliveries read before it delivered again.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelPlainResumeOwnPositionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint STORED_GLOBAL_CHECKPOINT = new GlobalCheckpoint(5);
    private static final Checkpoint STORED_BY_ANOTHER_NODE = new StringBasedCheckpoint("stored-by-another-node");
    private static final Checkpoint EVENT_BEFORE_THE_PAUSE = new StringBasedCheckpoint("event-before-the-pause");
    private static final Checkpoint EVENT_AFTER_THE_RESUME = new StringBasedCheckpoint("event-after-the-resume");
    private static final Checkpoint QUIET_BEFORE_THE_RESUME = new StringBasedCheckpoint("quiet-before-the-resume");
    private static final Checkpoint QUIET_AFTER_THE_RESUME = new StringBasedCheckpoint("quiet-after-the-resume");

    private final RepositionableQuietPositionReportingModel wrapped = RepositionableQuietPositionReportingModel.continuingFromItsOwnPositionForAGlobalCheckpoint();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();

    @Test
    void a_quiet_position_is_not_stored_after_a_plain_resume_that_goes_on_from_the_wrapped_models_own_position_until_a_later_delivery_stores_its_checkpoint() {
        // Given
        AtomicBoolean persistEvents = new AtomicBoolean();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig((CloudEvent cloudEvent) -> persistEvents.get()).saveQuietPositionEvery(Duration.ofMillis(1)));
        storage.save(SUBSCRIPTION_ID, STORED_GLOBAL_CHECKPOINT);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> {
        });
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_BEFORE_THE_PAUSE);
        model.pauseSubscription(SUBSCRIPTION_ID);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        Consumer<Checkpoint> saverAfterTheResume = saverWhenWanted();
        if (saverAfterTheResume != null) {
            saverAfterTheResume.accept(QUIET_BEFORE_THE_RESUME);
        }
        Checkpoint storedWhileTheEventIsUnsaved = storage.read(SUBSCRIPTION_ID);
        persistEvents.set(true);
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_AFTER_THE_RESUME);
        Consumer<Checkpoint> saverAfterTheLaterDelivery = saverWhenWanted();
        if (saverAfterTheLaterDelivery != null) {
            saverAfterTheLaterDelivery.accept(QUIET_AFTER_THE_RESUME);
        }

        // Then
        assertAll(
                () -> assertThat(storedWhileTheEventIsUnsaved).as("checkpoint stored while the event of the first run is not saved").isEqualTo(STORED_GLOBAL_CHECKPOINT),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_GLOBAL_CHECKPOINT),
                () -> assertThat(saverAfterTheLaterDelivery).as("quiet position wanted after a delivery stored its checkpoint").isNotNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a later delivery stored its own").isEqualTo(QUIET_AFTER_THE_RESUME)
        );
    }

    @Test
    void a_plain_resume_that_goes_to_a_checkpoint_the_wrapped_model_restarts_at_still_counts_as_a_reposition() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(new StringBasedCheckpoint("start")), cloudEvent -> {
        });
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_BEFORE_THE_PAUSE);
        Consumer<Checkpoint> saverHandedOutBeforeThePause = saverWhenWanted();
        assertThat(saverHandedOutBeforeThePause).as("quiet position wanted before the pause").isNotNull();
        model.pauseSubscription(SUBSCRIPTION_ID);
        storage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        saverHandedOutBeforeThePause.accept(QUIET_BEFORE_THE_RESUME);
        Checkpoint storedAfterTheSaverOfTheEarlierRun = storage.read(SUBSCRIPTION_ID);
        Consumer<Checkpoint> saverHandedOutAfterTheResume = saverWhenWanted();
        assertThat(saverHandedOutAfterTheResume).as("quiet position wanted after the resume").isNotNull();
        saverHandedOutAfterTheResume.accept(QUIET_AFTER_THE_RESUME);

        // Then
        assertAll(
                () -> assertThat(storedAfterTheSaverOfTheEarlierRun).as("checkpoint stored after a quiet position handed out before the pause").isEqualTo(STORED_BY_ANOTHER_NODE),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BY_ANOTHER_NODE),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position handed out after the resume").isEqualTo(QUIET_AFTER_THE_RESUME)
        );
    }

    // Reads until a listener wants the quiet position, which the 1 ms interval allows after a moment. Null when none does
    private @Nullable Consumer<Checkpoint> saverWhenWanted() {
        try {
            return await().atMost(2, SECONDS).until(() -> wrapped.beforeReading(SUBSCRIPTION_ID), Objects::nonNull);
        } catch (ConditionTimeoutException e) {
            return null;
        }
    }
}
