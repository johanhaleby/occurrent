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

import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscription from a {@link StartAt} of its own, whose persist predicate never stores an event, has no position
 * stored for a quiet read. The position of a {@link RepositionableSubscriptions#resumeSubscription(String, StartAt)}
 * the wrapped model restarts at is stored, and once it is, quiet positions are saved after it with no event
 * delivered.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelOwnStartResumeQuietPositionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint OWN_START = new StringBasedCheckpoint("own-start");
    private static final Checkpoint RESUMED_AT = new StringBasedCheckpoint("resumed-at");
    private static final Checkpoint QUIET_POSITION = new StringBasedCheckpoint("quiet-position");

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();

    @Test
    void a_quiet_position_is_saved_after_a_resume_at_a_position_the_wrapped_model_restarts_at_with_no_event_delivered() {
        // Given
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        DurableSubscriptionModel model = subscribed(wrapped);
        model.pauseSubscription(SUBSCRIPTION_ID);

        // When
        repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(RESUMED_AT));
        Checkpoint storedByTheResume = storage.read(SUBSCRIPTION_ID);
        Consumer<Checkpoint> saver = saverWhenWanted(wrapped, 2);
        if (saver != null) {
            saver.accept(QUIET_POSITION);
        }

        // Then
        assertAll(
                () -> assertThat(storedByTheResume).as("checkpoint stored by the resume").isEqualTo(RESUMED_AT),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(RESUMED_AT),
                () -> assertThat(saver).as("quiet position wanted after the resume").isNotNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position after the resume").isEqualTo(QUIET_POSITION)
        );
    }

    @Test
    void no_quiet_position_is_saved_when_the_subscription_is_not_resumed_at_a_position() {
        // Given
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        subscribed(wrapped);

        // When
        Consumer<Checkpoint> saver = saverWhenWanted(wrapped, 1);
        if (saver != null) {
            saver.accept(QUIET_POSITION);
        }

        // Then
        assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position with no resume").isNull();
    }

    @Test
    void a_resume_at_a_position_the_wrapped_model_does_not_restart_at_stores_nothing_and_no_quiet_position_is_saved() {
        // Given
        RepositionableQuietPositionReportingModel wrapped = RepositionableQuietPositionReportingModel.keepingTheDefaultRestartsAt();
        DurableSubscriptionModel model = subscribed(wrapped);
        model.pauseSubscription(SUBSCRIPTION_ID);

        // When
        repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(RESUMED_AT));
        Checkpoint storedByTheResume = storage.read(SUBSCRIPTION_ID);
        Consumer<Checkpoint> saver = saverWhenWanted(wrapped, 1);
        if (saver != null) {
            saver.accept(QUIET_POSITION);
        }

        // Then
        assertAll(
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(RESUMED_AT),
                () -> assertThat(storedByTheResume).as("checkpoint stored by the resume").isNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position").isNull()
        );
    }

    private DurableSubscriptionModel subscribed(RepositionableQuietPositionReportingModel wrapped) {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(__ -> false).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(OWN_START), cloudEvent -> {
        });
        return model;
    }

    private static @Nullable Consumer<Checkpoint> saverWhenWanted(RepositionableQuietPositionReportingModel model, int seconds) {
        try {
            return await().atMost(seconds, SECONDS).until(() -> model.beforeReading(SUBSCRIPTION_ID), Objects::nonNull);
        } catch (ConditionTimeoutException e) {
            return null;
        }
    }

    private static RepositionableSubscriptions repositionableIn(DurableSubscriptionModel model) {
        return RepositionableSubscriptions.findIn(model).orElseThrow();
    }
}
