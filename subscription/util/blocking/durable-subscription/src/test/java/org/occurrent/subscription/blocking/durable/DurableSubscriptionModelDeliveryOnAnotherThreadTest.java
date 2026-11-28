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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * A wrapped model that reads on one thread and delivers the events it read on another, such as a model of the user's own
 * that hands its events to a pool. A delivery on a thread that made no numbered read belongs to the latest read, so a quiet
 * position saved since the previous delivery doesn't make the checkpoint of the next delivery look like it came from a read
 * that is already past, which would drop it with no sign.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DurableSubscriptionModelDeliveryOnAnotherThreadTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint FIRST_EVENT = new StringBasedCheckpoint("first-event");
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");
    private static final Checkpoint SECOND_EVENT = new StringBasedCheckpoint("second-event");
    private static final Checkpoint QUIET_AFTER_THE_SECOND_EVENT = new StringBasedCheckpoint("quiet-after-the-second-event");

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final ExecutorService deliveryThread = Executors.newSingleThreadExecutor();

    @AfterEach
    void stopTheDeliveryThread() {
        deliveryThread.shutdownNow();
    }

    @Test
    void a_delivery_on_a_thread_that_made_no_read_stores_its_checkpoint_after_a_quiet_position_was_saved() throws Exception {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        deliverOnTheDeliveryThread(wrapped, FIRST_EVENT);
        boolean quietPositionSaved = saveAQuietPositionWhenWanted(wrapped, QUIET);
        Checkpoint storedAfterTheQuietPosition = storage.read(SUBSCRIPTION_ID);

        // When
        deliverOnTheDeliveryThread(wrapped, SECOND_EVENT);

        // Then
        assertAll(
                () -> assertThat(quietPositionSaved).as("quiet position wanted after the first event").isTrue(),
                () -> assertThat(storedAfterTheQuietPosition).as("checkpoint stored after the quiet position").isEqualTo(QUIET),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a delivery on a thread that made no read").isEqualTo(SECOND_EVENT)
        );
    }

    /**
     * The wrapped model restarts at the checkpoint the plain resume goes to, so the resume stores nothing and notes the
     * checkpoint it found stored. A delivery on the delivery thread after the resume still stores its checkpoint, and a
     * quiet position is saved once what is stored is that checkpoint.
     */
    @Test
    void a_delivery_on_a_thread_that_made_no_read_stores_its_checkpoint_after_a_plain_resume_to_a_position_the_wrapped_model_restarts_at_and_a_quiet_position_is_saved_after_it() throws Exception {
        // Given
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        deliverOnTheDeliveryThread(wrapped, FIRST_EVENT);
        saveAQuietPositionWhenWanted(wrapped, QUIET);
        model.pauseSubscription(SUBSCRIPTION_ID);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        deliverOnTheDeliveryThread(wrapped, SECOND_EVENT);
        Checkpoint storedAfterTheSecondEvent = storage.read(SUBSCRIPTION_ID);
        boolean quietPositionSavedAfterTheSecondEvent = saveAQuietPositionWhenWanted(wrapped, QUIET_AFTER_THE_SECOND_EVENT);

        // Then
        assertAll(
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(QUIET),
                () -> assertThat(storedAfterTheSecondEvent).as("checkpoint stored after a delivery on a thread that made no read, after the resume").isEqualTo(SECOND_EVENT),
                () -> assertThat(quietPositionSavedAfterTheSecondEvent).as("quiet position wanted once the checkpoint of the delivery is stored").isTrue(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position once the checkpoint of the delivery is stored").isEqualTo(QUIET_AFTER_THE_SECOND_EVENT)
        );
    }

    private void deliverOnTheDeliveryThread(QuietPositionReportingModel wrapped, Checkpoint position) throws Exception {
        deliveryThread.submit(() -> wrapped.deliver(SUBSCRIPTION_ID, position)).get(10, SECONDS);
    }

    // Reads on threads of the test, never on the delivery thread, until a listener wants the quiet position, which the
    // 1 ms interval allows after a moment. False when none does within 2 seconds
    private static boolean saveAQuietPositionWhenWanted(QuietPositionReportingModel wrapped, Checkpoint quietPosition) {
        AtomicReference<Consumer<Checkpoint>> saver = new AtomicReference<>();
        try {
            await().atMost(2, SECONDS).until(() -> {
                saver.set(wrapped.beforeReading(SUBSCRIPTION_ID));
                return saver.get() != null;
            });
        } catch (ConditionTimeoutException e) {
            return false;
        }
        saver.get().accept(quietPosition);
        return true;
    }
}
