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

package org.occurrent.tck.subscription.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.ConformanceEvents;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * The contract a model owes when it can report where the event feed currently is.
 * <p>
 * This is the contract #395 calls easy to miss and expensive to get wrong. A catch-up subscription replays history and
 * then hands over to live delivery, and the checkpoint read before the replay is what the handover starts from. A model
 * whose checkpoint cannot be started from loses every event written during the replay, and nothing would say so.
 * <p>
 * A model that cannot report a position declines this suite by not extending it. There is no declaration, because
 * {@link CheckpointAwareSubscriptionModel} is itself the declaration, and the position is asked rather than declared for
 * the same reason {@code PositionOrderedReader.writesPosition()} does on the event-store side. A declaration can go
 * stale while a runtime answer cannot.
 * <p>
 * <strong>What this suite deliberately does not assert.</strong> {@code globalCheckpoint()} documents null as
 * "an unresolvable problem", and the honest consequence is that such a model cannot sit behind catch-up at all.
 * Asserting that refusal needs a catch-up model to drive, which lives in a wrapper module the TCK does not and should
 * not depend on, so it does not belong here. It landed instead as
 * {@code StreamCatchupSubscriptionModelTest.a_model_that_reports_no_checkpoint_cannot_sit_behind_catchup}, in the
 * module that owns the catch-up model. See the amendment to ADR 94.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the checkpoint-aware subscription model contract")
@Timeout(60)
public abstract class CheckpointAwareSubscriptionModelConformance extends SubscriptionModelSuite {

    /**
     * Creates a fixture whose model has no subscriptions and implements {@link CheckpointAwareSubscriptionModel}.
     * Called before every test method.
     */
    @Override
    protected abstract SubscriptionModelFixture createFixture();

    @Override
    protected void checkFixtureCanAnswerThisSuite(SubscriptionModelFixture fixture) {
        SubscriptionModel model = fixture.subscriptionModel();
        if (!(model instanceof CheckpointAwareSubscriptionModel)) {
            throw new IllegalStateException(model.getClass().getName() + " does not implement "
                    + CheckpointAwareSubscriptionModel.class.getSimpleName() + ", so it cannot answer this suite. A "
                    + "model that cannot report where the feed is declines by not extending it.");
        }
    }

    private CheckpointAwareSubscriptionModel checkpointAware() {
        return (CheckpointAwareSubscriptionModel) fixture().subscriptionModel();
    }

    private static String subscriptionId() {
        return UUID.randomUUID().toString();
    }

    @Test
    void answers_where_the_feed_is_rather_than_throwing() {
        // Null is a documented answer, so what is asserted here is that asking is safe on a model with no
        // subscriptions, which is exactly the state catch-up asks in.
        @Nullable Checkpoint checkpoint = checkpointAware().globalCheckpoint();

        if (checkpoint != null) {
            assertThat(checkpoint.asString())
                    .as("a checkpoint has to survive being stored and read back, and its string form is all a "
                            + "CheckpointStorage keeps")
                    .isNotBlank();
        }
    }

    @Test
    void answering_twice_does_not_consume_the_position() {
        @Nullable Checkpoint first = checkpointAware().globalCheckpoint();
        @Nullable Checkpoint second = checkpointAware().globalCheckpoint();

        assertThat(first == null)
                .as("a model that can report a position must keep being able to, since catch-up reads it once per "
                        + "subscription rather than once per model")
                .isEqualTo(second == null);
    }

    @Test
    void a_subscription_started_from_the_reported_position_receives_what_is_written_after_it() {
        @Nullable Checkpoint before = checkpointAware().globalCheckpoint();
        if (before == null) {
            // Documented as an unresolvable problem. The model cannot seed a catch-up handover, and the suite says so
            // by asserting the model is still usable live rather than by pretending the position exists.
            assertDeliversFromTheDefaultPosition();
            return;
        }

        CloudEvent written = ConformanceEvents.event(UUID.randomUUID().toString(), "NameDefined");
        RecordedEvents recorded = new RecordedEvents();
        fixture().subscriptionModel()
                .subscribe(subscriptionId(), null, StartAt.checkpoint(before), recorded)
                .waitUntilStarted(deliveryTimeout());

        fixture().publish(List.of(written));

        assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                .as("the position was read before the write, so a subscription starting from it owes that event. This "
                        + "is the handover a catch-up subscription performs, and losing the event here means losing "
                        + "every event written while history replayed")
                .contains(written.getId());
    }

    private void assertDeliversFromTheDefaultPosition() {
        CloudEvent written = ConformanceEvents.event(UUID.randomUUID().toString(), "NameDefined");
        RecordedEvents recorded = new RecordedEvents();
        fixture().subscriptionModel()
                .subscribe(subscriptionId(), recorded)
                .waitUntilStarted(deliveryTimeout());

        fixture().publish(List.of(written));

        assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                .as("a model that cannot report a position is still a working live model, it just cannot seed a "
                        + "catch-up handover")
                .contains(written.getId());
    }
}
