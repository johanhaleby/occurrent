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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.ConformanceEvents;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.idsOf;
import static org.occurrent.tck.subscription.blocking.SubscriptionModelConformance.DELIVERY_TIMEOUT;

/**
 * What a model owes a subscription that outlives the model itself: an event published while nothing was running is
 * either still delivered afterwards, or it is gone, and which of the two is a promise the model makes rather than
 * something a caller finds out the hard way.
 * <p>
 * This is the half of the contract {@link SubscriptionModelConformance} deliberately leaves alone, because it needs
 * state that survives the model. A model whose events arrive by being handed to it has none, and no way to be handed
 * one while it is down, so it declines this suite by not extending it. That absence is visible and greppable, which is
 * the mechanism ADR 77 rule (d) already relies on, and it is the right one here: the alternative, a declaration on the
 * base fixture, would have a branch that asserts nothing at all for those models, and a declaration that is free on one
 * branch is a switch for turning off the only test of a property.
 * <p>
 * A model that <em>can</em> answer still gets to say which way it goes, through
 * {@link RestartableSubscriptionModelFixture#resumesAfterARestart()}, because two models with identical durable state
 * underneath them genuinely differ: a change-stream model reads from wherever the server is now, and the same model
 * wrapped in one that keeps a checkpoint reads from where the checkpoint says. Both branches are asserted.
 * <p>
 * <strong>Delivery here is at-least-once, not exactly-once.</strong> A model resuming from the last position it stored
 * rather than from just after it hands that event over a second time, and nothing in Occurrent promises otherwise, so
 * these assertions are about what must arrive and never about what must not repeat.
 * <p>
 * The 60 second class timeout is the same number the other suites use, and the margin here is thinner than it looks:
 * the longest test chains three {@code DELIVERY_TIMEOUT} waits at 10 seconds each, and between them it tears a model
 * down and builds another one, which against a real store means closing a change stream and opening a fresh one. That
 * leaves about 30 seconds for two rebuilds. Raise this before raising {@code DELIVERY_TIMEOUT}, since a wait that
 * outlives the class timeout reports a {@code TimeoutException} instead of naming the event that never arrived.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the restart contract")
@Timeout(60)
public abstract class RestartConformance extends SubscriptionModelSuite {

    /**
     * Creates a fixture whose model has no subscriptions, and which can rebuild that model over the state it leaves
     * behind. Called before every test method.
     */
    @Override
    protected abstract RestartableSubscriptionModelFixture createFixture();

    private RestartableSubscriptionModelFixture restartable() {
        // Safe by the covariant return above: a subclass cannot widen it back.
        return (RestartableSubscriptionModelFixture) fixture();
    }

    private static String subscriptionId() {
        return UUID.randomUUID().toString();
    }

    private void publish(CloudEvent event) {
        fixture().publish(List.of(event));
    }

    @Test
    void continues_where_it_left_off_or_starts_at_the_present_as_the_fixture_declares() {
        String id = subscriptionId();
        RecordedEvents beforeTheRestart = new RecordedEvents();
        assertThat(subscriptionModel().subscribe(id, beforeTheRestart).waitUntilStarted(DELIVERY_TIMEOUT))
                .as("the subscription must be listening before the event that establishes where it got to")
                .isTrue();
        CloudEvent delivered = ConformanceEvents.event("1", "NameDefined");
        publish(delivered);
        assertThat(idsOf(beforeTheRestart.awaitAtLeast(1, DELIVERY_TIMEOUT)))
                .as("the model has to be working before a restart says anything about it")
                .contains(delivered.getId());

        SubscriptionModel restarted = restartable().restart();

        // Published into the gap: no model is running, which is the only state that tells the two answers apart.
        CloudEvent whileNothingWasRunning = ConformanceEvents.event("2", "NameWasChanged");
        publish(whileNothingWasRunning);
        RecordedEvents afterTheRestart = new RecordedEvents();
        assertThat(restarted.subscribe(id, afterTheRestart).waitUntilStarted(DELIVERY_TIMEOUT))
                .as("the rebuilt subscription must report started, or whichever branch runs below reports a missing "
                        + "event as a checkpoint problem when it was really a subscription that never started")
                .isTrue();

        if (restartable().resumesAfterARestart()) {
            List<CloudEvent> received = afterTheRestart.awaitUntil(
                    events -> idsOf(events).contains(whileNothingWasRunning.getId()), DELIVERY_TIMEOUT);
            assertThat(idsOf(received))
                    .as("this model declares it resumes, so the event published while nothing was running is owed to "
                            + "the subscription that was running before. Losing it here is losing every event a "
                            + "deployment or a crash happened to span, which is the whole reason to keep a checkpoint")
                    .contains(whileNothingWasRunning.getId());
        } else {
            // The marker proves the fresh subscription is alive, so the gap event's absence is a start position rather
            // than a subscription that never listened.
            CloudEvent marker = ConformanceEvents.event("3", "MarkerEvent");
            publish(marker);
            assertThat(idsOf(afterTheRestart.awaitAtLeast(1, DELIVERY_TIMEOUT)))
                    .as("this model declares it starts at the present, so the event published while nothing was "
                            + "running is gone and only the marker arrives. Delivering it anyway would mean the model "
                            + "keeps a position it does not admit to")
                    .containsExactly(marker.getId());
        }
    }

    @Test
    void a_restarted_model_delivers_to_a_subscription_it_has_never_seen() {
        SubscriptionModel restarted = restartable().restart();

        RecordedEvents recorded = new RecordedEvents();
        assertThat(restarted.subscribe(subscriptionId(), recorded).waitUntilStarted(DELIVERY_TIMEOUT))
                .as("a rebuilt model is a working model, whatever it does about positions")
                .isTrue();
        CloudEvent afterTheRestart = ConformanceEvents.event("1", "NameDefined");

        publish(afterTheRestart);

        assertThat(idsOf(recorded.awaitAtLeast(1, DELIVERY_TIMEOUT)))
                .as("state left behind by the previous model must not stop a fresh subscription from delivering, which "
                        + "is what a checkpoint left in a state nothing can start from would do")
                .contains(afterTheRestart.getId());
    }
}
