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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.ConformanceEvents;
import org.occurrent.tck.latecommit.HeldAppend;
import org.occurrent.tck.latecommit.LateAppend;
import org.occurrent.tck.latecommit.LateCommitGuarantee;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * What a model owes a subscription that outlives the model itself. An event published while nothing was running is
 * either still delivered afterwards, or it is gone, and which of the two happens is a promise the model makes rather
 * than something a caller finds out the hard way.
 * <p>
 * This is the half of the contract {@link SubscriptionModelConformance} deliberately leaves alone, because it needs
 * state that survives the model. A model whose events arrive by being handed to it has none, and no way to be handed
 * one while it is down, so it declines this suite by not extending it. That absence is visible and searchable, which
 * is the mechanism ADR 77 rule (d) already relies on, and it is the right one here. The alternative, a declaration on
 * the base fixture, would have a branch that asserts nothing at all for those models, and a declaration that is free
 * on one branch is a switch for turning off the only test of a property.
 * <p>
 * A model that <em>can</em> answer still gets to say which way it goes, through
 * {@link RestartableSubscriptionModelFixture#resumesAfterARestart()}, because two models with identical durable state
 * underneath them genuinely differ. A change-stream model reads from wherever the server is now, and the same model
 * wrapped in one that keeps a checkpoint reads from where the checkpoint says. Both branches are asserted.
 * <p>
 * A model that resumes also owes the events a store commits out of position order. A store that reserves an event's
 * position before the transaction that writes it commits can let an event with a lower position become visible after
 * one with a higher position, and a subscription that stored the higher one as where it got to must still be delivered
 * the lower one when it restarts. {@link RestartableSubscriptionModelFixture#lateCommits()} is how the suite makes a
 * store do that, and a store that cannot declares so.
 * <p>
 * <strong>Delivery here is at-least-once, not exactly-once.</strong> A model resuming from the last position it stored
 * rather than from just after it hands that event over a second time, and nothing in Occurrent promises otherwise, so
 * these assertions are about what must arrive and never about what must not repeat.
 * <p>
 * The 60 second class timeout is the same number the other suites use, and the margin here is thinner than it looks.
 * The longest test chains four {@code deliveryTimeout()} waits, 40 seconds at the default, and between them it tears a
 * model down and builds another one, which against a real store means closing a change stream and opening a fresh one.
 * That leaves about 20 seconds for the rebuild. A fixture declaring a longer budget puts a matching {@code @Timeout} on
 * its own subclass, which {@link SubscriptionModelFixture#deliveryTimeout()} explains, since a wait that outlives the
 * class timeout reports a {@code TimeoutException} instead of naming the event that never arrived.
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

    private static final String HELD_STREAM = "stream:held";

    /**
     * Touched in setup, so a fixture that never answered fails before the first assertion rather than halfway through
     * a test.
     */
    @Override
    protected void checkFixtureCanAnswerThisSuite(SubscriptionModelFixture fixture) {
        ((RestartableSubscriptionModelFixture) fixture).lateCommits();
        ((RestartableSubscriptionModelFixture) fixture).honoursAGlobalPosition();
    }

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
        assertThat(subscriptionModel().subscribe(id, beforeTheRestart).waitUntilStarted(deliveryTimeout()))
                .as("the subscription must be listening before the event that establishes where it got to")
                .isTrue();
        CloudEvent delivered = ConformanceEvents.event("1", "NameDefined");
        publish(delivered);
        assertThat(idsOf(beforeTheRestart.awaitAtLeast(1, deliveryTimeout())))
                .as("the model has to be working before a restart says anything about it")
                .contains(delivered.getId());

        SubscriptionModel restarted = restartable().restart();

        // Published into the gap: no model is running, which is the only state that tells the two answers apart.
        CloudEvent whileNothingWasRunning = ConformanceEvents.event("2", "NameWasChanged");
        publish(whileNothingWasRunning);
        RecordedEvents afterTheRestart = new RecordedEvents();
        assertThat(restarted.subscribe(id, afterTheRestart).waitUntilStarted(deliveryTimeout()))
                .as("the rebuilt subscription must report started, or whichever branch runs below reports a missing "
                        + "event as a checkpoint problem when it was really a subscription that never started")
                .isTrue();

        if (restartable().resumesAfterARestart()) {
            List<CloudEvent> received = afterTheRestart.awaitUntil(
                    events -> idsOf(events).contains(whileNothingWasRunning.getId()), deliveryTimeout());
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
            assertThat(idsOf(afterTheRestart.awaitAtLeast(1, deliveryTimeout())))
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
        assertThat(restarted.subscribe(subscriptionId(), recorded).waitUntilStarted(deliveryTimeout()))
                .as("a rebuilt model is a working model, whatever it does about positions")
                .isTrue();
        CloudEvent afterTheRestart = ConformanceEvents.event("1", "NameDefined");

        publish(afterTheRestart);

        assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                .as("state left behind by the previous model must not stop a fresh subscription from delivering, which "
                        + "is what a checkpoint left in a state nothing can start from would do")
                .contains(afterTheRestart.getId());
    }

    @Test
    void a_restart_delivers_an_event_that_committed_after_a_later_one_was_delivered() {
        restartAfterALateCommit(restartable().resumesAfterARestart(), "resumes", false, (restarted, id, storedPosition, recorded) -> restarted.subscribe(id, recorded));
    }

    @Test
    void a_subscription_started_at_a_bare_global_position_is_refused() {
        if (restartable().honoursAGlobalPosition()) {
            restartAfterALateCommit(true, "honours a global position", false,
                    (restarted, id, storedPosition, recorded) -> restarted.subscribe(id, null, StartAt.checkpoint(storedPosition.get()), recorded));
            return;
        }
        String id = subscriptionId();
        RecordedEvents recorded = new RecordedEvents();
        StartAt bare = StartAt.checkpoint(GlobalCheckpoint.of(1));

        if (restartable().refusesABareGlobalPosition() && fixture().acceptedStartAtVariants().contains(StartAtVariant.CHECKPOINT)) {
            assertThatThrownBy(() -> subscriptionModel().subscribe(id, null, bare, recorded).waitUntilStarted(deliveryTimeout()))
                    .as("this model accepts a checkpoint but cannot prove that nothing committed below a bare position, "
                            + "so it refuses the start and names the subscription instead of resuming from a position "
                            + "that may skip an event")
                    .isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining(id);
        } else {
            assertThatThrownBy(() -> subscriptionModel().subscribe(id, null, bare, recorded))
                    .as("this model does not start from a position the application stored itself, so it refuses it "
                            + "rather than quietly ignoring it")
                    .isInstanceOf(UnsupportedStartAtException.class);
        }
        assertThat(recorded.soFar())
                .as("a refused subscription never listened, so nothing reaches its handler")
                .isEmpty();
    }

    @Test
    void a_subscription_started_at_the_checkpoint_delivered_with_an_event_delivers_an_event_that_committed_below_it() {
        if (restartable().honoursAGlobalPosition() || !restartable().refusesABareGlobalPosition()) {
            // Only a model that refuses a bare position owes the checkpoint that replaces it. One that honours a bare
            // position is covered by the test above, and one that does not accept a position has no checkpoint to
            // start from.
            return;
        }
        restartAfterALateCommit(true, "hands out a checkpoint with every event it delivers", true,
                (restarted, id, storedPosition, recorded) -> restarted.subscribe(id, null, StartAt.checkpoint(storedPosition.get()), recorded));
    }

    /**
     * Starts how the rebuilt subscription is made. {@code storedPosition} is the position an application would have
     * stored after the later event was delivered, which is only asked for by a start that applies one.
     */
    private interface Resume {
        Subscription start(SubscriptionModel restarted, String id, Supplier<Checkpoint> storedPosition, RecordedEvents recorded);
    }

    private void restartAfterALateCommit(boolean owesTheLateEvent, String declaration, boolean storesTheDeliveredCheckpoint, Resume resume) {
        String id = subscriptionId();
        RecordedEvents beforeTheRestart = new RecordedEvents();
        List<CloudEvent> deliveredBefore = new ArrayList<>();
        assertThat(subscriptionModel().subscribe(id, beforeTheRestart).waitUntilStarted(deliveryTimeout()))
                .as("the subscription must be listening before the append is held, or the later event would not be "
                        + "delivered to it")
                .isTrue();
        CloudEvent inFlight = ConformanceEvents.event("in-flight", "NameDefined");
        CloudEvent overtaking = ConformanceEvents.event("overtaking", "NameWasChanged");
        CloudEvent marker = ConformanceEvents.event("marker", "MarkerEvent");
        AtomicReference<HeldAppend> hold = new AtomicReference<>();

        try (LateAppend<Void> late = LateAppend.<Void>start(restartable().lateCommits(),
                held -> {
                    hold.set(held);
                    return () -> {
                        held.eventStore().write(HELD_STREAM, List.of(inFlight));
                        return null;
                    };
                },
                () -> {
                    publish(inFlight);
                    return null;
                },
                held -> held == null ? arrived(beforeTheRestart, deliveredBefore, inFlight) : isStored(held, inFlight))) {
            publish(overtaking);
            assertThat(arrived(beforeTheRestart, deliveredBefore, overtaking))
                    .as("the model has to deliver the later event before a restart says anything about the position it "
                            + "stored")
                    .isTrue();

            SubscriptionModel restarted = restartable().restart();
            deliveredBefore.addAll(beforeTheRestart.soFar());
            late.release();
            if (!late.commitsInPositionOrder()) {
                late.requireWindowEntered(positionStoredBy(hold.get(), inFlight), positionDelivered(deliveredBefore, overtaking));
            }

            RecordedEvents afterTheRestart = new RecordedEvents();
            // A store that commits in position order has no positions to hand back from a delivered event, so the
            // fixture's own starting point stands in for the one an application would have stored.
            Supplier<Checkpoint> storedPosition = () -> late.commitsInPositionOrder()
                    ? fixture().aCheckpointToStartFrom()
                    : storesTheDeliveredCheckpoint
                    ? checkpointDeliveredWith(deliveredBefore, overtaking)
                    : GlobalCheckpoint.of(positionDelivered(deliveredBefore, overtaking));
            assertThat(resume.start(restarted, id, storedPosition, afterTheRestart).waitUntilStarted(deliveryTimeout()))
                    .as("the rebuilt subscription must report started, or whichever branch runs below reports a missing "
                            + "event as a position problem when it was really a subscription that never started")
                    .isTrue();
            publish(marker);

            if (owesTheLateEvent) {
                boolean deliveredAlready = idsOf(deliveredBefore).contains(inFlight.getId());
                List<CloudEvent> received = afterTheRestart.awaitUntil(events -> idsOf(events).contains(marker.getId())
                        && (deliveredAlready || idsOf(events).contains(inFlight.getId())), deliveryTimeout());
                assertThat(idsOf(received))
                        .as("the marker proves the rebuilt subscription is delivering, so an event missing below is a "
                                + "position that skipped it rather than a subscription that never listened")
                        .contains(marker.getId());
                Set<String> delivered = new LinkedHashSet<>(idsOf(deliveredBefore));
                delivered.addAll(idsOf(received));
                LateCommitGuarantee.require(delivered.contains(inFlight.getId()),
                        "the event that committed after the one the subscription stored is delivered before the restart "
                                + "or by the rebuilt subscription, since this model declares it " + declaration,
                        () -> "the subscription was delivered " + idsOf(deliveredBefore) + " before the restart and "
                                + idsOf(received) + " after it");
            } else {
                List<CloudEvent> received = afterTheRestart.awaitUntil(
                        events -> idsOf(events).contains(marker.getId()), deliveryTimeout());
                assertThat(idsOf(received))
                        .as("this model declares it starts at the present, so only the marker arrives, and an event "
                                + "that committed before the rebuilt subscription started is gone")
                        .containsExactly(marker.getId());
            }
        }
    }

    private boolean arrived(RecordedEvents recorded, List<CloudEvent> sink, CloudEvent event) {
        if (!idsOf(sink).contains(event.getId())) {
            sink.addAll(recorded.awaitUntil(events -> idsOf(events).contains(event.getId()), deliveryTimeout()));
        }
        return idsOf(sink).contains(event.getId());
    }

    private static boolean isStored(HeldAppend held, CloudEvent event) {
        return idsOf(held.eventStore().read(HELD_STREAM).events()).contains(event.getId());
    }

    private static long positionStoredBy(@Nullable HeldAppend held, CloudEvent event) {
        if (held == null) {
            throw new IllegalStateException("The store declared it holds an append, but no append was held");
        }
        try (var events = held.eventStore().read(HELD_STREAM).events()) {
            return events.filter(candidate -> candidate.getId().equals(event.getId()))
                    .mapToLong(OccurrentCloudEventExtension::getPosition)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Event " + event.getId() + " was not found in " + HELD_STREAM
                            + " once the held append had committed"));
        }
    }

    private static Checkpoint checkpointDeliveredWith(List<CloudEvent> delivered, CloudEvent event) {
        return delivered.stream()
                .filter(candidate -> candidate.getId().equals(event.getId()))
                .findFirst()
                .map(CheckpointAwareCloudEvent::getCheckpointOrThrowIAE)
                .map(checkpoint -> (Checkpoint) new StringBasedCheckpoint(checkpoint.asString()))
                .orElseThrow(() -> new IllegalStateException("Event " + event.getId() + " was not delivered"));
    }

    private static long positionDelivered(List<CloudEvent> delivered, CloudEvent event) {
        long position = delivered.stream()
                .filter(candidate -> candidate.getId().equals(event.getId()))
                .mapToLong(OccurrentCloudEventExtension::getPosition)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Event " + event.getId() + " was not delivered"));
        if (position <= 0) {
            throw new IllegalStateException("The delivered event " + event.getId() + " carries no position, so the suite "
                    + "cannot check that the held append reserved its position before it");
        }
        return position;
    }
}
