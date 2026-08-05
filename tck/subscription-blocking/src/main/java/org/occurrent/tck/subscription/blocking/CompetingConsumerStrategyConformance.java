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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.tck.subscription.blocking.RecordedLockChanges.Kind;
import org.occurrent.tck.subscription.blocking.RecordedLockChanges.LockChange;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The contract every {@link CompetingConsumerStrategy} owes: at most one subscriber consumes a subscription at a time,
 * and the lock always finds its way to somebody who wants it.
 * <p>
 * Occurrent consumes this contract two ways, and the suite covers both. {@code CompetingConsumerSubscriptionModel}
 * registers a listener and reacts to what it is told, while {@code SagaRunner} registers a consumer, never adds a
 * listener at all, and asks {@link CompetingConsumerStrategy#hasLock(String, String)} on every poll. A strategy that
 * serves only the first of those passes nothing here.
 * <p>
 * <strong>What the suite does not have an opinion about is how a strategy coordinates.</strong> Occurrent's two
 * implementations use a lease in MongoDB, and none of that appears below: no test knows a lease exists, waits out one,
 * or asserts when one expires. What is asserted instead is the property a lease is one way of providing, that a holder
 * which stops coordinating loses the lock to a rival rather than blocking the subscription forever. The lease's own
 * timing is a property of the implementation, and Occurrent pins it where it belongs, in deterministic tests against
 * the MongoDB support class with a clock the test moves itself.
 * <p>
 * Every wait here is for something that must arrive, bounded by {@link CompetingConsumerStrategyFixture#timeToConverge()}.
 * There is no wait for a quiet period: it would pass just as well against a strategy that coordinates with nobody, and
 * any constant short enough to keep the suite quick is short enough to flake on a loaded machine.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the competing consumer strategy contract")
@Timeout(60)
public abstract class CompetingConsumerStrategyConformance {

    private static final String HOLDER = "the-holder";
    private static final String RIVAL = "the-rival";

    private @Nullable CompetingConsumerStrategyFixture fixture;
    private final List<CompetingConsumerStrategy> obtainedFromTheFactory = new ArrayList<>();

    /**
     * Creates a fixture whose storage holds no locks. Called before every test method.
     */
    protected abstract CompetingConsumerStrategyFixture createFixture();

    @BeforeEach
    final void createFixtureAndCheckItAnswers() {
        CompetingConsumerStrategyFixture created = requireNonNull(createFixture(), "createFixture() returned null");
        // Touch the accessor now, so a fixture that has not wired up its strategy says so before the first assertion
        // rather than halfway through a test that looks like a coordination failure.
        requireNonNull(created.competingConsumerStrategy(),
                created.getClass().getName() + " returned null from competingConsumerStrategy()");
        requireNonNull(created.timeToConverge(),
                created.getClass().getName() + " returned null from timeToConverge()");
        this.fixture = created;
    }

    @AfterEach
    final void shutDownEveryStrategyAndCloseTheFixture() {
        List<CompetingConsumerStrategy> toShutDown = List.copyOf(obtainedFromTheFactory);
        obtainedFromTheFactory.clear();
        CompetingConsumerStrategyFixture current = this.fixture;
        this.fixture = null;
        try {
            toShutDown.forEach(CompetingConsumerStrategy::shutdown);
        } finally {
            if (current != null) {
                current.close();
            }
        }
    }

    protected final CompetingConsumerStrategyFixture fixture() {
        CompetingConsumerStrategyFixture current = this.fixture;
        if (current == null) {
            throw new IllegalStateException("No fixture. It is created and closed per test method, so it cannot be "
                    + "reached from @BeforeAll or @AfterAll. Anything shared across the class, a container or a "
                    + "client, belongs in one of those rather than in the fixture.");
        }
        return current;
    }

    protected final CompetingConsumerStrategy strategy() {
        return fixture().competingConsumerStrategy();
    }

    /**
     * Another strategy contending over the same storage, shut down after the test whatever happens.
     */
    private CompetingConsumerStrategy rival() {
        CompetingConsumerStrategy rival = requireNonNull(fixture().newCompetingConsumerStrategy(),
                fixture().getClass().getName() + " returned null from newCompetingConsumerStrategy()");
        obtainedFromTheFactory.add(rival);
        return rival;
    }

    @Test
    void the_first_consumer_to_register_gets_the_lock() {
        String subscription = subscriptionId();

        boolean acquired = strategy().registerCompetingConsumer(subscription, HOLDER);

        assertThat(acquired).as("nothing else has registered, so there is nobody to lose to").isTrue();
        assertThat(strategy().hasLock(subscription, HOLDER))
                .as("hasLock has to agree with what registering just returned, since a consumer polling for the lock "
                        + "and a consumer reading the return value must not get different answers")
                .isTrue();
    }

    @Test
    void a_rival_registering_for_the_same_subscription_does_not_get_the_lock() {
        String subscription = subscriptionId();
        strategy().registerCompetingConsumer(subscription, HOLDER);

        boolean acquired = rival().registerCompetingConsumer(subscription, RIVAL);

        assertThat(acquired)
                .as("two subscribers on one subscription and one of them consuming is the whole point of the contract")
                .isFalse();
        assertThat(strategy().hasLock(subscription, HOLDER))
                .as("the rival registering must not disturb the holder")
                .isTrue();
    }

    @Test
    void consumers_of_different_subscriptions_do_not_contend() {
        String first = subscriptionId();
        String second = subscriptionId();

        boolean acquiredFirst = strategy().registerCompetingConsumer(first, HOLDER);
        boolean acquiredSecond = rival().registerCompetingConsumer(second, RIVAL);

        assertThat(acquiredFirst).isTrue();
        assertThat(acquiredSecond)
                .as("competition is per subscription. A strategy locking more coarsely than that would let one "
                        + "subscription starve every other one running on the same storage")
                .isTrue();
    }

    @Test
    void a_consumer_that_never_registered_does_not_have_the_lock() {
        String subscription = subscriptionId();
        strategy().registerCompetingConsumer(subscription, HOLDER);

        assertThat(strategy().hasLock(subscription, "someone-else"))
                .as("hasLock answers for the subscriber it is asked about, not for the subscription")
                .isFalse();
        assertThat(strategy().hasLock(subscriptionId(), HOLDER))
                .as("and for the subscription it is asked about, not for the subscriber")
                .isFalse();
    }

    @Test
    void registering_reports_the_grant_to_a_listener() {
        String subscription = subscriptionId();
        RecordedLockChanges listener = new RecordedLockChanges();
        strategy().addListener(listener);

        strategy().registerCompetingConsumer(subscription, HOLDER);

        assertThat(listener.awaitAtLeast(1, fixture().timeToConverge()))
                .as("a consumer driven by callbacks starts consuming when it is told it may, so a strategy that grants "
                        + "the lock without saying so leaves that consumer idle while it holds the lock")
                .containsExactly(granted(subscription, HOLDER));
    }

    @Test
    void unregistering_hands_the_lock_to_a_rival() {
        String subscription = subscriptionId();
        strategy().registerCompetingConsumer(subscription, HOLDER);
        CompetingConsumerStrategy rival = rival();
        RecordedLockChanges rivalListener = new RecordedLockChanges();
        rival.addListener(rivalListener);
        rival.registerCompetingConsumer(subscription, RIVAL);

        strategy().unregisterCompetingConsumer(subscription, HOLDER);

        assertThat(rivalListener.awaitAtLeast(1, fixture().timeToConverge()))
                .as("the rival never registers a second time, so a strategy that only reconsiders who holds a lock "
                        + "when somebody registers leaves the subscription unconsumed from here on")
                .containsExactly(granted(subscription, RIVAL));
        assertThat(rival.hasLock(subscription, RIVAL)).isTrue();
    }

    @Test
    void releasing_leaves_the_lock_to_be_taken_rather_than_lying_unheld() {
        String subscription = subscriptionId();
        strategy().registerCompetingConsumer(subscription, HOLDER);
        CompetingConsumerStrategy rival = rival();
        rival.registerCompetingConsumer(subscription, RIVAL);

        strategy().releaseCompetingConsumer(subscription, HOLDER);

        // Deliberately not "the rival gets it". Releasing keeps the consumer that released registered, so it is one of
        // the candidates for the lock it just gave up, and which of them wins is a race between schedules the contract
        // says nothing about. Demanding the rival here would be asserting the phase two background threads happen to
        // be in. What a released lock may never do is stay unheld, and a caller that needs the stronger guarantee
        // unregisters instead, which is the test above.
        assertThat(awaitEither(
                () -> rival.hasLock(subscription, RIVAL),
                () -> strategy().hasLock(subscription, HOLDER)))
                .as("somebody registered for this subscription has to end up consuming it. A release that left the "
                        + "lock lying there would stop the subscription for good while both consumers sat waiting for "
                        + "the other one")
                .isTrue();
    }

    @Test
    void a_released_consumer_loses_the_lock_and_takes_it_back_on_its_own() {
        String subscription = subscriptionId();
        strategy().registerCompetingConsumer(subscription, HOLDER);

        strategy().releaseCompetingConsumer(subscription, HOLDER);

        assertThat(strategy().hasLock(subscription, HOLDER))
                .as("releasing means the consumer no longer receives events, so a consumer polling for the lock must "
                        + "stop being told it has one. Answering yes here is what would let a saga's timer poller keep "
                        + "firing timers after the lock it was gated on was given up")
                .isFalse();
        assertThat(awaitLock(strategy(), subscription, HOLDER))
                .as("and releasing leaves the consumer in the running, so it takes the lock back on its own with "
                        + "nobody calling registerCompetingConsumer again. This is what a subscription paused by the "
                        + "system rather than by a user rests on, since nothing will explicitly resume it")
                .isTrue();
    }

    @Test
    void an_unregistered_consumer_loses_the_lock_and_does_not_take_it_back() {
        String unregistered = subscriptionId();
        String released = subscriptionId();
        strategy().registerCompetingConsumer(unregistered, HOLDER);
        strategy().registerCompetingConsumer(released, HOLDER);

        strategy().unregisterCompetingConsumer(unregistered, HOLDER);
        strategy().releaseCompetingConsumer(released, HOLDER);

        assertThat(strategy().hasLock(unregistered, HOLDER)).isFalse();
        // The released consumer taking its lock back is the previous test's property, used here as the one thing that
        // proves the strategy's own coordination has run to completion. Waiting out a period instead would pass just
        // as well against a strategy whose coordination never runs at all.
        assertThat(awaitLock(strategy(), released, HOLDER))
                .as("the strategy has to reach a settled answer for the released consumer before the assertion below "
                        + "says anything about the unregistered one")
                .isTrue();
        assertThat(strategy().hasLock(unregistered, HOLDER))
                .as("unregistering is the stronger of the two, since the consumer is forgotten and does not come back "
                        + "without registering again, which is what lets a subscription paused by a user stay paused "
                        + "until the user resumes it")
                .isFalse();
    }

    @Test
    void a_rival_takes_over_from_a_holder_that_stopped_coordinating() {
        String subscription = subscriptionId();
        CompetingConsumerStrategy holder = rival();
        holder.registerCompetingConsumer(subscription, HOLDER);
        RecordedLockChanges listener = new RecordedLockChanges();
        strategy().addListener(listener);
        strategy().registerCompetingConsumer(subscription, RIVAL);

        stopCoordinating(holder);

        assertThat(listener.awaitAtLeast(1, fixture().timeToConverge()))
                .as("a consumer that stops coordinating without releasing or unregistering is what a crashed instance "
                        + "looks like from the outside, and a lock it keeps forever is a subscription nobody consumes "
                        + "ever again. This is the one liveness property the whole pattern exists for")
                .containsExactly(granted(subscription, RIVAL));
        assertThat(strategy().hasLock(subscription, RIVAL)).isTrue();
    }

    @Test
    void a_consumer_with_no_listener_learns_it_has_the_lock_by_asking() {
        String subscription = subscriptionId();
        strategy().registerCompetingConsumer(subscription, HOLDER);
        // No listener anywhere in this test on purpose. A saga's timer poller registers a consumer, never adds one, and
        // gates every tick on hasLock, so a strategy that reports a change only through listeners serves it nothing.
        CompetingConsumerStrategy rival = rival();
        rival.registerCompetingConsumer(subscription, RIVAL);

        strategy().unregisterCompetingConsumer(subscription, HOLDER);

        assertThat(awaitLock(rival, subscription, RIVAL))
                .as("hasLock is the whole of what a strategy reports to a consumer that polls, so it has to keep up "
                        + "with a handover on its own")
                .isTrue();
    }

    @Test
    void a_lock_change_is_reported_once_rather_than_on_every_refresh() {
        String subscription = subscriptionId();
        RecordedLockChanges listener = new RecordedLockChanges();
        strategy().addListener(listener);
        strategy().registerCompetingConsumer(subscription, HOLDER);

        strategy().releaseCompetingConsumer(subscription, HOLDER);

        // Three is the smallest number of changes this can settle on, so waiting for three ends as soon as the lock
        // has been given up and taken back rather than after a fixed period.
        List<LockChange> changes = listener.awaitAtLeast(3, fixture().timeToConverge());

        assertThat(changes)
                .as("nobody else is competing for this subscription, so the consumer holds the lock, gives it up, and "
                        + "is the only candidate left to take it back")
                .startsWith(granted(subscription, HOLDER), prohibited(subscription, HOLDER), granted(subscription, HOLDER));
        // A repeat is what this is about, so it is stated as the absence of one rather than as an exact sequence.
        // Losing the lock again and taking it back again is allowed, and demanding a length of three would only
        // assert that the machine was not busy.
        assertThat(withoutConsecutiveRepeats(changes))
                .as("a listener is told what changed, not what is currently true. A strategy reporting on every round "
                        + "of its own coordination instead lands a repeat here for each round the lock stayed free, "
                        + "and a consumer acting on each one redoes the work it already did")
                .isEqualTo(changes);
    }

    @Test
    void a_removed_listener_is_told_nothing_further() {
        String subscription = subscriptionId();
        RecordedLockChanges removed = new RecordedLockChanges();
        RecordedLockChanges kept = new RecordedLockChanges();
        strategy().addListener(removed);
        strategy().addListener(kept);

        strategy().removeListener(removed);
        strategy().registerCompetingConsumer(subscription, HOLDER);

        assertThat(kept.awaitAtLeast(1, fixture().timeToConverge()))
                .as("the listener that stayed is what says the strategy reports at all, so the assertion below is "
                        + "about removal rather than about a strategy that reports nothing to anyone")
                .containsExactly(granted(subscription, HOLDER));
        assertThat(removed.soFar())
                .as("a consumer that has finished with a strategy removes its listener, and one that keeps being "
                        + "called reacts on behalf of a subscription it no longer has")
                .isEmpty();
    }

    /**
     * Shuts a strategy down as a stand-in for the process it runs in going away, and takes it off the list the suite
     * shuts down afterwards so it is never shut down twice.
     */
    private void stopCoordinating(CompetingConsumerStrategy strategy) {
        obtainedFromTheFactory.remove(strategy);
        strategy.shutdown();
    }

    /**
     * Waits until the subscriber holds the lock, or {@code timeToConverge()} runs out, and answers whether it does.
     * <p>
     * This polls, which the rest of the suite avoids. It has to, since {@code hasLock} is a question and a consumer
     * with no listener has nothing else to go on, so a suite covering that consumer has nothing to block on either.
     * The answer is returned rather than asserted so the caller says what the answer means.
     */
    private boolean awaitLock(CompetingConsumerStrategy strategy, String subscriptionId, String subscriberId) {
        return awaitUntil(() -> strategy.hasLock(subscriptionId, subscriberId));
    }

    /**
     * Waits until either question answers yes. Two of them, because where the contract allows more than one consumer
     * to end up with the lock, insisting on a particular one would be asserting a race rather than a contract.
     */
    private boolean awaitEither(BooleanSupplier one, BooleanSupplier other) {
        return awaitUntil(() -> one.getAsBoolean() || other.getAsBoolean());
    }

    private boolean awaitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + fixture().timeToConverge().toNanos();
        while (true) {
            if (condition.getAsBoolean()) {
                return true;
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            sleep(POLL_INTERVAL);
        }
    }

    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a lock to be granted", e);
        }
    }

    /**
     * The same changes with any run of identical ones collapsed to one, so a comparison against the original says
     * whether anything was reported twice running and shows where.
     */
    private static List<LockChange> withoutConsecutiveRepeats(List<LockChange> changes) {
        List<LockChange> collapsed = new ArrayList<>();
        changes.forEach(change -> {
            if (collapsed.isEmpty() || !collapsed.get(collapsed.size() - 1).equals(change)) {
                collapsed.add(change);
            }
        });
        return collapsed;
    }

    private static LockChange granted(String subscriptionId, String subscriberId) {
        return new LockChange(subscriptionId, subscriberId, Kind.GRANTED);
    }

    private static LockChange prohibited(String subscriptionId, String subscriberId) {
        return new LockChange(subscriptionId, subscriberId, Kind.PROHIBITED);
    }

    private static String subscriptionId() {
        return UUID.randomUUID().toString();
    }
}
