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

package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerStatus.State;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.HistoryLossMessages;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * What {@link CompetingConsumerSubscriptionModel#competingConsumerStatus(String)} reports for a subscription on this
 * node. A subscription is failing exactly when something keeps it from where it belongs here: the wrapped model refused
 * to start it and this node gave it up, the wrapped model refused to resume it from its position and this node holds it
 * paused, or a call that was tried again failed with no success since. Standing by for a lease another node holds, a
 * pause by the user and a stopped model are where it belongs, so they are not failing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerReportsItsStatusTest {

    private static final String NODE = "node";
    private static final String ID = "subscription";
    private static final Duration EVENTUALLY = Duration.ofSeconds(10);

    private final Wrapped wrapped = new Wrapped();
    private final Strategy strategy = new Strategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);

    @AfterEach
    void shutdown() {
        wrapped.pausesFail = false;
        strategy.hasLockFails = false;
        wrapped.pauseGate = null;
        model.shutdown();
    }

    @Test
    void reports_a_subscription_that_runs_under_the_lease_this_node_holds_as_lease_held_and_not_failing() {
        strategy.grantOnRegister = true;

        subscribe();

        CompetingConsumerStatus status = status();
        assertThat(status.state()).isEqualTo(State.LEASE_HELD);
        assertThat(status.subscriptionId()).isEqualTo(ID);
        assertThat(status.subscriberId()).isEqualTo(NODE);
        assertThat(status.isFailing()).as("failing").isFalse();
    }

    @Test
    void reports_a_subscription_that_waits_for_a_lease_another_node_holds_as_standby_and_not_failing() {
        strategy.grantOnRegister = false;

        subscribe();

        assertThat(status().state()).isEqualTo(State.STANDBY);
        assertThat(status().isFailing()).as("failing").isFalse();
    }

    @Test
    void reports_a_subscription_whose_lease_this_node_lost_as_standby_and_not_failing() {
        strategy.grantOnRegister = true;
        subscribe();

        strategy.holders.remove(ID);
        model.onConsumeProhibited(ID, NODE);

        assertThat(model.isPaused(ID)).as("paused, which is how the model reports a lost lease").isTrue();
        assertThat(status().state()).isEqualTo(State.STANDBY);
        assertThat(status().isFailing()).as("failing").isFalse();
    }

    @Test
    void reports_a_subscription_the_user_paused_as_paused_and_not_failing_until_the_user_resumes_it() {
        strategy.grantOnRegister = true;
        subscribe();

        model.pauseSubscription(ID);

        assertThat(status().state()).isEqualTo(State.PAUSED);
        assertThat(status().isFailing()).as("failing").isFalse();

        model.resumeSubscription(ID);

        assertThat(status().state()).isEqualTo(State.LEASE_HELD);
        assertThat(status().isFailing()).as("failing").isFalse();
    }

    @Test
    void reports_every_subscription_as_stopped_and_not_failing_while_the_model_is_stopped_until_it_is_started() {
        strategy.grantOnRegister = true;
        subscribe();

        model.stop();

        assertThat(status().state()).isEqualTo(State.STOPPED);
        assertThat(status().isFailing()).as("failing").isFalse();

        model.start();

        assertThat(status().state()).isEqualTo(State.LEASE_HELD);
    }

    @Test
    void reports_nothing_for_an_id_it_does_not_know_and_lists_the_statuses_of_the_ones_it_knows_ordered_by_id() {
        strategy.grantOnRegister = true;
        model.subscribe(NODE, "b", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        model.subscribe(NODE, "a", null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThat(model.competingConsumerStatus("unknown")).isEmpty();
        assertThat(model.competingConsumerStatuses()).extracting(CompetingConsumerStatus::subscriptionId).containsExactly("a", "b");
    }

    @Test
    void reports_a_pause_in_the_wrapped_model_that_keeps_failing_as_failing_since_its_first_failure_and_not_failing_once_it_works() {
        // Given a subscription that lost its lease, whose pause in the wrapped model fails every time it is tried
        strategy.grantOnRegister = true;
        subscribe();
        wrapped.pausesFail = true;
        strategy.holders.remove(ID);

        // When
        model.onConsumeProhibited(ID, NODE);

        // Then it stays on standby, which is where it belongs, but it is failing, and the failing is not forgotten between tries
        CompetingConsumerStatus first = awaitFailing();
        assertThat(first.state()).isEqualTo(State.STANDBY);
        assertThat(first.failure().cause()).hasMessageContaining("pause fails");
        int pausesSoFar = wrapped.pauseCalls.get();
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(wrapped.pauseCalls.get()).as("pauses tried").isGreaterThanOrEqualTo(pausesSoFar + 2));
        CompetingConsumerStatus later = status();
        assertThat(later.isFailing()).as("failing while the pause still fails").isTrue();
        assertThat(later.failure().since()).as("since").isEqualTo(first.failure().since());

        // When the wrapped model pauses it
        wrapped.pausesFail = false;

        // Then
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(status().isFailing()).as("failing").isFalse());
        assertThat(status().state()).isEqualTo(State.STANDBY);
        assertThat(wrapped.isPaused(ID)).as("paused in the wrapped model").isTrue();
    }

    @Test
    void reports_a_lease_strategy_that_cannot_answer_for_a_granted_lease_as_failing_on_standby_and_as_lease_held_once_it_answers() {
        // Given a waiting subscription whose strategy fails when asked whether the granted lease is still held
        strategy.grantOnRegister = false;
        subscribe();
        strategy.hasLockFails = true;
        strategy.holders.add(ID);

        // When
        model.onConsumeGranted(ID, NODE);

        // Then
        CompetingConsumerStatus failing = awaitFailing();
        assertThat(failing.state()).isEqualTo(State.STANDBY);
        assertThat(failing.failure().cause()).hasMessageContaining("hasLock fails");

        // When the strategy answers again
        strategy.hasLockFails = false;

        // Then the try runs the subscription, and it is no longer failing
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(status().state()).isEqualTo(State.LEASE_HELD));
        assertThat(status().isFailing()).as("failing").isFalse();
    }

    @Test
    void forgets_the_failure_of_a_subscription_when_it_is_cancelled() {
        strategy.grantOnRegister = true;
        subscribe();
        wrapped.pausesFail = true;
        strategy.holders.remove(ID);
        model.onConsumeProhibited(ID, NODE);
        awaitFailing();

        model.cancelSubscription(ID);

        assertThat(model.competingConsumerStatus(ID)).isEmpty();
        assertThat(model.competingConsumerStatuses()).isEmpty();
    }

    @Test
    void reports_a_subscription_the_wrapped_model_refused_to_start_as_given_up_and_failing_with_the_refusal() {
        // Given
        strategy.grantOnRegister = true;
        subscribe();

        // When the wrapped model refuses the start after subscribe returned
        wrapped.refuse(ID);

        // Then
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(model.competingConsumerStatus(ID)).as("status").isPresent());
        CompetingConsumerStatus status = status();
        assertThat(status.state()).isEqualTo(State.GIVEN_UP);
        assertThat(status.isFailing()).as("failing").isTrue();
        assertThat(status.failure().cause()).isInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("Refused the start of " + ID);
        assertThat(model.subscriptionIds()).as("subscription ids, the id is free").doesNotContain(ID);
        assertThat(model.competingConsumerStatuses()).as("statuses").containsExactly(status);
    }

    @Test
    void reports_a_refused_start_as_given_up_while_the_call_that_holds_the_subscription_has_not_returned() throws Exception {
        // Given a pause by the user that waits inside the wrapped model, holding the subscription
        strategy.grantOnRegister = true;
        subscribe();
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        wrapped.pauseGate = new Gate(inside, release);
        Thread pause = new Thread(() -> {
            try {
                model.pauseSubscription(ID);
            } catch (RuntimeException ignored) {
                // The subscription is refused while the pause waits, so nothing is left for it to pause
            }
        });
        pause.start();
        try {
            assertThat(inside.await(10, SECONDS)).as("the pause is inside the wrapped model").isTrue();

            // When the wrapped model refuses the start meanwhile
            wrapped.refuse(ID);

            // Then it is reported at once, which the pause that holds the subscription cannot hold up
            assertThat(pause.isAlive()).as("the pause still waits").isTrue();
            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(model.competingConsumerStatus(ID)).map(CompetingConsumerStatus::state).contains(State.GIVEN_UP));
            assertThat(status().isFailing()).as("failing").isTrue();
        } finally {
            release.countDown();
            pause.join(10_000);
        }
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(model.subscriptionIds()).as("subscription ids, once the pause returned").doesNotContain(ID));
        assertThat(status().state()).isEqualTo(State.GIVEN_UP);
    }

    @Test
    void stops_reporting_a_given_up_subscription_when_the_id_is_subscribed_again_and_reports_the_new_one() {
        strategy.grantOnRegister = true;
        subscribe();
        wrapped.refuse(ID);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(model.competingConsumerStatus(ID)).map(CompetingConsumerStatus::state).contains(State.GIVEN_UP));

        subscribe();

        assertThat(status().state()).isEqualTo(State.LEASE_HELD);
        assertThat(status().isFailing()).as("failing").isFalse();
        assertThat(model.competingConsumerStatuses()).as("statuses").hasSize(1);
    }

    @Test
    void stops_reporting_a_given_up_subscription_when_the_id_is_subscribed_again_and_that_subscribe_fails() {
        strategy.grantOnRegister = true;
        subscribe();
        wrapped.refuse(ID);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(model.competingConsumerStatus(ID)).map(CompetingConsumerStatus::state).contains(State.GIVEN_UP));
        wrapped.subscribesFail = true;

        assertThatThrownBy(this::subscribe).isInstanceOf(IllegalStateException.class);

        assertThat(model.competingConsumerStatus(ID)).as("the id of a subscribe that failed is unknown, not given up").isEmpty();
    }

    @Test
    void stops_reporting_a_given_up_subscription_when_it_is_cancelled_although_nothing_else_knows_the_id() {
        strategy.grantOnRegister = true;
        subscribe();
        wrapped.refuse(ID);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(model.competingConsumerStatus(ID)).map(CompetingConsumerStatus::state).contains(State.GIVEN_UP));
        assertThat(model.subscriptionIds()).as("subscription ids").doesNotContain(ID);

        model.cancelSubscription(ID);

        assertThat(model.competingConsumerStatus(ID)).isEmpty();
        assertThat(model.competingConsumerStatuses()).isEmpty();
    }

    @Test
    void reports_nothing_once_the_model_is_shut_down_not_even_for_a_given_up_subscription() {
        strategy.grantOnRegister = true;
        subscribe();
        wrapped.refuse(ID);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(model.competingConsumerStatus(ID)).map(CompetingConsumerStatus::state).contains(State.GIVEN_UP));

        model.shutdown();

        assertThat(model.competingConsumerStatus(ID)).isEmpty();
        assertThat(model.competingConsumerStatuses()).isEmpty();
    }

    @Test
    void reports_a_subscription_held_paused_because_the_wrapped_model_refused_to_start_it_from_its_position_as_paused_and_failing_with_the_refusal() {
        // Given a waiting subscription whose lease is granted while the wrapped model refuses to start it from its position
        strategy.grantOnRegister = false;
        subscribe();
        wrapped.refusing = true;

        // When
        strategy.holders.add(ID);
        model.onConsumeGranted(ID, NODE);

        // Then it is held paused, and it is failing, which a pause by the user is not
        CompetingConsumerStatus status = status();
        assertThat(status.state()).isEqualTo(State.PAUSED);
        assertThat(status.isFailing()).as("failing").isTrue();
        assertThat(status.failure().cause()).isInstanceOf(CatchupCannotResumeException.class);
        assertThat(model.isPaused(ID)).as("paused").isTrue();
    }

    @Test
    void stops_reporting_the_refusal_of_a_held_subscription_once_the_user_resumes_it() {
        // Given
        strategy.grantOnRegister = false;
        subscribe();
        wrapped.refusing = true;
        strategy.holders.add(ID);
        model.onConsumeGranted(ID, NODE);
        assertThat(status().isFailing()).as("failing while held").isTrue();
        wrapped.refusing = false;
        strategy.grantOnRegister = true;

        // When
        model.resumeSubscription(ID);

        // Then
        assertThat(status().state()).isEqualTo(State.LEASE_HELD);
        assertThat(status().isFailing()).as("failing").isFalse();
    }

    @Test
    void keeps_a_held_subscription_failing_when_the_user_resumes_it_and_the_wrapped_model_refuses_again() {
        // Given
        strategy.grantOnRegister = false;
        subscribe();
        wrapped.refusing = true;
        strategy.holders.add(ID);
        model.onConsumeGranted(ID, NODE);
        strategy.grantOnRegister = true;

        // When
        assertThatThrownBy(() -> model.resumeSubscription(ID)).isInstanceOf(CatchupCannotResumeException.class);

        // Then
        assertThat(status().state()).isEqualTo(State.PAUSED);
        assertThat(status().isFailing()).as("failing").isTrue();
    }

    @Test
    void forgets_the_refusal_of_a_held_subscription_when_it_is_cancelled_and_does_not_report_it_for_a_new_subscription_with_the_same_id() {
        // Given
        strategy.grantOnRegister = false;
        subscribe();
        wrapped.refusing = true;
        strategy.holders.add(ID);
        model.onConsumeGranted(ID, NODE);
        assertThat(status().isFailing()).as("failing while held").isTrue();
        model.cancelSubscription(ID);
        wrapped.refusing = false;

        // When
        subscribe();
        model.pauseSubscription(ID);

        // Then
        assertThat(status().state()).isEqualTo(State.PAUSED);
        assertThat(status().isFailing()).as("failing, a pause by the user").isFalse();
    }

    @Test
    void reports_a_call_that_failed_as_failing_from_the_moment_it_failed_until_the_try_that_follows_succeeds() throws Exception {
        // Given a try that waits before it first runs, so the failure can be seen before any try has failed
        CountDownLatch tryMayRun = new CountDownLatch(1);
        model.runBeforeATryWaitsForItsBackoff(() -> {
            try {
                tryMayRun.await(30, SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        strategy.grantOnRegister = true;
        subscribe();
        wrapped.pausesFail = true;
        strategy.holders.remove(ID);

        try {
            // When the pause that follows the loss of the lease fails
            model.onConsumeProhibited(ID, NODE);

            // Then
            CompetingConsumerStatus failing = status();
            assertThat(failing.isFailing()).as("failing before any try").isTrue();
            assertThat(failing.failure().cause()).hasMessageContaining("pause fails");
        } finally {
            wrapped.pausesFail = false;
            tryMayRun.countDown();
        }

        // And when the try that follows succeeds
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(status().isFailing()).as("failing once the try succeeded").isFalse());
        assertThat(wrapped.isPaused(ID)).as("paused in the wrapped model").isTrue();
    }

    @Test
    void does_not_report_the_refusal_of_a_subscription_that_was_held_and_resumed_when_the_user_pauses_it_later() {
        strategy.grantOnRegister = false;
        subscribe();
        wrapped.refusing = true;
        strategy.holders.add(ID);
        model.onConsumeGranted(ID, NODE);
        wrapped.refusing = false;
        strategy.grantOnRegister = true;
        model.resumeSubscription(ID);

        model.pauseSubscription(ID);

        assertThat(status().state()).isEqualTo(State.PAUSED);
        assertThat(status().isFailing()).as("failing, a pause by the user").isFalse();
    }

    private void subscribe() {
        model.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    private CompetingConsumerStatus status() {
        return model.competingConsumerStatus(ID).orElseThrow(() -> new AssertionError("expected a status for " + ID));
    }

    private CompetingConsumerStatus awaitFailing() {
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(model.competingConsumerStatus(ID)).as("status").map(CompetingConsumerStatus::isFailing).contains(true));
        return status();
    }

    private record Gate(CountDownLatch inside, CountDownLatch release) {
        void pass() {
            inside.countDown();
            try {
                release.await(30, SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // An in-memory model whose pause can fail or wait, whose start and resume can be refused from the position, and which
    // can refuse a start after subscribe returned, telling its listeners as a MongoDB model does
    private static final class Wrapped extends InMemorySubscriptionModel implements StartRefusalReportingSubscriptions {
        private final List<StartRefusalListener> listeners = new CopyOnWriteArrayList<>();
        private final AtomicInteger pauseCalls = new AtomicInteger();
        private volatile boolean pausesFail;
        private volatile boolean refusing;
        private volatile boolean subscribesFail;
        private volatile @Nullable Gate pauseGate;

        private Wrapped() {
            super(RetryStrategy.none());
        }

        void refuse(String subscriptionId) {
            super.cancelSubscription(subscriptionId);
            listeners.forEach(listener -> listener.handleStartRefusal(subscriptionId, new UnsupportedStartAtException(StartAt.now(), "Refused the start of " + subscriptionId)));
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            refuseFromThePosition();
            failIfSubscribesFail();
            return super.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            refuseFromThePosition();
            failIfSubscribesFail();
            return super.subscribePaused(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            refuseFromThePosition();
            return super.resumeSubscription(subscriptionId);
        }

        private void failIfSubscribesFail() {
            if (subscribesFail) {
                throw new IllegalStateException("The wrapped model cannot subscribe");
            }
        }

        private void refuseFromThePosition() {
            if (refusing) {
                throw HistoryLossMessages.cannotStartFrom(ID, (Checkpoint) () -> "stored position");
            }
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            pauseCalls.incrementAndGet();
            @Nullable Gate gate = pauseGate;
            if (gate != null) {
                gate.pass();
            }
            if (pausesFail) {
                throw new IllegalStateException("The wrapped model's pause fails");
            }
            super.pauseSubscription(subscriptionId);
        }

        @Override
        public void addStartRefusalListener(StartRefusalListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeStartRefusalListener(StartRefusalListener listener) {
            listeners.remove(listener);
        }
    }

    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private volatile boolean grantOnRegister;
        private volatile boolean hasLockFails;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            if (hasLockFails) {
                throw new IllegalStateException("The lease strategy's hasLock fails");
            }
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }
}
