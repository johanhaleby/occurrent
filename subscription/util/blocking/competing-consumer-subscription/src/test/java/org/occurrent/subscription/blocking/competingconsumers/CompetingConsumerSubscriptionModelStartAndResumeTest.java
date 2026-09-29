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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What starting the model, and resuming or pausing a subscription, do when the lease is not free, when the strategy
 * throws, or when the wrapped model throws on starting itself or on a subscription, competing or not. The strategy
 * tells its listeners about a grant on the thread that registers, the way the MongoDB lease strategies do, and nothing
 * here needs MongoDB.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelStartAndResumeTest {

    private static final String SUBSCRIBER_ID = "subscriber";

    private final RecordingDelegate delegate = new RecordingDelegate();
    private final SynchronousLeaseStrategy strategy = new SynchronousLeaseStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

    @Test
    void start_throws_what_the_wrapped_model_threw_once_every_consumer_had_its_turn() {
        strategy.grantOnRegister = false;
        subscribe("failing-1");
        subscribe("healthy");
        subscribe("failing-2");
        model.stop();
        strategy.grantOnRegister = true;
        delegate.throwsOn.addAll(Set.of("failing-1", "failing-2"));

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("the caller of start learns that a subscription did not start").isInstanceOf(IllegalStateException.class);
        assertThat(thrown.getSuppressed()).as("and learns about every one of them").hasSize(1);
        assertThat(delegate.running).as("a failing consumer does not keep the others from starting").containsExactly("healthy");
        assertThat(strategy.holders).as("a consumer that failed to start gave its lease back").containsExactly("healthy");
    }

    @Test
    void resuming_a_consumer_the_wrapped_model_throws_on_throws_to_the_caller() {
        strategy.grantOnRegister = false;
        subscribe("failing");
        model.pauseSubscription("failing");
        strategy.grantOnRegister = true;
        delegate.throwsOn.add("failing");

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("failing"));

        assertThat(thrown).as("the grant that registering brought failed to start the subscription, and the caller of resume is told")
                .isInstanceOf(IllegalStateException.class);
        assertThat(strategy.holders).isEmpty();
        assertThat(model.isRunning("failing")).isFalse();
    }

    @Test
    void a_consumer_start_resumes_while_another_node_holds_its_lease_is_resumed_once_this_node_wins_it() {
        strategy.grantOnRegister = true;
        subscribe("stopped");
        model.stop();
        strategy.grantOnRegister = false;
        model.start(true);
        assertThat(model.isRunning("stopped")).isFalse();

        strategy.grant("stopped");

        assertThat(model.isRunning("stopped"))
                .as("the grant resumes the subscription rather than handing the lease back as if a user had paused it")
                .isTrue();
        assertThat(strategy.holders).containsExactly("stopped");
        assertThat(strategy.calls).as("nothing gave the lease up after the grant").endsWith("grant stopped");
    }

    @Test
    void a_consumer_whose_registration_threw_on_start_is_resumed_by_the_next_start() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        strategy.registerThrows = true;
        assertThat(catchThrowable(() -> model.start(true))).as("the lease store is down").isInstanceOf(IllegalStateException.class);
        strategy.registerThrows = false;

        model.start(true);

        assertThat(delegate.running).as("x resumes on the next start once the lease store is back").contains("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_user_pause_of_a_consumer_waiting_for_its_lease_keeps_it_paused_after_the_grant() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.pauseSubscription("x");
        strategy.grantOnRegister = false;
        model.resumeSubscription("x");

        Throwable thrown = catchThrowable(() -> model.pauseSubscription("x"));
        strategy.grant("x");

        assertThat(delegate.running).as("the grant does not resume a subscription the user paused").doesNotContain("x");
        assertThat(thrown).as("the pause is recorded rather than refused").isNull();
        assertThat(model.isPaused("x")).isTrue();
        assertThat(strategy.holders).as("the grant was handed back").isEmpty();
    }

    @Test
    void a_consumer_the_wrapped_model_fails_to_resume_on_start_resumes_on_the_next_grant() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        delegate.throwsOn.add("x");
        assertThat(catchThrowable(() -> model.start(true))).isInstanceOf(IllegalStateException.class);
        assertThat(strategy.calls).as("x gives its lease back and stays registered, so it keeps competing for it").endsWith("release x");
        delegate.throwsOn.clear();

        strategy.grant("x");

        assertThat(delegate.running).as("x keeps competing for the lease, so the next grant resumes it, with no other node to take it over").contains("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_non_competing_subscription_the_wrapped_model_fails_to_resume_does_not_keep_a_competing_consumer_from_starting() {
        strategy.grantOnRegister = true;
        subscribe("x");
        subscribeNonCompeting("nc");
        model.stop();
        delegate.throwsOn.add("nc");

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("the caller of start learns that nc did not resume").hasMessage("The wrapped model cannot start nc right now");
        assertThat(delegate.running).as("x starts although nc fails").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_wrapped_model_that_fails_to_start_does_not_keep_the_subscriptions_from_their_turn() {
        strategy.grantOnRegister = true;
        subscribe("x");
        subscribeNonCompeting("nc");
        model.stop();
        delegate.startThrows = true;

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("the caller of start learns that the wrapped model did not start").hasMessage("The wrapped model cannot start right now");
        assertThat(thrown.getSuppressed()).as("nc and x still get their turn, and fail as well, since resuming them starts the wrapped model").hasSize(2);
        assertThat(strategy.calls).as("x gives back the lease it won").endsWith("release x");
        delegate.startThrows = false;
        strategy.grant("x");
        assertThat(delegate.running).as("x keeps competing for the lease, so the next grant resumes it").containsExactly("x");
    }

    @Test
    void a_stop_after_a_start_that_found_the_lease_taken_keeps_the_subscription_stopped_when_this_node_is_granted_the_lease() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        strategy.grantOnRegister = false;
        model.start(true);

        model.stop();
        strategy.grant("x");

        assertThat(delegate.running).as("nothing runs after the user stopped the model").isEmpty();
        assertThat(strategy.holders).isEmpty();
        assertThat(model.isPaused("x")).isTrue();
    }

    @Test
    void a_stop_after_a_start_that_failed_part_way_keeps_the_subscription_stopped_when_this_node_is_granted_the_lease() {
        strategy.grantOnRegister = true;
        subscribe("x");
        subscribeNonCompeting("nc");
        model.stop();
        strategy.grantOnRegister = false;
        delegate.startThrows = true;
        assertThat(catchThrowable(() -> model.start(true))).isInstanceOf(IllegalStateException.class);
        delegate.startThrows = false;

        model.stop();
        strategy.grant("x");

        assertThat(delegate.running).as("nothing runs after the user stopped the model").isEmpty();
        assertThat(strategy.holders).isEmpty();
    }

    @Test
    void a_stop_after_a_start_that_found_the_lease_taken_keeps_a_consumer_waiting_for_its_lease_from_starting_when_this_node_is_granted_it() {
        strategy.grantOnRegister = false;
        subscribe("x");
        model.stop();
        model.start(true);

        model.stop();
        strategy.grant("x");

        assertThat(delegate.running).as("nothing runs after the user stopped the model").isEmpty();
        assertThat(strategy.holders).isEmpty();
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_takes_no_lease_until_the_model_is_started() {
        strategy.grantOnRegister = true;
        model.stop();

        subscribe("x");

        assertThat(strategy.holders).as("a stopped node holds no lease, so another node can take x").isEmpty();
        assertThat(delegate.running).isEmpty();
        assertThat(model.isPaused("x")).isTrue();
        model.start(true);
        assertThat(delegate.running).as("x competes for its lease once the model is started").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_stays_paused_after_a_resume_of_another_started_the_wrapped_model() {
        strategy.grantOnRegister = true;
        subscribe("y");
        model.stop();
        model.resumeSubscription("y");

        subscribe("x");

        assertThat(delegate.running).as("x delivers nothing while the model is stopped").containsExactly("y");
        assertThat(strategy.holders).containsExactly("y");
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_and_another_node_holds_its_lease_starts_once_this_node_wins_it_after_a_start() {
        strategy.grantOnRegister = false;
        model.stop();
        subscribe("x");
        model.start(true);

        strategy.grant("x");

        assertThat(delegate.running).containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_custom_lease_strategy_that_throws_on_one_consumer_does_not_keep_another_from_resuming() {
        strategy.grantOnRegister = true;
        subscribe("a");
        subscribe("b");
        model.stop();
        strategy.hasLockThrowsOn.add("a");

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("the caller of start learns that the strategy threw for a").hasMessage("A custom lease strategy failed to answer for a");
        assertThat(delegate.running).as("b resumes although asking about a threw").containsExactly("b");
    }

    @Test
    void a_subscription_that_wins_its_lease_while_the_wrapped_model_is_stopped_is_delivered_once_after_a_start() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        strategy.grantOnRegister = false;
        model.start(true);
        assertThat(delegate.isRunning()).as("a start that won no lease does not start the wrapped model").isFalse();
        strategy.grantOnRegister = true;

        subscribe("y");
        strategy.grantOnRegister = false;
        model.start(true);

        assertThat(delegate.running).as("y is subscribed once, in a wrapped model that was started first").containsExactly("y");
    }

    @Test
    void a_subscription_that_wins_its_lease_while_the_wrapped_model_fails_to_start_gives_the_lease_back_and_keeps_nothing() {
        strategy.grantOnRegister = true;
        subscribe("y");
        model.stop();
        model.start(false);
        delegate.startThrows = true;

        Throwable thrown = catchThrowable(() -> subscribe("x"));

        assertThat(thrown).as("the caller of subscribe learns that the wrapped model did not start").hasMessage("The wrapped model cannot start right now");
        assertThat(strategy.holders).as("the lease nothing on this node serves is given back").isEmpty();
        assertThat(model.subscriptionIds()).as("the failed subscription is not kept").containsExactly("y");
    }

    @Test
    void a_subscription_the_user_resumes_while_the_model_is_stopped_runs_once_this_node_wins_its_lease_later() {
        strategy.grantOnRegister = false;
        subscribe("x");
        model.stop();
        model.resumeSubscription("x");

        strategy.grant("x");

        assertThat(delegate.running).as("a resume runs x whether the lease is won straight away or later").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_stop_that_the_wrapped_model_fails_still_gives_every_lease_back() {
        strategy.grantOnRegister = true;
        subscribe("x");
        delegate.stopThrows = true;

        Throwable thrown = catchThrowable(model::stop);

        assertThat(thrown).as("the caller of stop learns that the wrapped model did not stop").hasMessage("The wrapped model cannot stop right now");
        assertThat(strategy.holders).as("a stopped node holds no lease, although the wrapped model failed to stop").isEmpty();
        assertThat(model.isPaused("x")).isTrue();
    }

    @Test
    void a_grant_for_a_lease_this_node_no_longer_holds_starts_nothing() {
        strategy.grantOnRegister = false;
        subscribe("x");

        strategy.grantWithoutTheLease("x");

        assertThat(delegate.running).as("x waits for a lease this node holds").isEmpty();
        assertThat(model.isRunning("x")).isFalse();
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_is_held_paused_by_the_wrapped_model_from_the_subscribe() {
        strategy.grantOnRegister = true;
        model.stop();

        subscribe("x");

        assertThat(delegate.isPaused("x")).as("the wrapped model has x, and records where it starts, from the subscribe").isTrue();
        assertThat(model.isRunning("x")).isFalse();
        model.start(true);
        assertThat(delegate.running).as("winning the lease resumes x once").containsExactly("x");
    }

    @Test
    void a_start_without_resuming_makes_a_subscription_made_while_the_model_was_stopped_compete_for_its_lease() {
        strategy.grantOnRegister = true;
        model.stop();
        subscribe("x");

        model.start(false);

        assertThat(delegate.running).as("x was never paused, so it competes for its lease and runs once it wins it").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_start_without_resuming_makes_a_subscription_waiting_for_its_lease_at_the_stop_compete_again() {
        strategy.grantOnRegister = false;
        subscribe("x");
        model.stop();

        model.start(false);
        strategy.grant("x");

        assertThat(delegate.running).as("x was waiting, not paused, so it competes again and runs once it wins its lease").containsExactly("x");
    }

    @Test
    void a_consumer_recorded_as_running_that_the_wrapped_model_fails_to_resume_is_resumed_by_a_later_grant() {
        strategy.grantOnRegister = true;
        // Nothing pauses x when it gives its lease back, so only what this model records for it decides the later grant
        strategy.tellsTheListenersAboutARelease = false;
        subscribe("x");
        // The wrapped model has lost x, as it does for a catch-up replay that failed, while x is recorded as running here
        delegate.cancelSubscription("x");
        delegate.throwsOn.add("x");
        assertThat(catchThrowable(() -> model.resumeSubscription("x"))).isInstanceOf(IllegalStateException.class);
        assertThat(strategy.calls).as("x gives its lease back and stays registered").endsWith("release x");
        delegate.throwsOn.clear();

        strategy.grant("x");

        assertThat(delegate.running).as("the later grant tries x again").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    private void subscribe(String subscriptionId) {
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    // A start position that resolves to null here makes the model hand the subscription straight to the wrapped model
    private void subscribeNonCompeting(String subscriptionId) {
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.dynamic(__ -> null), __ -> {
        });
    }

    /**
     * Keeps track of which subscriptions deliver and which are paused, and throws when starting any subscription in
     * {@link #throwsOn}. Like {@code SpringMongoSubscriptionModel}, it holds a subscription made while it is stopped
     * paused, and starts itself to resume a subscription. A subscription delivered twice is listed twice in
     * {@link #running}.
     */
    private static final class RecordingDelegate implements SubscriptionModel {
        private final Set<String> throwsOn = new HashSet<>();
        private final List<String> running = new ArrayList<>();
        private final Set<String> paused = new HashSet<>();
        private boolean started = true;
        private boolean startThrows;
        private boolean stopThrows;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throwIfRefused(subscriptionId);
            if (started) {
                running.add(subscriptionId);
            } else {
                paused.add(subscriptionId);
            }
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running.removeIf(subscriptionId::equals);
            paused.remove(subscriptionId);
        }

        @Override
        public void stop() {
            if (stopThrows) {
                throw new IllegalStateException("The wrapped model cannot stop right now");
            }
            started = false;
            paused.addAll(running);
            running.clear();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            if (startThrows) {
                throw new IllegalStateException("The wrapped model cannot start right now");
            }
            started = true;
        }

        @Override
        public boolean isRunning() {
            return started;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throwIfRefused(subscriptionId);
            if (!started) {
                start(false);
            }
            paused.remove(subscriptionId);
            running.add(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            running.removeIf(subscriptionId::equals);
            paused.add(subscriptionId);
        }

        private void throwIfRefused(String subscriptionId) {
            if (throwsOn.contains(subscriptionId)) {
                throw new IllegalStateException("The wrapped model cannot start " + subscriptionId + " right now");
            }
        }
    }

    private record FakeSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    /**
     * Grants the lease on register when {@link #grantOnRegister} is set, and tells the listeners on the registering
     * thread, as a lease strategy does for a lease that changed hands. {@link #grant(String)} plays a refresh round
     * granting a lease that another node gave up, which, as with the MongoDB lease strategies, only a registered
     * consumer can win. Releasing a lease keeps the consumer registered, unregistering does not. Both tell the listeners
     * when the consumer held the lease, a release only while {@link #tellsTheListenersAboutARelease} is set.
     */
    private static final class SynchronousLeaseStrategy implements CompetingConsumerStrategy {
        private final List<String> calls = new ArrayList<>();
        private final Set<String> registered = new HashSet<>();
        private final Set<String> holders = new HashSet<>();
        private final Set<String> hasLockThrowsOn = new HashSet<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();
        private boolean grantOnRegister;
        private boolean registerThrows;
        private boolean tellsTheListenersAboutARelease = true;

        /**
         * A grant the strategy decided before the lease moved on, which reaches the listeners once this node no longer
         * holds it.
         */
        void grantWithoutTheLease(String subscriptionId) {
            calls.add("stale grant " + subscriptionId);
            listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, SUBSCRIBER_ID));
        }

        void grant(String subscriptionId) {
            if (!registered.contains(subscriptionId)) {
                calls.add("no grant for unregistered " + subscriptionId);
                return;
            }
            calls.add("grant " + subscriptionId);
            holders.add(subscriptionId);
            listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, SUBSCRIBER_ID));
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("register " + subscriptionId);
            if (registerThrows) {
                throw new IllegalStateException("The lease store cannot be reached");
            }
            registered.add(subscriptionId);
            if (grantOnRegister && holders.add(subscriptionId)) {
                listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
            }
            return holders.contains(subscriptionId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("unregister " + subscriptionId);
            registered.remove(subscriptionId);
            if (holders.remove(subscriptionId)) {
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("release " + subscriptionId);
            if (holders.remove(subscriptionId) && tellsTheListenersAboutARelease) {
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            if (hasLockThrowsOn.contains(subscriptionId)) {
                throw new IllegalStateException("A custom lease strategy failed to answer for " + subscriptionId);
            }
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }
    }
}
