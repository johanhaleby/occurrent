package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Covers the state transitions a competing consumer goes through when it is paused before it has won the lock,
 * deterministically and without MongoDB. {@link CompetingConsumerSubscriptionModelTest#can_pause_and_resume_subscription_that_is_in_waiting_state()}
 * exercises the same scenario against a real lease strategy, this class isolates the transitions the strategy's own
 * timing would otherwise hide.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelPausedWhileWaitingTest {

    private static final String SUBSCRIBER_ID = "subscriber";

    private final FakeSubscriptionModel delegate = new FakeSubscriptionModel();
    private final FakeCompetingConsumerStrategy strategy = new FakeCompetingConsumerStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

    @Test
    void pausing_a_waiting_consumer_is_recorded_and_reported_truthfully() {
        String subscriptionId = subscribeAsWaiting();

        model.pauseSubscription(subscriptionId);

        assertThat(model.isPaused(subscriptionId)).isTrue();
        assertThat(model.isRunning(subscriptionId)).isFalse();
    }

    @Test
    void pausing_a_waiting_consumer_unregisters_it_rather_than_leaving_it_competing() {
        String subscriptionId = subscribeAsWaiting();
        strategy.calls.clear();

        model.pauseSubscription(subscriptionId);

        assertThat(strategy.calls).containsExactly("unregister:" + subscriptionId + ":" + SUBSCRIBER_ID);
    }

    @Test
    void a_grant_arriving_while_paused_does_not_start_delivery_and_hands_the_lock_back() {
        String subscriptionId = subscribeAsWaiting();
        model.pauseSubscription(subscriptionId);
        strategy.calls.clear();

        // Simulates the strategy's own refresh thread granting the lock after the pause unregistered it, the race
        // this fix works around rather than closes (occurrent#651).
        strategy.grant(subscriptionId, SUBSCRIBER_ID);

        assertThat(delegate.isRunning(subscriptionId)).isFalse();
        assertThat(model.isPaused(subscriptionId)).isTrue();
        assertThat(strategy.calls).containsExactly("unregister:" + subscriptionId + ":" + SUBSCRIBER_ID);
    }

    @Test
    void resuming_registers_again_and_starts_immediately_if_that_register_grants_the_lock() {
        String subscriptionId = subscribeAsWaiting();
        model.pauseSubscription(subscriptionId);
        strategy.willGrantOnNextRegister();

        model.resumeSubscription(subscriptionId);

        assertThat(model.isRunning(subscriptionId)).isTrue();
        assertThat(model.isPaused(subscriptionId)).isFalse();
        assertThat(delegate.isRunning(subscriptionId)).isTrue();
    }

    @Test
    void pausing_a_waiting_consumer_twice_refuses_the_second_pause() {
        String subscriptionId = subscribeAsWaiting();
        model.pauseSubscription(subscriptionId);

        Throwable throwable = catchThrowable(() -> model.pauseSubscription(subscriptionId));

        assertThat(throwable).isInstanceOf(SubscriptionNotRunningException.class);
    }

    @Test
    void a_stop_and_start_round_trip_keeps_the_pause_through_stop_then_resumes_it_on_start() {
        String subscriptionId = subscribeAsWaiting();
        model.pauseSubscription(subscriptionId);

        model.stop();
        assertThat(model.isPaused(subscriptionId))
                .as("stop() converts only running consumers to paused, so an already-paused waiting consumer stays paused")
                .isTrue();

        model.start(true);
        assertThat(model.isPaused(subscriptionId)).isFalse();
        assertThat(model.isRunning(subscriptionId)).isFalse();
        assertThat(strategy.isRegistered(subscriptionId, SUBSCRIBER_ID))
                .as("start() resumes every paused consumer, including one that was waiting when it was paused")
                .isTrue();
    }

    @Test
    void a_system_paused_consumer_does_not_resume_from_a_late_grant_while_the_model_is_stopped() {
        String subscriptionId = "subscriptionId";
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), event -> {
        });
        strategy.prohibit(subscriptionId, SUBSCRIBER_ID);
        assertThat(model.isPaused(subscriptionId)).isTrue();

        model.stop();
        strategy.calls.clear();

        // The race in occurrent#651: a grant can still land for a consumer stop() just unregistered.
        strategy.grant(subscriptionId, SUBSCRIBER_ID);

        assertThat(delegate.isRunning(subscriptionId))
                .as("the model is stopped, so a system-paused consumer must not resume delivery")
                .isFalse();
        assertThat(strategy.calls).containsExactly("unregister:" + subscriptionId + ":" + SUBSCRIBER_ID);
    }

    /**
     * Gives a rival subscriber the lock first, so the model's own consumer for the same subscription id loses the
     * race and is left Waiting.
     */
    private String subscribeAsWaiting() {
        String subscriptionId = "subscriptionId";
        strategy.grantToRival(subscriptionId, "rival-subscriber");
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), event -> {
        });
        return subscriptionId;
    }

    private record Key(String subscriptionId, String subscriberId) {
    }

    private static final class FakeSubscriptionModel implements SubscriptionModel {
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            runningIds.add(subscriptionId);
            pausedIds.remove(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return runningIds.contains(subscriptionId) && !pausedIds.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            pausedIds.remove(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            pausedIds.add(subscriptionId);
        }
    }

    private record FakeSubscription(String id) implements SubscriptionHandle {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    /**
     * Mirrors the shipped Mongo strategies' observable contract. The lock is held by at most one subscriber per
     * subscription id, register grants it when free or forced and reports the transition, unregister forgets the
     * consumer for good, release keeps it competing. {@link #grant} simulates a grant the strategy's own refresh
     * thread hands out with no register call from the model, the shape of the race in occurrent#651.
     */
    private static final class FakeCompetingConsumerStrategy implements CompetingConsumerStrategy {
        // subscriptionId -> subscriberId currently holding the lock, absent if nobody does
        private final Map<String, String> lockHolder = new HashMap<>();
        private final Set<Key> registered = new HashSet<>();
        private final List<CompetingConsumerListener> listeners = new ArrayList<>();
        private final List<String> calls = new ArrayList<>();
        private boolean forceGrantOnNextRegister = false;

        void willGrantOnNextRegister() {
            forceGrantOnNextRegister = true;
        }

        boolean isRegistered(String subscriptionId, String subscriberId) {
            return registered.contains(new Key(subscriptionId, subscriberId));
        }

        void grant(String subscriptionId, String subscriberId) {
            lockHolder.put(subscriptionId, subscriberId);
            listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
        }

        /**
         * Simulates the strategy losing the lease for a currently-held consumer with no register call from the
         * model, the way an expired lease does in the real refresh thread.
         */
        void prohibit(String subscriptionId, String subscriberId) {
            listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }

        /**
         * Gives a rival subscriber the lock with no register call, so a later register for a different subscriber
         * on the same subscription id is refused.
         */
        void grantToRival(String subscriptionId, String rivalSubscriberId) {
            lockHolder.put(subscriptionId, rivalSubscriberId);
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            Key key = new Key(subscriptionId, subscriberId);
            calls.add("register:" + subscriptionId + ":" + subscriberId);
            boolean wasHolder = subscriberId.equals(lockHolder.get(subscriptionId));
            boolean forced = forceGrantOnNextRegister;
            forceGrantOnNextRegister = false;
            boolean lockIsFree = !lockHolder.containsKey(subscriptionId);
            boolean acquired = forced || wasHolder || lockIsFree;
            registered.add(key);
            if (acquired) {
                lockHolder.put(subscriptionId, subscriberId);
            }
            // wasHolder implies acquired here (it is one of the ORed terms above), so a holder can never lose the
            // lock through its own register call, only onConsumeGranted is reachable from this method.
            if (!wasHolder && acquired) {
                listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
            }
            return acquired;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("unregister:" + subscriptionId + ":" + subscriberId);
            boolean wasHolder = subscriberId.equals(lockHolder.get(subscriptionId));
            registered.remove(new Key(subscriptionId, subscriberId));
            if (wasHolder) {
                lockHolder.remove(subscriptionId);
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("release:" + subscriptionId + ":" + subscriberId);
            boolean wasHolder = subscriberId.equals(lockHolder.get(subscriptionId));
            if (wasHolder) {
                lockHolder.remove(subscriptionId);
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return subscriberId.equals(lockHolder.get(subscriptionId));
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
