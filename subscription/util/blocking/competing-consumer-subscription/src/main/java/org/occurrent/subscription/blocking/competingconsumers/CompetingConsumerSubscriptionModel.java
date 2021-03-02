package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.api.blocking.CompetingConsumersStrategy.CompetingConsumerListener;

import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.not;
import static org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel.CompetingConsumerState.*;

// TODO Add retry!!
public class CompetingConsumerSubscriptionModel implements DelegatingSubscriptionModel, SubscriptionModel, SubscriptionModelLifeCycle, CompetingConsumerListener {

    private final SubscriptionModel delegate;
    private final CompetingConsumersStrategy competingConsumersStrategy;

    private final ConcurrentMap<SubscriptionIdAndSubscriberId, CompetingConsumer> competingConsumers = new ConcurrentHashMap<>();

    public <T extends SubscriptionModel & SubscriptionModelLifeCycle> CompetingConsumerSubscriptionModel(T subscriptionModel, CompetingConsumersStrategy strategy) {
        requireNonNull(subscriptionModel, "Subscription model cannot be null");
        requireNonNull(subscriptionModel, CompetingConsumersStrategy.class.getSimpleName() + " cannot be null");
        this.delegate = subscriptionModel;
        this.competingConsumersStrategy = strategy;
        this.competingConsumersStrategy.addListener(this);
    }

    public Subscription subscribe(String subscriberId, String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriberId, "SubscriberId cannot be null");
        Objects.requireNonNull(subscriptionId, "SubscriptionId cannot be null");

        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        final CompetingConsumerSubscription competingConsumerSubscription;
        if (competingConsumersStrategy.registerCompetingConsumer(subscriptionId, subscriberId)) {
            Subscription subscription = delegate.subscribe(subscriptionId, filter, startAtSupplier, action);
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId, subscription);
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, RUNNING));
        } else {
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, WAITING, () -> delegate.subscribe(subscriptionId, filter, startAtSupplier, action)));
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId);
        }
        return competingConsumerSubscription;
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEvent> action) {
        return subscribe(UUID.randomUUID().toString(), subscriptionId, filter, startAtSupplier, action);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        competingConsumersStrategy.unregisterCompetingConsumer(subscriptionId, subscriptionId);
        ((SubscriptionModelLifeCycle) delegate).cancelSubscription(subscriptionId);
    }

    @Override
    public void stop() {
        unregisterAllCompetingConsumers();
        ((SubscriptionModelLifeCycle) delegate).stop();
    }

    @Override
    public void start() {
        registerAllCompetingConsumers();
        ((SubscriptionModelLifeCycle) delegate).start();
    }

    @Override
    public boolean isRunning() {
        return ((SubscriptionModelLifeCycle) delegate).isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return ((SubscriptionModelLifeCycle) delegate).isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return ((SubscriptionModelLifeCycle) delegate).isPaused(subscriptionId);
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        if (isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
        } else if (!isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not found");
        }

        SubscriptionModelLifeCycle delegate = (SubscriptionModelLifeCycle) this.delegate;
        findCompetingConsumerMatching(competingConsumer -> competingConsumer.isWaitingFor(subscriptionId))
                .map(competingConsumer -> {
                    // This works because method is synchronized and we've checked that it's already paused earlier
                    Subscription subscription = delegate.resumeSubscription(subscriptionId);
                    if (registerCompetingConsumer(subscriptionId, subscriptionId)) {
                        competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerRunning(() -> pauseSubscription(subscriptionId)));
                    }
                    return subscription;
                })
                .orElseThrow(() -> new IllegalStateException("Cannot resume subscription " + subscriptionId + " since another consumer currently subscribes to it."));

        return delegate.resumeSubscription(subscriptionId);
    }

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        if (isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not running");
        } else if (!isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not found");
        }

        findCompetingConsumerMatching(competingConsumer -> competingConsumer.hasSubscriptionId(subscriptionId) && competingConsumer.isRunning())
                .ifPresent(competingConsumer -> {
                    ((SubscriptionModelLifeCycle) delegate).pauseSubscription(subscriptionId);
                    unregisterCompetingConsumer(subscriptionId, subscriptionId);
                    // Register running action as "empty" since we don't want the subscription to be automatically resumed even if
                    // the subscriber is granted consumer status again (since it has been paused explicitly).
                    competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerWaiting(() -> {
                    }));
                });
    }

    @Override
    public SubscriptionModel getDelegatedSubscriptionModel() {
        return delegate;
    }

    @PreDestroy
    @Override
    public void shutdown() {
        unregisterAllCompetingConsumers();
        delegate.shutdown();
    }

    @Override
    public synchronized void onConsumeGranted(String subscriptionId, String subscriberId) {
        CompetingConsumer competingConsumer = competingConsumers.get(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId));
        if (competingConsumer == null) {
            return;
        }

        final CompetingConsumer updated;
        if (competingConsumer.isWaiting()) {
            competingConsumer.nextAction.run();
            updated = competingConsumer.registerRunning(() -> ((SubscriptionModelLifeCycle) delegate).pauseSubscription(subscriptionId));
        } else if (competingConsumer.isPaused()) {
            updated = competingConsumer.registerPaused(true);
        } else {
            updated = competingConsumer;
        }
        competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, updated);
    }

    @Override
    public synchronized void onConsumeProhibited(String subscriptionId, String subscriberId) {
        CompetingConsumer competingConsumer = competingConsumers.get(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId));
        if (competingConsumer == null) {
            return;
        }

        final CompetingConsumer updated;
        if (competingConsumer.isRunning()) {
            competingConsumer.nextAction.run();
            updated = competingConsumer.registerWaiting(() -> ((SubscriptionModelLifeCycle) delegate).resumeSubscription(subscriptionId));
        } else if (competingConsumer.isPaused()) {
            updated = competingConsumer.registerPaused(false);
        } else {
            updated = competingConsumer;
        }
        competingConsumers.put(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId), updated);
    }

    private static class SubscriptionIdAndSubscriberId {
        private final String subscriptionId;
        private final String subscriberId;

        private SubscriptionIdAndSubscriberId(String subscriptionId, String subscriberId) {
            this.subscriptionId = subscriptionId;
            this.subscriberId = subscriberId;
        }

        private static SubscriptionIdAndSubscriberId from(String subscriptionId, String subscriberId) {
            return new SubscriptionIdAndSubscriberId(subscriptionId, subscriberId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SubscriptionIdAndSubscriberId)) return false;
            SubscriptionIdAndSubscriberId that = (SubscriptionIdAndSubscriberId) o;
            return Objects.equals(subscriptionId, that.subscriptionId) && Objects.equals(subscriberId, that.subscriberId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subscriptionId, subscriberId);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", SubscriptionIdAndSubscriberId.class.getSimpleName() + "[", "]")
                    .add("subscriptionId='" + subscriptionId + "'")
                    .add("subscriberId='" + subscriberId + "'")
                    .toString();
        }
    }


    static class CompetingConsumer {

        final SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId;
        final CompetingConsumerState state;
        final Runnable nextAction;
        final boolean hasPermissionToConsume;

        CompetingConsumer(SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId, CompetingConsumerState state, boolean hasPermissionToConsume) {
            this(subscriptionIdAndSubscriberId, state, hasPermissionToConsume, () -> {
            });
        }

        CompetingConsumer(SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId, CompetingConsumerState state, boolean hasPermissionToConsume, Runnable nextAction) {
            this.subscriptionIdAndSubscriberId = subscriptionIdAndSubscriberId;
            this.state = state;
            this.hasPermissionToConsume = hasPermissionToConsume;
            this.nextAction = nextAction;
        }

        boolean hasId(String subscriptionId, String subscriberId) {
            return hasSubscriptionId(subscriptionId) && Objects.equals(getSubscriberId(), subscriberId);
        }

        boolean hasSubscriptionId(String subscriptionId) {
            return Objects.equals(getSubscriptionId(), subscriptionId);
        }

        boolean isPaused() {
            return state == CompetingConsumerState.PAUSED;
        }

        boolean isRunning() {
            return state == RUNNING;
        }

        boolean isRunningFor(String subscriptionId) {
            return isRunning() && hasSubscriptionId(subscriptionId);
        }

        boolean isWaiting() {
            return state == WAITING;
        }

        boolean isWaitingFor(String subscriptionId) {
            return isWaiting() && hasSubscriptionId(subscriptionId);
        }

        String getSubscriptionId() {
            return subscriptionIdAndSubscriberId.subscriptionId;
        }

        String getSubscriberId() {
            return subscriptionIdAndSubscriberId.subscriberId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompetingConsumer)) return false;
            CompetingConsumer that = (CompetingConsumer) o;
            return hasPermissionToConsume == that.hasPermissionToConsume && Objects.equals(subscriptionIdAndSubscriberId, that.subscriptionIdAndSubscriberId) && state == that.state && Objects.equals(nextAction, that.nextAction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subscriptionIdAndSubscriberId, state, nextAction, hasPermissionToConsume);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", CompetingConsumer.class.getSimpleName() + "[", "]")
                    .add("subscriptionIdAndSubscriberId=" + subscriptionIdAndSubscriberId)
                    .add("state=" + state)
                    .add("nextAction=" + nextAction)
                    .add("hasPermissionToConsume=" + hasPermissionToConsume)
                    .toString();
        }

        CompetingConsumer registerRunning(Runnable nextAction) {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, RUNNING, true, nextAction);
        }

        CompetingConsumer registerWaiting(Runnable nextAction) {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, WAITING, false, nextAction);
        }

        CompetingConsumer registerPaused(boolean hasPermissionToConsume) {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, PAUSED, hasPermissionToConsume, () -> {
            });
        }
    }

    enum CompetingConsumerState {
        RUNNING, WAITING, PAUSED
    }

    private void unregisterAllCompetingConsumers() {
        unregisterCompetingConsumersMatching(CompetingConsumer::isRunning);
    }

    private void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        unregisterCompetingConsumersMatching(c -> c.hasId(subscriptionId, subscriberId));
    }

    private void unregisterCompetingConsumersMatching(Predicate<CompetingConsumer> predicate) {
        competingConsumers.values().stream().filter(predicate).forEach(s -> competingConsumersStrategy.unregisterCompetingConsumer(s.getSubscriptionId(), s.getSubscriberId()));
    }

    private boolean registerAllCompetingConsumers() {
        return registerCompetingConsumerMatching(not(CompetingConsumer::isRunning));
    }

    private boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        return registerCompetingConsumerMatching(c -> c.hasId(subscriptionId, subscriberId));
    }

    private Optional<CompetingConsumer> findCompetingConsumerMatching(Predicate<CompetingConsumer> predicate) {
        return competingConsumers.values().stream().filter(predicate).findFirst();
    }

    private boolean registerCompetingConsumerMatching(Predicate<CompetingConsumer> predicate) {
        return competingConsumers.values().stream()
                .filter(predicate)
                .collect(() -> new AtomicBoolean(false),
                        (matching, s) -> {
                            if (competingConsumersStrategy.registerCompetingConsumer(s.getSubscriptionId(), s.getSubscriberId())) {
                                matching.set(true);
                            }
                        },
                        (aBoolean, aBoolean2) -> aBoolean.set(aBoolean.get()))
                .get();
    }
}