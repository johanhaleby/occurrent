package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;

import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.not;
import static org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel.CompetingConsumerState.*;

// TODO Add retry!!
public class CompetingConsumerSubscriptionModel implements DelegatingSubscriptionModel, SubscriptionModel, SubscriptionModelLifeCycle, CompetingConsumerListener {

    private final SubscriptionModel delegate;
    private final CompetingConsumerStrategy competingConsumerStrategy;

    private final ConcurrentMap<SubscriptionIdAndSubscriberId, CompetingConsumer> competingConsumers = new ConcurrentHashMap<>();

    public CompetingConsumerSubscriptionModel(SubscriptionModel subscriptionModel, CompetingConsumerStrategy strategy) {
        requireNonNull(subscriptionModel, "Subscription model cannot be null");
        requireNonNull(subscriptionModel, CompetingConsumerStrategy.class.getSimpleName() + " cannot be null");
        this.delegate = subscriptionModel;
        this.competingConsumerStrategy = strategy;
        this.competingConsumerStrategy.addListener(this);
    }

    public Subscription subscribe(String subscriberId, String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriberId, "SubscriberId cannot be null");
        Objects.requireNonNull(subscriptionId, "SubscriptionId cannot be null");

        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        final CompetingConsumerSubscription competingConsumerSubscription;
        if (competingConsumerStrategy.registerCompetingConsumer(subscriptionId, subscriberId)) {
            Subscription subscription = delegate.subscribe(subscriptionId, filter, startAt, action);
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId, subscription);
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, new Running()));
        } else {
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, new Waiting(() -> delegate.subscribe(subscriptionId, filter, startAt, action))));
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId);
        }
        return competingConsumerSubscription;
    }

    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(UUID.randomUUID().toString(), subscriptionId, filter, startAt, action);
    }

    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        delegate.cancelSubscription(subscriptionId);
        findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId))
                .ifPresent(this::removeCompetingConsumer);
    }

    @Override
    public synchronized void stop() {
        if (!isRunning()) {
            return;
        }

        delegate.stop();
        removeAllCompetingConsumers();
    }

    @Override
    public synchronized void start() {
        if (isRunning()) {
            throw new IllegalStateException(CompetingConsumerSubscriptionModel.class.getSimpleName() + " is already started");
        }

        competingConsumers.values().stream()
                .filter(not(CompetingConsumer::isRunning))
                .forEach(cc -> {
                            // Only change state if we have permission to consume
                            if (cc.isWaiting() && competingConsumerStrategy.registerCompetingConsumer(cc.getSubscriptionId(), cc.getSubscriberId())) {
                                startWaitingConsumer(cc);
                            } else if (cc.isPaused()) {
                                resumeSubscription(cc.getSubscriptionId());
                            }
                        }
                );
    }

    @Override
    public boolean isRunning() {
        return delegate.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return delegate.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return delegate.isPaused(subscriptionId);
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        if (isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
        } else if (!isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not found");
        }

        SubscriptionModelLifeCycle delegate = this.delegate;
        return findFirstCompetingConsumerMatching(competingConsumer -> competingConsumer.isPausedFor(subscriptionId))
                .flatMap(competingConsumer -> {
                    final Subscription subscription;
                    if (hasLock(subscriptionId, subscriptionId) || registerCompetingConsumer(subscriptionId, subscriptionId)) {
                        competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerRunning());
                        // This works because method is and we've checked that it's already paused earlier
                        subscription = delegate.resumeSubscription(subscriptionId);
                    } else {
                        subscription = null;
                    }
                    return Optional.ofNullable(subscription);
                })
                .orElseThrow(() -> new IllegalStateException("Cannot resume subscription " + subscriptionId + " since another consumer currently subscribes to it."));
    }

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        if (isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not running");
        } else if (!isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not found");
        }

        findFirstCompetingConsumerMatching(competingConsumer -> competingConsumer.hasSubscriptionId(subscriptionId) && competingConsumer.isRunning())
                .ifPresent(competingConsumer -> {
                    System.out.println("### BEFORE PAUSINGING DELEGAT");
                    delegate.pauseSubscription(subscriptionId);
                    competingConsumers.put(SubscriptionIdAndSubscriberId.from(competingConsumer), competingConsumer.registerPaused());
                    competingConsumerStrategy.unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
                    System.out.println("### AFTER PAUSINGING DELEGAT");
                });
    }

    @Override
    public SubscriptionModel getDelegatedSubscriptionModel() {
        return delegate;
    }

    @PreDestroy
    @Override
    public synchronized void shutdown() {
        removeAllCompetingConsumers();
        competingConsumerStrategy.removeListener(this);
        delegate.shutdown();
    }

    @Override
    public synchronized void onConsumeGranted(String subscriptionId, String subscriberId) {
        CompetingConsumer competingConsumer = competingConsumers.get(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId));
        if (competingConsumer == null) {
            return;
        }

        if (competingConsumer.isWaiting()) {
            System.out.println("### Starting competing consumer " + subscriberId);
            startWaitingConsumer(competingConsumer);
        } else if (competingConsumer.isPaused()) {
            Paused state = (Paused) competingConsumer.state;
            System.out.println("### Competing consumer " + subscriberId + " + is paused, will start: " + state.hasPermissionToConsume);
            if (state.hasPermissionToConsume) {
                resumeSubscription(subscriptionId);
            }
        }
    }

    @Override
    public synchronized void onConsumeProhibited(String subscriptionId, String subscriberId) {
        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        CompetingConsumer competingConsumer = competingConsumers.get(subscriptionIdAndSubscriberId);
        if (competingConsumer == null) {
            return;
        }

        if (competingConsumer.isRunning()) {
            pauseSubscription(subscriptionId);
            pauseConsumer(competingConsumer, true);
        } else if (competingConsumer.isPaused()) {
            Paused paused = (Paused) competingConsumer.state;
            pauseConsumer(competingConsumer, paused.hasPermissionToConsume);
        }
    }

    private void startWaitingConsumer(CompetingConsumer cc) {
        ((Waiting) cc.state).startSubscription();
        competingConsumers.put(SubscriptionIdAndSubscriberId.from(cc.getSubscriptionId(), cc.getSubscriberId()), cc.registerRunning());
    }

    private void pauseConsumer(CompetingConsumer cc, boolean hasPermissionToConsume) {
        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(cc.getSubscriptionId(), cc.getSubscriberId());
        competingConsumers.put(subscriptionIdAndSubscriberId, cc.registerPaused(hasPermissionToConsume));
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

        private static SubscriptionIdAndSubscriberId from(CompetingConsumer cc) {
            return from(cc.getSubscriptionId(), cc.getSubscriberId());
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


        CompetingConsumer(SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId, CompetingConsumerState state) {
            this.subscriptionIdAndSubscriberId = subscriptionIdAndSubscriberId;
            this.state = state;
        }

        boolean hasId(String subscriptionId, String subscriberId) {
            return hasSubscriptionId(subscriptionId) && Objects.equals(getSubscriberId(), subscriberId);
        }

        boolean hasSubscriptionId(String subscriptionId) {
            return Objects.equals(getSubscriptionId(), subscriptionId);
        }

        boolean isPaused() {
            return state instanceof Paused;
        }

        boolean isRunning() {
            return state instanceof Running;
        }

        boolean isWaiting() {
            return state instanceof Waiting;
        }

        boolean isPausedFor(String subscriptionId) {
            return isPaused() && hasSubscriptionId(subscriptionId);
        }

        String getSubscriptionId() {
            return subscriptionIdAndSubscriberId.subscriptionId;
        }

        String getSubscriberId() {
            return subscriptionIdAndSubscriberId.subscriberId;
        }

        CompetingConsumer registerRunning() {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new Running());
        }

        CompetingConsumer registerPaused() {
            return registerPaused(state.hasPermissionToConsume());
        }

        CompetingConsumer registerPaused(boolean hasPermissionToConsume) {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new Paused(hasPermissionToConsume));
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", CompetingConsumer.class.getSimpleName() + "[", "]")
                    .add("subscriptionIdAndSubscriberId=" + subscriptionIdAndSubscriberId)
                    .add("state=" + state)
                    .toString();
        }
    }

    static abstract class CompetingConsumerState {
        private CompetingConsumerState() {
        }

        abstract boolean hasPermissionToConsume();

        static class Running extends CompetingConsumerState {
            @Override
            boolean hasPermissionToConsume() {
                return true;
            }
        }

        static class Waiting extends CompetingConsumerState {
            private final Supplier<Subscription> supplier;

            Waiting(Supplier<Subscription> supplier) {
                this.supplier = supplier;
            }

            @Override
            boolean hasPermissionToConsume() {
                return false;
            }

            private void startSubscription() {
                supplier.get();
            }
        }

        static class Paused extends CompetingConsumerState {
            private final boolean hasPermissionToConsume;

            Paused(boolean hasPermissionToConsume) {
                this.hasPermissionToConsume = hasPermissionToConsume;
            }

            @Override
            boolean hasPermissionToConsume() {
                return hasPermissionToConsume;
            }
        }
    }

    private void removeAllCompetingConsumers() {
        removeCompetingConsumersMatching(CompetingConsumer::isRunning);
    }

    private void removeCompetingConsumersMatching(Predicate<CompetingConsumer> predicate) {
        competingConsumers.values().stream().filter(predicate).forEach(this::removeCompetingConsumer);
    }

    private synchronized void removeCompetingConsumer(CompetingConsumer cc) {
        System.out.println("### BEFORE UNREGISTER");
        competingConsumers.remove(cc.subscriptionIdAndSubscriberId);
        competingConsumerStrategy.unregisterCompetingConsumer(cc.getSubscriptionId(), cc.getSubscriberId());
        System.out.println("### AFTER UNREGISTER");
    }

    private boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        return competingConsumerStrategy.registerCompetingConsumer(subscriptionId, subscriberId);
    }

    private boolean hasLock(String subscriptionId, String subscriberId) {
        return competingConsumerStrategy.hasLock(subscriptionId, subscriberId);
    }

    private Optional<CompetingConsumer> findFirstCompetingConsumerMatching(Predicate<CompetingConsumer> predicate) {
        return findCompetingConsumersMatching(predicate).findFirst();
    }

    private Stream<CompetingConsumer> findCompetingConsumersMatching(Predicate<CompetingConsumer> predicate) {
        return competingConsumers.values().stream().filter(predicate);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompetingConsumerSubscriptionModel)) return false;
        CompetingConsumerSubscriptionModel that = (CompetingConsumerSubscriptionModel) o;
        return Objects.equals(delegate, that.delegate) && Objects.equals(competingConsumerStrategy, that.competingConsumerStrategy) && Objects.equals(competingConsumers, that.competingConsumers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, competingConsumerStrategy, competingConsumers);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CompetingConsumerSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("delegate=" + delegate)
                .add("competingConsumersStrategy=" + competingConsumerStrategy)
                .add("competingConsumers=" + competingConsumers)
                .toString();
    }
}