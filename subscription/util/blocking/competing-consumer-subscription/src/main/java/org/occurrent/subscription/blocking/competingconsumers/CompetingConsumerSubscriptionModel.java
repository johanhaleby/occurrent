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

/**
 * A competing consumer subscription model wraps another subscription model to allow several subscribers to subscribe to the same subscription. One of the subscribes will get a lock of the subscription
 * and receive events from it. If a subscriber looses its lock, another subscriber will take over automatically. To achieve distributed locking, the subscription model uses a {@link CompetingConsumerStrategy} to
 * support different algorithms. You can write custom algorithms by implementing this interface yourself. Here's an example of how to create and use the {@link CompetingConsumerSubscriptionModel}. This example
 * uses the {@code NativeMongoLeaseCompetingConsumerStrategy} from module {@code org.occurrent:subscription-mongodb-native-blocking-competing-consumer-strategy}.
 * It also wraps the <a href="https://occurrent.org/documentation#durable-subscriptions-blocking">DurableSubscriptionModel</a> which in turn wraps the
 * <a href="https://occurrent.org/documentation#blocking-subscription-using-the-native-java-mongodb-driver">Native MongoDB</a> subscription model.
 * <br>
 * <br>
 * <pre>
 * MongoDatabase mongoDatabase = mongoClient.getDatabase("some-database");
 * SubscriptionPositionStorage positionStorage = NativeMongoSubscriptionPositionStorage(mongoDatabase, "position-storage");
 * SubscriptionModel wrappedSubscriptionModel = new DurableSubscriptionModel(new NativeMongoSubscriptionModel(mongoDatabase, "events", TimeRepresentation.DATE), positionStorage);
 *
 * // Create the CompetingConsumerSubscriptionModel
 * NativeMongoLeaseCompetingConsumerStrategy competingConsumerStrategy = NativeMongoLeaseCompetingConsumerStrategy.withDefaults(mongoDatabase);
 * CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel = new CompetingConsumerSubscriptionModel(wrappedSubscriptionModel, competingConsumerStrategy);
 *
 * // Now subscribe!
 * competingConsumerSubscriptionModel.subscribe("subscriptionId", type("SomeEvent"));
 * </pre>
 * <p>
 * If the above code is executed on multiple nodes/processes, then only <i>one</i> subscriber will receive events.
 */
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

    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *
     * @param subscriberId   The unique if of the subscriber
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
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
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, new Waiting(() -> {
                if (!delegate.isRunning()) {
                    delegate.start();
                }
                return delegate.subscribe(subscriptionId, filter, startAt, action);
            })));
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId);
        }
        return competingConsumerSubscription;
    }

    /**
     * @see SubscriptionModel#subscribe(String, SubscriptionFilter, StartAt, Consumer)
     */
    @Override
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(UUID.randomUUID().toString(), subscriptionId, filter, startAt, action);
    }

    /**
     * @see SubscriptionModelLifeCycle#cancelSubscription(String)
     */
    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        delegate.cancelSubscription(subscriptionId);
        findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId))
                .ifPresent(cc -> unregisterCompetingConsumer(cc, __ -> competingConsumers.remove(cc.subscriptionIdAndSubscriberId)));
    }

    /**
     * @see SubscriptionModelLifeCycle#stop()
     */
    @Override
    public synchronized void stop() {
        if (!isRunning()) {
            return;
        }

        delegate.stop();
        unregisterAllCompetingConsumers(cc -> competingConsumers.put(cc.subscriptionIdAndSubscriberId, cc.registerPaused()));
    }

    /**
     * @see SubscriptionModelLifeCycle#start()
     */
    @Override
    public synchronized void start() {
        if (isRunning()) {
            throw new IllegalStateException(CompetingConsumerSubscriptionModel.class.getSimpleName() + " is already started");
        }


        // Note that we deliberately don't start the delegate subscription model here!!
        // This is because we're not sure that we have the lock. It'll the underlying SM be started
        // automatically if required (since it's instructed to do so in the Waiting state supplier).
        competingConsumers.values().stream()
                .filter(not(CompetingConsumer::isRunning))
                .forEach(cc -> {
                            // Only change state if we have permission to consume
                            if (cc.isWaiting()) {
                                // Registering a competing consumer will start the subscription automatically if lock was acquired
                                competingConsumerStrategy.registerCompetingConsumer(cc.getSubscriptionId(), cc.getSubscriberId());
                            } else if (cc.isPaused()) {
                                resumeSubscription(cc.getSubscriptionId());
                            }
                        }
                );
    }

    /**
     * @see SubscriptionModelLifeCycle#isRunning()
     */
    @Override
    public boolean isRunning() {
        return delegate.isRunning();
    }

    /**
     * @see SubscriptionModelLifeCycle#isRunning(String)
     */
    @Override
    public boolean isRunning(String subscriptionId) {
        return delegate.isRunning(subscriptionId);
    }

    /**
     * @see SubscriptionModelLifeCycle#isPaused(String)
     */
    @Override
    public boolean isPaused(String subscriptionId) {
        return delegate.isPaused(subscriptionId);
    }

    /**
     * @see SubscriptionModelLifeCycle#resumeSubscription(String)
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        if (isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
        }
        return findFirstCompetingConsumerMatching(competingConsumer -> competingConsumer.hasSubscriptionId(subscriptionId))
                .map(competingConsumer -> {
                    final Subscription subscription;
                    String subscriberId = competingConsumer.getSubscriberId();
                    if (hasLock(subscriptionId, subscriberId)) {
                        if (competingConsumer.isWaiting()) {
                            subscription = startWaitingConsumer(competingConsumer);
                        } else {
                            competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerRunning());
                            // This works because we've checked that it's already paused earlier
                            subscription = delegate.resumeSubscription(subscriptionId);
                        }
                    } else if (registerCompetingConsumer(subscriptionId, subscriberId) && !competingConsumer.isWaiting()) {
                        // This works because we've checked that it's already paused earlier
                        competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerRunning());
                        subscription = delegate.resumeSubscription(subscriptionId);
                    } else {
                        // We're not allowed to resume since we don't have the lock.
                        subscription = new CompetingConsumerSubscription(subscriptionId, subscriberId);
                    }
                    return subscription;
                })
                .orElseThrow(() -> new IllegalStateException("Cannot resume subscription " + subscriptionId + " since another consumer currently subscribes to it."));
    }

    /**
     * @see SubscriptionModelLifeCycle#pauseSubscription(String)
     */
    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        if (isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already paused");
        }
        findFirstCompetingConsumerMatching(competingConsumer -> competingConsumer.hasSubscriptionId(subscriptionId))
                .filter(not(CompetingConsumer::isWaiting))
                .ifPresent(competingConsumer -> {
                    delegate.pauseSubscription(subscriptionId);
                    competingConsumers.put(SubscriptionIdAndSubscriberId.from(competingConsumer), competingConsumer.registerPaused(true));
                    competingConsumerStrategy.unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
                });
    }

    /**
     * @see DelegatingSubscriptionModel#getDelegatedSubscriptionModel()
     */
    @Override
    public SubscriptionModel getDelegatedSubscriptionModel() {
        return delegate;
    }

    /**
     * @see SubscriptionModelLifeCycle#shutdown()
     */
    @PreDestroy
    @Override
    public synchronized void shutdown() {
        delegate.shutdown();
        unregisterAllCompetingConsumers(cc -> competingConsumers.remove(cc.subscriptionIdAndSubscriberId));
        competingConsumerStrategy.removeListener(this);
        competingConsumerStrategy.shutdown();
    }

    @Override
    public synchronized void onConsumeGranted(String subscriptionId, String subscriberId) {
        CompetingConsumer competingConsumer = competingConsumers.get(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId));
        if (competingConsumer == null) {
            return;
        }

        if (competingConsumer.isWaiting()) {
            startWaitingConsumer(competingConsumer);
        } else if (competingConsumer.isPaused()) {
            Paused state = (Paused) competingConsumer.state;
            if (!state.pausedByUser) {
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
            pauseConsumer(competingConsumer, false);
        } else if (competingConsumer.isPaused()) {
            Paused paused = (Paused) competingConsumer.state;
            pauseConsumer(competingConsumer, paused.pausedByUser);
        }
    }

    private Subscription startWaitingConsumer(CompetingConsumer cc) {
        String subscriptionId = cc.getSubscriptionId();
        competingConsumers.put(SubscriptionIdAndSubscriberId.from(subscriptionId, cc.getSubscriberId()), cc.registerRunning());
        return ((Waiting) cc.state).startSubscription();
    }

    private void pauseConsumer(CompetingConsumer cc, boolean pausedByUser) {
        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(cc.getSubscriptionId(), cc.getSubscriberId());
        competingConsumers.put(subscriptionIdAndSubscriberId, cc.registerPaused(pausedByUser));
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

        CompetingConsumer registerPaused(boolean pausedByUser) {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new Paused(pausedByUser));
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

            private Subscription startSubscription() {
                return supplier.get();
            }
        }

        static class Paused extends CompetingConsumerState {
            private final boolean pausedByUser;

            Paused(boolean pausedByUser) {
                this.pausedByUser = pausedByUser;
            }

            @Override
            boolean hasPermissionToConsume() {
                return pausedByUser;
            }
        }
    }

    private void unregisterAllCompetingConsumers(Consumer<CompetingConsumer> andDo) {
        unregisterCompetingConsumersMatching(CompetingConsumer::isRunning, andDo);
    }

    private void unregisterCompetingConsumersMatching(Predicate<CompetingConsumer> predicate, Consumer<CompetingConsumer> and) {
        competingConsumers.values().stream().filter(predicate).forEach(cc -> unregisterCompetingConsumer(cc, and));
    }

    private synchronized void unregisterCompetingConsumer(CompetingConsumer cc, Consumer<CompetingConsumer> and) {
        and.accept(cc);
        competingConsumerStrategy.unregisterCompetingConsumer(cc.getSubscriptionId(), cc.getSubscriberId());
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