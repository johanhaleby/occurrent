package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

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
    private static final Logger log = LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);

    private final SubscriptionModel delegate;
    private final CompetingConsumerStrategy competingConsumerStrategy;

    private final ConcurrentMap<SubscriptionIdAndSubscriberId, CompetingConsumer> competingConsumers = new ConcurrentHashMap<>();
    // A set that hold which subscriptions whose StartAt position have indicated that they should not use the competing consumer model
    private final Set<String> nonCompetingConsumersSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());

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

        final Subscription subscription;
        if (startAt.get(new SubscriptionModelContext(CompetingConsumerSubscriptionModel.class)) == null) {
            nonCompetingConsumersSubscriptions.add(subscriptionId);
            // We're not allowed to start the competing consumer subscription, just delegate to parent.
            // One reason for this might be if we're starting a non-durable in-memory subscription on multiple nodes.
            // Then typically you want all nodes to receive all events, and thus there's no need for a  CompetingConsumerSubscription.
            subscription = getDelegatedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
        } else {
            subscription = startCompetingConsumerSubscription(subscriberId, subscriptionId, filter, startAt, action);
        }

        return subscription;
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
        logDebug("Cancelling CompetingConsumer subscription (subscriptionId={})", subscriptionId);
        delegate.cancelSubscription(subscriptionId);
        findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId))
                .ifPresent(cc -> unregisterCompetingConsumer(cc, __ -> competingConsumers.remove(cc.subscriptionIdAndSubscriberId)));
    }

    /**
     * @see SubscriptionModelLifeCycle#stop()
     */
    @Override
    public synchronized void stop() {
        logDebug("Stopping CompetingConsumer subscription model");
        if (!isRunning()) {
            return;
        }

        delegate.stop();
        unregisterAllCompetingConsumers(cc -> {
            logDebug("Stopped CompetingConsumer subscription (subscriberId={}, subscriptionId={})", cc.getSubscriberId(), cc.getSubscriptionId());
            competingConsumers.put(cc.subscriptionIdAndSubscriberId, cc.registerPaused());
        });
    }

    /**
     * @see SubscriptionModelLifeCycle#start()
     */
    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        logDebug("Starting CompetingConsumer subscription model");
        if (isRunning()) {
            throw new IllegalStateException(CompetingConsumerSubscriptionModel.class.getSimpleName() + " is already started");
        }

        if (!nonCompetingConsumersSubscriptions.isEmpty()) {
            delegate.start(false); // This will automatically start all paused subscriptions (including those in nonCompetingConsumersSubscriptions)
            nonCompetingConsumersSubscriptions.forEach(delegate::resumeSubscription);
        }

        if (!resumeSubscriptionsAutomatically) {
            // Note that we deliberately don't start the delegated subscription model here!!
            // This is because we're not sure that we have the lock. The underlying SM will be started
            // automatically if required (since it's instructed to do so in the Waiting state supplier).
            competingConsumers.values().stream()
                    .filter(not(CompetingConsumer::isRunning))
                    .forEach(cc -> {
                                logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={}, state={})", cc.getSubscriberId(), cc.getSubscriptionId(), cc.state.getClass().getSimpleName());
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
        logDebug("Trying to resume CompetingConsumer subscription (subscriptionId={})", subscriptionId);
        if (isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
        }

        if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
            return delegate.resumeSubscription(subscriptionId);
        }

        return findFirstCompetingConsumerMatching(competingConsumer -> competingConsumer.hasSubscriptionId(subscriptionId))
                .map(competingConsumer -> {
                    final Subscription subscription;
                    String subscriberId = competingConsumer.getSubscriberId();
                    boolean hasLock = hasLock(subscriptionId, subscriberId);
                    logDebug("Resuming CompetingConsumer (subscriberId={}, subscriptionId={}, state={}, hasLock={})", subscriberId, subscriptionId, competingConsumer.state.getClass().getSimpleName(), hasLock);
                    if (hasLock) {
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
        pauseSubscription(subscriptionId, true);
    }

    private CompetingConsumerSubscription startCompetingConsumerSubscription(String subscriberId, String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);

        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        final CompetingConsumerSubscription competingConsumerSubscription;
        if (competingConsumerStrategy.registerCompetingConsumer(subscriptionId, subscriberId)) {
            logDebug("Successfully registered CompetingConsumer subscription (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            Subscription subscription = delegate.subscribe(subscriptionId, filter, startAt, action);
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId, subscription);
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Running()));
        } else {
            logDebug("CompetingConsumer already registered, overriding to Waiting (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Waiting(() -> {
                logDebug("Starting delegated CompetingConsumer subscription after waiting (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                if (!delegate.isRunning()) {
                    delegate.start();
                }
                return delegate.subscribe(subscriptionId, filter, startAt, action);
            })));
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId);
        }
        return competingConsumerSubscription;
    }


    private synchronized void pauseSubscription(String subscriptionId, boolean pausedByUser) {
        logDebug("Trying to pause CompetingConsumer subscription (subscriptionId={}, pausedByUser={})", subscriptionId, pausedByUser);
        if (isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already paused");
        }

        if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
            delegate.pauseSubscription(subscriptionId);
        } else {
            CompetingConsumer competingConsumer = findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId)).orElse(null);
            if (competingConsumer == null) {
                logDebug("Failed to find CompetingConsumer for subscription (subscriptionId={}, pausedByUser={})", subscriptionId, pausedByUser);
            } else if (competingConsumer.isWaiting()) {
                logDebug("CompetingConsumer in waiting state, will ignore (subscriptionId={}, subscriberId={}, pausedByUser={})", subscriptionId, competingConsumer.getSubscriberId(), pausedByUser);
            } else {
                delegate.pauseSubscription(subscriptionId);
                competingConsumers.put(SubscriptionIdAndSubscriberId.from(competingConsumer), competingConsumer.registerPaused(pausedByUser));
                if (pausedByUser) {
                    logDebug("Will unregister competing consumer because subscription was paused explicitly by user (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId());
                    // If subscription is paused by user explicitly, then the user needs to resume it again explicitly to start it.
                    // In these cases, we unregister the competing consumer. This means that it cannot be leader again until the subscription is
                    // resumed explicitly (it'll re-register the competing consumers).
                    competingConsumerStrategy.unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
                } else {
                    logDebug("Will release competing consumer because subscription was paused by system (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId());
                    // Subscription was not paused by the user, thus we just "release" the competing consumer so that it can re-gain leader status
                    // later without explicitly resuming the subscription.
                    competingConsumerStrategy.releaseCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
                }
            }
        }
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
        logDebug("Trying to shutdown CompetingConsumer subscription model");
        delegate.shutdown();
        nonCompetingConsumersSubscriptions.clear();
        unregisterAllCompetingConsumers(cc -> competingConsumers.remove(cc.subscriptionIdAndSubscriberId));
        competingConsumerStrategy.removeListener(this);
        competingConsumerStrategy.shutdown();
    }

    @Override
    public synchronized void onConsumeGranted(String subscriptionId, String subscriberId) {
        logDebug("Consumption granted to CompetingConsumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        CompetingConsumer competingConsumer = competingConsumers.get(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId));
        if (competingConsumer == null) {
            logDebug("Failed to find CompetingConsumer, returning (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return;
        }

        if (competingConsumer.isWaiting()) {
            startWaitingConsumer(competingConsumer);
        } else if (competingConsumer.isPaused()) {
            CompetingConsumerState.Paused state = (CompetingConsumerState.Paused) competingConsumer.state;
            if (state.pausedByUser) {
                logDebug("Won't resume CompetingConsumer, because it was paused by user (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            } else {
                resumeSubscription(subscriptionId);
            }
        }
    }

    @Override
    public synchronized void onConsumeProhibited(String subscriptionId, String subscriberId) {
        logDebug("Consumption prohibited for CompetingConsumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        CompetingConsumer competingConsumer = competingConsumers.get(subscriptionIdAndSubscriberId);
        if (competingConsumer == null) {
            logDebug("CompetingConsumer couldn't be found when calling onConsumeProhibited (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return;
        }

        if (competingConsumer.isRunning()) {
            logDebug("CompetingConsumer is running, will pause subscription and consumers (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            pauseSubscription(subscriptionId, false);
            pauseConsumer(competingConsumer, false);
        } else if (competingConsumer.isPaused()) {
            logDebug("CompetingConsumer is paused (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            CompetingConsumerState.Paused paused = (CompetingConsumerState.Paused) competingConsumer.state;
            pauseConsumer(competingConsumer, paused.pausedByUser);
        } else {
            logDebug("CompetingConsumer is neither running nor paused, won't do anything (subscriberId={}, subscriptionId={}, state={})", subscriberId, subscriptionId, competingConsumer.state.getClass().getSimpleName());
        }
    }

    private Subscription startWaitingConsumer(CompetingConsumer cc) {
        logDebug("Start CompetingConsumer that has previously been waiting (subscriberId={}, subscriptionId={})", cc.getSubscriberId(), cc.getSubscriptionId());
        String subscriptionId = cc.getSubscriptionId();
        competingConsumers.put(SubscriptionIdAndSubscriberId.from(subscriptionId, cc.getSubscriberId()), cc.registerRunning());
        return ((CompetingConsumerState.Waiting) cc.state).startSubscription();
    }

    private void pauseConsumer(CompetingConsumer cc, boolean pausedByUser) {
        logDebug("Pausing CompetingConsumer (subscriberId={}, subscriptionId={}, pausedByUser={})", cc.getSubscriberId(), cc.getSubscriptionId(), pausedByUser);
        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(cc.getSubscriptionId(), cc.getSubscriberId());
        competingConsumers.put(subscriptionIdAndSubscriberId, cc.registerPaused(pausedByUser));
    }

    private record SubscriptionIdAndSubscriberId(String subscriptionId, String subscriberId) {

        private static SubscriptionIdAndSubscriberId from(String subscriptionId, String subscriberId) {
            return new SubscriptionIdAndSubscriberId(subscriptionId, subscriberId);
        }

        private static SubscriptionIdAndSubscriberId from(CompetingConsumer cc) {
            return from(cc.getSubscriptionId(), cc.getSubscriberId());
        }
    }


    private record CompetingConsumer(SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId, CompetingConsumerState state) {

        boolean hasId(String subscriptionId, String subscriberId) {
            return hasSubscriptionId(subscriptionId) && Objects.equals(getSubscriberId(), subscriberId);
        }

        boolean hasSubscriptionId(String subscriptionId) {
            return Objects.equals(getSubscriptionId(), subscriptionId);
        }

        boolean isPaused() {
            return state instanceof CompetingConsumerState.Paused;
        }

        boolean isRunning() {
            return state instanceof CompetingConsumerState.Running;
        }

        boolean isWaiting() {
            return state instanceof CompetingConsumerState.Waiting;
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
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Running());
        }

        CompetingConsumer registerPaused() {
            return registerPaused(state.hasPermissionToConsume());
        }

        CompetingConsumer registerPaused(boolean pausedByUser) {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Paused(pausedByUser));
        }
    }

    private sealed interface CompetingConsumerState {

        boolean hasPermissionToConsume();

        final class Running implements CompetingConsumerState {
            @Override
            public boolean hasPermissionToConsume() {
                return true;
            }
        }

        final class Waiting implements CompetingConsumerState {
            private final Supplier<Subscription> supplier;

            Waiting(Supplier<Subscription> supplier) {
                this.supplier = supplier;
            }

            @Override
            public boolean hasPermissionToConsume() {
                return false;
            }

            private Subscription startSubscription() {
                return supplier.get();
            }
        }

        final class Paused implements CompetingConsumerState {
            private final boolean pausedByUser;

            Paused(boolean pausedByUser) {
                this.pausedByUser = pausedByUser;
            }

            @Override
            public boolean hasPermissionToConsume() {
                return pausedByUser;
            }
        }
    }

    private void unregisterAllCompetingConsumers(Consumer<CompetingConsumer> andDo) {
        logDebug("Unregistering all CompetingConsumer's");
        unregisterCompetingConsumersMatching(CompetingConsumer::isRunning, andDo);
    }

    private void unregisterCompetingConsumersMatching(Predicate<CompetingConsumer> predicate, Consumer<CompetingConsumer> and) {
        competingConsumers.values().stream().filter(predicate).forEach(cc -> unregisterCompetingConsumer(cc, and));
    }

    private synchronized void unregisterCompetingConsumer(CompetingConsumer cc, Consumer<CompetingConsumer> and) {
        logDebug("Unregistering CompetingConsumer (subscriberId={}, subscriptionId={})", cc.getSubscriberId(), cc.getSubscriptionId());
        and.accept(cc);
        competingConsumerStrategy.unregisterCompetingConsumer(cc.getSubscriptionId(), cc.getSubscriberId());
    }

    private boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        logDebug("Registering CompetingConsumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
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

    private static void logDebug(String message, Object... params) {
        if (log.isDebugEnabled()) {
            log.debug(message, params);
        }
    }
}