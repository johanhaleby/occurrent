package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.SubscriptionRefusedException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

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
 * CheckpointStorage positionStorage = NativeMongoCheckpointStorage(mongoDatabase, "position-storage");
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
 * <br>
 * <br>
 * That is also the scope of the usual "a subscription id identifies one subscription" rule here: it holds within one
 * {@link CompetingConsumerSubscriptionModel} instance, which refuses a subscription id it already has, and says nothing
 * about the other instances, which are meant to use the very same id.
 * <br>
 * <br>
 * {@link #pauseSubscription(String)} works on a node that has not won the lock, not only on the one delivering events.
 * Pausing such a node records the pause and stops it from competing until it is explicitly resumed, so cluster-wide
 * pause is calling {@link #pauseSubscription(String)} on every node
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0112-a-competing-consumer-can-be-paused-while-still-waiting-for-the-lock.md">ADR 112</a>).
 * <br>
 * <br>
 * After {@link #stop()}, and until the next {@link #start(boolean)}, this node holds no lease and delivers nothing for a
 * competing subscription, also one made while it is stopped. {@code stop()} pauses a subscription in the wrapped model
 * when that model still runs it, which it can after throwing from its own {@code stop()}. A subscription the wrapped
 * model cannot pause keeps its lease and stays running, since it still delivers, and {@code stop()} throws. The other
 * exception is a subscription the user resumes with {@link #resumeSubscription(String)}. It competes for its lease and
 * runs once this node wins it, whether that happens straight away or on a later grant.
 * <br>
 * <br>
 * A competing subscription made while this model is stopped goes to the wrapped model straight away, through
 * {@link SubscriptionModel#subscribePaused}, which holds it paused whether or not the wrapped model runs. It starts from
 * the position the wrapped model gives a subscription made while that model is stopped. For a
 * {@code DurableSubscriptionModel} with no stored position, that is the position when {@code subscribe(..)} was called,
 * so an event written before {@link #start(boolean)} is delivered. A running wrapped model that refuses
 * {@code subscribePaused} with {@link UnsupportedOperationException}, as its default implementation does, gets the
 * subscription only once this node wins the lease, the same as a subscription that loses the lease in
 * {@code subscribe(..)}, so it starts where that model starts a subscription at that moment. The subscription competes
 * for its lease once this model is started, with or without resuming subscriptions automatically, and winning the
 * lease resumes it, or subscribes it in the wrapped model when that model does not have it.
 * <br>
 * <br>
 * The wrapped model is started, without resuming what it holds paused, before a subscription whose lease this node
 * holds is subscribed or resumed there, since a stopped model holds a new subscription paused.
 */
@NullMarked
public class CompetingConsumerSubscriptionModel implements SubscriptionModelWrapper, SubscriptionModel, SubscriptionModelLifeCycle, IntrospectableSubscriptions, CompetingConsumerListener {
    private static final Logger log = LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);

    private final SubscriptionModel delegate;
    private final CompetingConsumerStrategy competingConsumerStrategy;

    private final AtomicBoolean stoppedByUser = new AtomicBoolean(false);
    // Consumers the user resumed since stop(), which compete and run although this model is stopped
    private final Set<SubscriptionIdAndSubscriberId> resumedByTheUserWhileStopped = ConcurrentHashMap.newKeySet();

    private final ConcurrentMap<SubscriptionIdAndSubscriberId, CompetingConsumer> competingConsumers = new ConcurrentHashMap<>();
    // Subscriptions whose StartAt position indicated they should not use the competing consumer model
    private final Set<String> nonCompetingConsumersSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public CompetingConsumerSubscriptionModel(SubscriptionModel subscriptionModel, CompetingConsumerStrategy strategy) {
        requireNonNull(subscriptionModel, "Subscription model cannot be null");
        requireNonNull(strategy, CompetingConsumerStrategy.class.getSimpleName() + " cannot be null");
        this.delegate = subscriptionModel;
        this.competingConsumerStrategy = strategy;
        this.competingConsumerStrategy.addListener(this);
    }

    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *
     * @param subscriberId   The unique if of the subscriber
     * @param subscriptionId The id of the subscription, must be unique in this subscription model instance! Other
     *                       instances are expected to use the same subscription id, since that is what makes them
     *                       compete for it.
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     * @throws DuplicateSubscriptionIdException If this subscription model instance already has a subscription with this id.
     */
    // Holds the monitor while the wrapped model subscribes, as a grant does. Checking whether this model is stopped,
    // winning the lease and subscribing are one step, or a stop() in between would find nothing to stop, and the
    // subscription would run with its lease after stop() returned.
    public synchronized Subscription subscribe(String subscriberId, String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriberId, "SubscriberId cannot be null");
        Objects.requireNonNull(subscriptionId, "SubscriptionId cannot be null");
        if (isSubscriptionIdInUse(subscriptionId)) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }

        final Subscription subscription;
        if (startAt.get(new SubscriptionModelContext(CompetingConsumerSubscriptionModel.class)) == null) {
            // Not allowed to start the competing consumer subscription, delegate to parent instead. One case: a
            // non-durable in-memory subscription started on multiple nodes, where every node should receive every
            // event, so competing consumption is not wanted.
            subscription = getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
            // Recorded only once the delegate has accepted it. Recording first would leave the id occupied by a
            // subscription that was refused, and the check above would then refuse it for good.
            nonCompetingConsumersSubscriptions.add(subscriptionId);
        } else {
            subscription = startCompetingConsumerSubscription(subscriberId, subscriptionId, filter, startAt, action);
        }

        return subscription;
    }

    /**
     * @see SubscriptionModel#subscribe(String, SubscriptionFilter, StartAt, Consumer)
     */
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(UUID.randomUUID().toString(), subscriptionId, filter, startAt, action);
    }

    /**
     * @see SubscriptionModelLifeCycle#cancelSubscription(String)
     */
    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        logDebug("Cancelling CompetingConsumer subscription (subscriptionId={})", subscriptionId);
        delegate.cancelSubscription(subscriptionId);
        // Forgotten here too, not only in the competing consumer map, so the id is free for a new subscription
        // afterwards. Remembering a cancelled one also made start() resume a subscription the delegate no longer has.
        nonCompetingConsumersSubscriptions.remove(subscriptionId);
        findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId))
                .ifPresent(cc -> unregisterCompetingConsumer(cc, __ -> {
                    competingConsumers.remove(cc.subscriptionIdAndSubscriberId);
                    resumedByTheUserWhileStopped.remove(cc.subscriptionIdAndSubscriberId);
                }));
    }

    /**
     * @see SubscriptionModelLifeCycle#stop()
     */
    @Override
    public synchronized void stop() {
        logDebug("Stopping CompetingConsumer subscription model");
        // Whether the wrapped model runs says nothing about whether this model has anything left to stop. After a
        // start() that won no lease, or failed part way, the wrapped model can still be stopped while consumers are
        // registered, and a grant would resume one after a stop() that had returned without doing anything.
        stoppedByUser.set(true);
        resumedByTheUserWhileStopped.clear();
        try {
            if (delegate.isRunning()) {
                delegate.stop();
            }
        } catch (Throwable e) {
            // The consumers are stopped also when stopping the wrapped model throws, or this node would keep leases
            // for subscriptions it no longer means to serve
            RuntimeException consumerFailure = stopEveryConsumer();
            if (consumerFailure != null) {
                e.addSuppressed(consumerFailure);
            }
            throw e;
        }
        RuntimeException consumerFailure = stopEveryConsumer();
        if (consumerFailure != null) {
            throw consumerFailure;
        }
    }

    /**
     * Pauses every running consumer and unregisters every consumer, so none of them competes for the lock. A consumer
     * left registered keeps competing through the strategy's refresh thread, so a stopped model can take a lock it then
     * refuses to act on, and start() sees no status change and never starts it. A waiting one stays waiting, so
     * start() registers it again.
     * <p>
     * A running consumer that the wrapped model still runs is paused there first, since a wrapped model that threw from
     * stop(), or one that keeps its subscriptions running across a stop, still delivers them. One that cannot be paused
     * there keeps its lease and stays recorded as running, since it still delivers, and the failure is returned with
     * any later one attached as suppressed.
     */
    private @Nullable RuntimeException stopEveryConsumer() {
        @Nullable RuntimeException firstFailure = null;
        for (CompetingConsumer cc : List.copyOf(competingConsumers.values())) {
            String subscriptionId = cc.getSubscriptionId();
            try {
                if (cc.isRunning() && delegate.isRunning(subscriptionId)) {
                    delegate.pauseSubscription(subscriptionId);
                    if (delegate.isRunning(subscriptionId)) {
                        throw new IllegalStateException("Subscription " + subscriptionId + " still runs in the wrapped subscription model after it was paused there, so this node keeps its lease");
                    }
                }
                unregisterCompetingConsumer(cc, c -> {
                    logDebug("Stopped CompetingConsumer subscription (subscriberId={}, subscriptionId={})", c.getSubscriberId(), c.getSubscriptionId());
                    if (c.isRunning()) {
                        competingConsumers.put(c.subscriptionIdAndSubscriberId, c.registerPaused(true));
                    }
                });
            } catch (RuntimeException e) {
                firstFailure = withSuppressed(firstFailure, e);
            }
        }
        return firstFailure;
    }

    /**
     * @see SubscriptionModelLifeCycle#start()
     */
    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        logDebug("Starting CompetingConsumer subscription model");
        stoppedByUser.set(false);
        resumedByTheUserWhileStopped.clear();
        // A subscription the wrapped model fails to start, competing or not, must not keep the subscriptions after it
        // from starting, so every failure is thrown once every subscription has had its turn. A competing consumer
        // that fails has already given its lease back by then.
        @Nullable RuntimeException firstFailure = null;
        if (!nonCompetingConsumersSubscriptions.isEmpty()) {
            try {
                delegate.start(false);
            } catch (RuntimeException e) {
                firstFailure = e;
            }
            // Only the paused ones. Starting a model that is already started arrives here too, and the delegate
            // refuses to resume a subscription that is already running.
            for (String subscriptionId : nonCompetingConsumersSubscriptions) {
                try {
                    if (delegate.isPaused(subscriptionId)) {
                        delegate.resumeSubscription(subscriptionId);
                    }
                } catch (RuntimeException e) {
                    firstFailure = withSuppressed(firstFailure, e);
                }
            }
        }

        // Deliberately not starting the wrapped model here, since no lease is known to be held. A consumer starts it
        // once this node holds its lease, before subscribing or resuming there.
        for (CompetingConsumer cc : competingConsumers.values()) {
            if (cc.isRunning()) {
                continue;
            }
            try {
                // A waiting consumer competes again whatever the flag says, since nothing paused it. That includes one
                // made while this model was stopped. A paused one is resumed only when asked to.
                if (cc.isWaiting()) {
                    logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={}, state={})", cc.getSubscriberId(), cc.getSubscriptionId(), cc.state.getClass().getSimpleName());
                    registerAndStartIfGranted(cc);
                } else if (resumeSubscriptionsAutomatically && cc.isPaused()) {
                    logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={}, state={})", cc.getSubscriberId(), cc.getSubscriptionId(), cc.state.getClass().getSimpleName());
                    resumeSubscription(cc.getSubscriptionId());
                }
            } catch (RuntimeException e) {
                firstFailure = withSuppressed(firstFailure, e);
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    // The first failure, with every later one attached to it as suppressed
    private static RuntimeException withSuppressed(@Nullable RuntimeException firstFailure, RuntimeException failure) {
        if (firstFailure == null) {
            return failure;
        }
        firstFailure.addSuppressed(failure);
        return firstFailure;
    }

    /**
     * @see SubscriptionModelLifeCycle#isRunning()
     */
    @Override
    public boolean isRunning() {
        return getWrappedSubscriptionModel().isRunning();
    }

    /**
     * @see SubscriptionModelLifeCycle#isRunning(String)
     */
    @Override
    public boolean isRunning(String subscriptionId) {
        return getWrappedSubscriptionModel().isRunning(subscriptionId);
    }

    // Reports its own consumers as well as the delegate's, because the delegate knows a consumer that has not won the
    // lock yet only when it was made while this model was stopped.
    @Override
    public Set<String> subscriptionIds() {
        Set<String> ids = competingConsumers.keySet().stream()
                .map(SubscriptionIdAndSubscriberId::subscriptionId)
                .collect(Collectors.toCollection(HashSet::new));
        IntrospectableSubscriptions.findIn(getWrappedSubscriptionModel())
                .map(IntrospectableSubscriptions::subscriptionIds)
                .ifPresent(ids::addAll);
        return Set.copyOf(ids);
    }

    /**
     * @see SubscriptionModelLifeCycle#isPaused(String)
     */
    @Override
    public boolean isPaused(String subscriptionId) {
        // A consumer paused before it ever won the lock is known only here, unless it was made while this model was
        // stopped. subscriptionIds() merges both sources for the same reason.
        boolean pausedHere = findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId) && cc.isPaused()).isPresent();
        return pausedHere || delegate.isPaused(subscriptionId);
    }

    /**
     * @see SubscriptionModelLifeCycle#resumeSubscription(String)
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        logDebug("Trying to resume CompetingConsumer subscription (subscriptionId={})", subscriptionId);
        requireKnown(subscriptionId);
        if (isRunning(subscriptionId)) {
            logDebug("Subscription already is running, cannot resume (subscriptionId={}, delegate={})", subscriptionId, delegate.toString());
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }

        if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
            logDebug("Subscription was a non-competing consumer subscription, will delegate to {} (subscriptionId={})", delegate.getClass().getName(), subscriptionId);
            return delegate.resumeSubscription(subscriptionId);
        }

        logDebug("Finding first competing consumer that matches the subscription (subscriptionId={})", subscriptionId);
        return findFirstCompetingConsumerMatching(competingConsumer -> competingConsumer.hasSubscriptionId(subscriptionId))
                .map(competingConsumer -> {
                    if (stoppedByUser.get()) {
                        resumedByTheUserWhileStopped.add(competingConsumer.subscriptionIdAndSubscriberId);
                    }
                    if (competingConsumer.isPausedWhileWaiting()) {
                        // Restore the Waiting this consumer was paused from before anything else runs, so every
                        // branch below treats it exactly like a plain resume of a waiting consumer. The write has
                        // to land before any strategy call, because registering can grant the lock and call
                        // onConsumeGranted synchronously on this thread, and that callback only starts a consumer
                        // it finds Waiting in the map.
                        competingConsumer = competingConsumer.restoreWaiting();
                        competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer);
                    }
                    final Subscription subscription;
                    String subscriberId = competingConsumer.getSubscriberId();
                    boolean hasLock = hasLock(subscriptionId, subscriberId);
                    logDebug("Resuming CompetingConsumer (subscriberId={}, subscriptionId={}, state={}, hasLock={})", subscriberId, subscriptionId, competingConsumer.state.getClass().getSimpleName(), hasLock);
                    CompetingConsumer paused = competingConsumer;
                    if (hasLock) {
                        if (competingConsumer.isWaiting()) {
                            subscription = startWaitingConsumer(competingConsumer);
                        } else {
                            competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerRunning());
                            // Paused here, or recorded as running here although the wrapped model does not run it,
                            // since the isRunning check above asks the wrapped model
                            subscription = giveTheLeaseBackIfItThrows(paused.subscriptionIdAndSubscriberId, paused.state, () -> resumeInTheWrappedModel(subscriptionId));
                        }
                    } else if (competingConsumer.isWaiting()) {
                        subscription = registerAndStartIfGranted(competingConsumer);
                    } else if (registerAsRunning(competingConsumer)) {
                        // Paused here, or recorded as running here although the wrapped model does not run it
                        subscription = giveTheLeaseBackIfItThrows(paused.subscriptionIdAndSubscriberId, paused.state, () -> resumeInTheWrappedModel(subscriptionId));
                    } else {
                        // Not allowed to resume without the lock
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

    /**
     * Starts the wrapped model, without resuming what it holds paused, before a consumer whose lease this node holds is
     * subscribed or resumed there. A stopped model holds a new subscription paused, so a consumer subscribed there
     * without it would be recorded as running here and delivered nothing. Most subscriptions paused there are ones this
     * node holds no lease for, or ones that resume on a grant of their own, so none of them is resumed.
     */
    private void startTheWrappedModelIfStopped() {
        if (!delegate.isRunning()) {
            delegate.start(false);
        }
    }

    private Subscription resumeInTheWrappedModel(String subscriptionId) {
        startTheWrappedModelIfStopped();
        return delegate.resumeSubscription(subscriptionId);
    }

    private CompetingConsumerSubscription startCompetingConsumerSubscription(String subscriberId, String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);

        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        Supplier<Subscription> subscribeInTheWrappedModel = () -> {
            startTheWrappedModelIfStopped();
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        };
        CompetingConsumerState.Waiting waiting = new CompetingConsumerState.Waiting(() -> {
            logDebug("Starting delegated CompetingConsumer subscription after waiting (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return delegate.isPaused(subscriptionId) ? resumeInTheWrappedModel(subscriptionId) : subscribeInTheWrappedModel.get();
        });
        final CompetingConsumerSubscription competingConsumerSubscription;
        if (stoppedByUser.get()) {
            logDebug("Subscription model is stopped, recording CompetingConsumer as waiting without competing for the lease (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            // A stopped node competes for nothing, since a lease it won would lock every other node out of a
            // subscription it does not serve. The next start() makes it compete, as does a resume, and winning the
            // lease resumes it in the wrapped model.
            // Held paused in the wrapped model also when a resume since stop() has started it again. One that decides
            // the start position at subscribe, such as DurableSubscriptionModel, then delivers an event written before
            // this node wins the lease.
            @Nullable Subscription subscription;
            try {
                subscription = delegate.subscribePaused(subscriptionId, filter, startAt, action);
            } catch (UnsupportedOperationException e) {
                // A running wrapped model that cannot hold it paused gets it once this node wins the lease, as a
                // subscription that loses the lease in subscribe does, since subscribing and pausing it there could
                // deliver an event without the lease
                logDebug("Wrapped model cannot hold the subscription paused while it runs, subscribing it there once the lease is won (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                subscription = null;
            }
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, waiting));
            competingConsumerSubscription = subscription == null
                    ? new CompetingConsumerSubscription(subscriptionId, subscriberId)
                    : new CompetingConsumerSubscription(subscriptionId, subscriberId, subscription);
        } else if (competingConsumerStrategy.registerCompetingConsumer(subscriptionId, subscriberId)) {
            logDebug("Successfully registered CompetingConsumer subscription (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            Subscription subscription = giveTheLeaseBackIfItThrows(subscriptionIdAndSubscriberId, null, subscribeInTheWrappedModel);
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId, subscription);
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Running()));
        } else {
            logDebug("CompetingConsumer already registered, overriding to Waiting (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            competingConsumers.put(subscriptionIdAndSubscriberId, new CompetingConsumer(subscriptionIdAndSubscriberId, waiting));
            competingConsumerSubscription = new CompetingConsumerSubscription(subscriptionId, subscriberId);
        }
        return competingConsumerSubscription;
    }


    private synchronized void pauseSubscription(String subscriptionId, boolean pausedByUser) {
        logDebug("Trying to pause CompetingConsumer subscription (subscriptionId={}, pausedByUser={})", subscriptionId, pausedByUser);
        requireKnown(subscriptionId);
        CompetingConsumer competingConsumer = findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId)).orElse(null);
        if (pausedByUser && competingConsumer != null && competingConsumer.state instanceof CompetingConsumerState.Paused paused && !paused.pausedByUser) {
            logDebug("CompetingConsumer paused by the system, recording it as paused by user and unregistering it from the strategy (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId());
            // A consumer resumed without winning the lease waits for its grant, and the grant resumes it unless the
            // user paused it. Recorded first, so a grant out of the unregister below finds it paused by the user.
            competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerPaused(true));
            competingConsumerStrategy.unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
            return;
        }
        // The state recorded here, not the delegate's, since a consumer that won its lease before the wrapped model
        // was started is running here and paused there, and pausing it still has to give up the lease
        if (competingConsumer == null ? isPaused(subscriptionId) : competingConsumer.isPaused()) {
            throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
        }

        if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
            delegate.pauseSubscription(subscriptionId);
        } else {
            if (competingConsumer == null) {
                logDebug("Failed to find CompetingConsumer for subscription (subscriptionId={}, pausedByUser={})", subscriptionId, pausedByUser);
                // The delegate refuses this as well, but never sees it: an id with no competing consumer here stops at
                // this branch, so returning quietly was the wrapper answering for the delegate, and answering wrongly.
                throw new SubscriptionNotRunningException(subscriptionId);
            } else if (competingConsumer.isWaiting()) {
                logDebug("CompetingConsumer in waiting state, pausing and unregistering from the strategy so the lock passes to another consumer (subscriptionId={}, subscriberId={}, pausedByUser={})", subscriptionId, competingConsumer.getSubscriberId(), pausedByUser);
                // Only pausedByUser=true reaches a waiting consumer here. The other caller, onConsumeProhibited,
                // only pauses a consumer it already found running, and a waiting one never is. Recorded first, so
                // a synchronous onConsumeProhibited out of the unregister below finds this consumer already
                // paused rather than still waiting.
                competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerPausedWhileWaiting());
                // Staying registered would mean competing for the lock while paused. The strategy's own refresh
                // re-registers every consumer that lacks the lock, so a registered-but-paused consumer would win
                // it back and sit on it, and every other node would stay locked out until this one is resumed.
                competingConsumerStrategy.unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
            } else {
                try {
                    if (!delegate.isPaused(subscriptionId)) {
                        delegate.pauseSubscription(subscriptionId);
                    }
                } catch (SubscriptionRefusedException e) {
                    // The delegate no longer knows this id, most likely a catch-up subscription whose replay had
                    // already failed before this call. That leaves nothing to pause downstream, but the lease still
                    // needs releasing below, otherwise this node reports itself Running while holding a lease no
                    // delegate is actually serving.
                    logDebug("Delegate refused to pause subscription, continuing to release the lease (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId(), e);
                }
                pauseConsumer(competingConsumer, pausedByUser);
                if (pausedByUser) {
                    logDebug("Will unregister competing consumer because subscription was paused explicitly by user (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId());
                    // A user-paused subscription needs an explicit resume to restart, so unregister the competing
                    // consumer: it cannot become leader again until the subscription is explicitly resumed.
                    competingConsumerStrategy.unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
                } else {
                    logDebug("Will release competing consumer because subscription was paused by system (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId());
                    // Not paused by the user, so just release the competing consumer so it can re-gain leader
                    // status later without an explicit resume.
                    competingConsumerStrategy.releaseCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
                }
            }
        }
    }

    /**
     * @see SubscriptionModelWrapper#getWrappedSubscriptionModel()
     */
    @Override
    public SubscriptionModel getWrappedSubscriptionModel() {
        return delegate;
    }

    /**
     * This model resolves the start position to find out whether to compete for the subscription. The model it wraps
     * receives the caller's own {@link StartAt} either way and resolves it under its own class, so where the
     * subscription starts is settled there rather than here.
     *
     * @return {@code false}
     * @see SubscriptionModelWrapper#decidesWhereTheSubscriptionStarts()
     */
    @Override
    public boolean decidesWhereTheSubscriptionStarts() {
        return false;
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
        // The strategy may have decided the grant before this callback got the monitor, and a stop(), a pause or a
        // refresh may have given the lease up since. Acting on it would start a subscription without its lease.
        if (!hasLock(subscriptionId, subscriberId)) {
            logDebug("CompetingConsumer no longer holds the lease it was granted, ignoring the grant (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return;
        }

        // A subscription the user resumed since stop() runs once it wins the lease, on a later grant as much as on the
        // resume itself. Nothing else runs while this model is stopped.
        boolean mayRun = !stoppedByUser.get() || resumedByTheUserWhileStopped.contains(competingConsumer.subscriptionIdAndSubscriberId);
        switch (competingConsumer.state) {
            case CompetingConsumerState.Waiting waiting -> {
                if (mayRun) {
                    startWaitingConsumer(competingConsumer);
                } else {
                    logDebug("Won't start waiting consumer because subscription model was explicitly stopped by user (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                    handBackGrantedLock(competingConsumer);
                }
            }
            case CompetingConsumerState.Paused paused -> {
                if (paused.pausedByUser) {
                    logDebug("Won't resume CompetingConsumer, because it was paused by user (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                    handBackGrantedLock(competingConsumer);
                } else if (!mayRun) {
                    logDebug("Won't resume system-paused CompetingConsumer because subscription model was explicitly stopped by user (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                    handBackGrantedLock(competingConsumer);
                } else {
                    resumeSubscription(subscriptionId);
                }
            }
            case CompetingConsumerState.PausedWhileWaiting pausedWhileWaiting -> {
                logDebug("Won't start CompetingConsumer, because it was paused while waiting for the lock (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                handBackGrantedLock(competingConsumer);
            }
            case CompetingConsumerState.Running running -> {
                // Grant callbacks only fire on a change of status, so a consumer already running should not
                // reach here. If it somehow does, there is nothing to do since it already has what this
                // callback would give it.
            }
        }
    }

    /**
     * Unregisters a consumer the strategy just granted the lock to but that the model will not let consume right
     * now, so the lock passes on rather than being held by a consumer that will never act on it.
     */
    private void handBackGrantedLock(CompetingConsumer cc) {
        logDebug("Handing the granted lock back because CompetingConsumer is not allowed to consume right now (subscriberId={}, subscriptionId={})", cc.getSubscriberId(), cc.getSubscriptionId());
        competingConsumerStrategy.unregisterCompetingConsumer(cc.getSubscriptionId(), cc.getSubscriberId());
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
            // Pausing (not just stopping delivery) is what lets resume later use the checkpoint. Without it:
            // 1. Subscriber 1 loses lock
            // 2. An event is published (A)
            // 3. Subscriber 2 doesn't have lock yet
            // 4. No one has the lock is detected, Subscriber 2 is resumed, but A was already missed.
            // Also only one subscription can exist per id per CatchupSubscriptionModel instance.
            pauseSubscription(subscriptionId, false);
        } else if (competingConsumer.isPaused()) {
            logDebug("CompetingConsumer is already paused, won't do anything (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        } else {
            logDebug("CompetingConsumer is neither running nor paused, won't do anything (subscriberId={}, subscriptionId={}, state={})", subscriberId, subscriptionId, competingConsumer.state.getClass().getSimpleName());
        }
    }

    private Subscription startWaitingConsumer(CompetingConsumer cc) {
        logDebug("Start CompetingConsumer that has previously been waiting (subscriberId={}, subscriptionId={})", cc.getSubscriberId(), cc.getSubscriptionId());
        String subscriptionId = cc.getSubscriptionId();
        competingConsumers.put(SubscriptionIdAndSubscriberId.from(subscriptionId, cc.getSubscriberId()), cc.registerRunning());
        return giveTheLeaseBackIfItThrows(cc.subscriptionIdAndSubscriberId, cc.state, ((CompetingConsumerState.Waiting) cc.state)::startSubscription);
    }

    /**
     * Starts or resumes a consumer through the wrapped model while this node holds its lease, and gives the lease back
     * if the wrapped model throws. Holding on to it would leave the subscription with a lease nobody on this node
     * serves, and every other node locked out of it for as long as this node keeps refreshing.
     * <p>
     * {@code previous} is the state the consumer had before it was recorded as running, and it is put back first, so
     * a synchronous {@code onConsumeProhibited} out of giving the lease back finds nothing running to pause. A
     * consumer that was not recorded at all, a {@code null} {@code previous}, is removed and unregistered, and the
     * caller of {@code subscribe} gets the failure. Any other is released, so it stays a candidate and a later grant
     * tries it again, also on a node with no other node to take the subscription over. A waiting consumer is put back as
     * waiting, which a grant starts. Any other is put back as paused by the system, which a grant resumes, whoever paused
     * it, since starting it again is what was asked for. Put back as paused by the user, it would never compete for the
     * lease again. Put back as running, which is what a consumer the wrapped model does not run can be recorded as, a
     * grant would find nothing to do. Either way nothing would retry it.
     */
    private Subscription giveTheLeaseBackIfItThrows(SubscriptionIdAndSubscriberId key, @Nullable CompetingConsumerState previous, Supplier<Subscription> start) {
        try {
            return start.get();
        } catch (Throwable e) {
            log.warn("The wrapped subscription model failed to start itself, or to start or resume a subscription this node holds the lease for, so the lease is given back (subscriberId={}, subscriptionId={})",
                    key.subscriberId(), key.subscriptionId());
            boolean unregister = previous == null;
            if (previous == null) {
                competingConsumers.remove(key);
            } else if (previous instanceof CompetingConsumerState.Waiting) {
                competingConsumers.put(key, new CompetingConsumer(key, previous));
            } else {
                competingConsumers.put(key, new CompetingConsumer(key, new CompetingConsumerState.Paused(false)));
            }
            try {
                if (unregister) {
                    competingConsumerStrategy.unregisterCompetingConsumer(key.subscriptionId(), key.subscriberId());
                } else {
                    competingConsumerStrategy.releaseCompetingConsumer(key.subscriptionId(), key.subscriberId());
                }
            } catch (Throwable givingBackFailed) {
                e.addSuppressed(givingBackFailed);
            }
            throw e;
        }
    }

    /**
     * Registers a waiting consumer with the strategy and starts it if that grants the lock there and then.
     * <p>
     * Registering can win the lock synchronously, and {@link #onConsumeGranted(String, String)} then starts
     * the consumer itself, but only on a change of status. A register that finds the lock already held gets no
     * callback, so the answer is read from the return value, and the state is re-read afterwards to see
     * whether the callback already acted on it.
     */
    private Subscription registerAndStartIfGranted(CompetingConsumer cc) {
        String subscriptionId = cc.getSubscriptionId();
        String subscriberId = cc.getSubscriberId();
        boolean acquiredLock = registerCompetingConsumer(subscriptionId, subscriberId);
        CompetingConsumer current = competingConsumers.get(cc.subscriptionIdAndSubscriberId);
        // hasLock as well, since a callback that failed to start the consumer left it waiting and gave the lease back
        if (acquiredLock && current != null && current.isWaiting() && hasLock(subscriptionId, subscriberId)) {
            return startWaitingConsumer(current);
        }
        return new CompetingConsumerSubscription(subscriptionId, subscriberId);
    }

    void pauseConsumer(CompetingConsumer cc, boolean pausedByUser) {
        logDebug("Pausing CompetingConsumer (subscriberId={}, subscriptionId={}, pausedByUser={})", cc.getSubscriberId(), cc.getSubscriptionId(), pausedByUser);
        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(cc);
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
            return state instanceof CompetingConsumerState.Paused || state instanceof CompetingConsumerState.PausedWhileWaiting;
        }

        boolean isRunning() {
            return state instanceof CompetingConsumerState.Running;
        }

        boolean isWaiting() {
            return state instanceof CompetingConsumerState.Waiting;
        }

        boolean isPausedWhileWaiting() {
            return state instanceof CompetingConsumerState.PausedWhileWaiting;
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

        CompetingConsumer registerPaused(boolean pausedByUser) {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Paused(pausedByUser));
        }

        // Only for a currently-Waiting consumer. The Waiting is kept rather than discarded, because it holds
        // the start supplier that is the only way to bring this consumer up. It never subscribed, so there is
        // nothing for delegate.resumeSubscription to resume.
        CompetingConsumer registerPausedWhileWaiting() {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.PausedWhileWaiting((CompetingConsumerState.Waiting) state));
        }

        // Only for a currently-PausedWhileWaiting consumer. Restores the Waiting it was paused from, supplier
        // intact.
        CompetingConsumer restoreWaiting() {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, ((CompetingConsumerState.PausedWhileWaiting) state).waiting);
        }
    }

    sealed interface CompetingConsumerState {

        final class Running implements CompetingConsumerState {
        }

        final class Waiting implements CompetingConsumerState {
            private final Supplier<Subscription> supplier;

            Waiting(Supplier<Subscription> supplier) {
                this.supplier = supplier;
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
        }

        final class PausedWhileWaiting implements CompetingConsumerState {
            private final Waiting waiting;

            PausedWhileWaiting(Waiting waiting) {
                this.waiting = waiting;
            }
        }
    }

    private void unregisterAllCompetingConsumers(Consumer<CompetingConsumer> andDo) {
        logDebug("Unregistering all CompetingConsumer's");
        unregisterCompetingConsumersMatching(cc -> true, andDo);
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

    /**
     * Records the consumer as running, registers it, and records it as paused by the system if registering did not win
     * the lock. The consumer stays registered, so the grant that comes once the lease is free resumes it. Recorded as
     * paused by the user instead, as a consumer {@link #stop()} paused is, that grant would hand the lease back and the
     * consumer would never compete for it again.
     * <p>
     * The order matters. Registering can be granted the lock there and then, and {@link #onConsumeGranted(String, String)}
     * resumes a paused consumer itself, so a consumer still recorded as paused when registration is granted is resumed
     * once by that callback and once by the caller here, and the second resume finds the delegate already running.
     * Recording it as running first leaves the callback nothing to do.
     * <p>
     * Only for a paused consumer. A waiting one has to stay waiting across the call, because that is what makes
     * {@code onConsumeGranted} subscribe it.
     * <p>
     * A register that throws puts the consumer back as it was. Left recorded as running, the next {@link #start()} would
     * skip it, and no grant would ever come for a consumer the strategy never registered.
     */
    private boolean registerAsRunning(CompetingConsumer competingConsumer) {
        SubscriptionIdAndSubscriberId key = competingConsumer.subscriptionIdAndSubscriberId;
        competingConsumers.put(key, competingConsumer.registerRunning());
        final boolean acquired;
        try {
            acquired = registerCompetingConsumer(key.subscriptionId(), key.subscriberId());
        } catch (RuntimeException e) {
            competingConsumers.put(key, competingConsumer);
            throw e;
        }
        if (!acquired) {
            competingConsumers.put(key, competingConsumer.registerPaused(false));
        }
        return acquired;
    }

    private boolean hasLock(String subscriptionId, String subscriberId) {
        return competingConsumerStrategy.hasLock(subscriptionId, subscriberId);
    }

    /**
     * Uniqueness is scoped to this instance, and only to this instance. Several instances subscribing to one
     * subscription id is the competing consumer pattern itself, and the strategy is what coordinates them. Several
     * subscriptions for one id <i>inside</i> one instance is a different thing, and nothing here can express it:
     * {@link #cancelSubscription(String)}, {@link #pauseSubscription(String)} and {@link #resumeSubscription(String)}
     * all resolve by subscription id alone, so the second one would be unreachable through every one of them, and both
     * would be sharing the single delegate that refuses a duplicate id in its own right.
     * <p>
     * Both collections count, because a subscription whose start position opted out of competing consumption occupies
     * the id just as much as a competing one does.
     */
    // A subscription id is unique per model instance, so an id neither collection here holds is unknown to this
    // model, whatever the delegate may separately know about it.
    private void requireKnown(String subscriptionId) {
        if (!isSubscriptionIdInUse(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
    }

    private boolean isSubscriptionIdInUse(String subscriptionId) {
        return nonCompetingConsumersSubscriptions.contains(subscriptionId)
                || findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId)).isPresent();
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

    @Override
    public String toString() {
        return new StringJoiner(", ", CompetingConsumerSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("delegate=" + delegate)
                .add("competingConsumerStrategy=" + competingConsumerStrategy)
                .add("competingConsumers=" + competingConsumers)
                .add("nonCompetingConsumersSubscriptions=" + nonCompetingConsumersSubscriptions)
                .toString();
    }
}