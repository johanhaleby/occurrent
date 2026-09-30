package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.RetryStrategy;
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

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
 * competing subscription, also one made while it is stopped, with three exceptions. {@code stop()} pauses a
 * subscription in the wrapped model when that model still runs it, which it can after throwing from its own
 * {@code stop()}. The first exception is a subscription the wrapped model cannot pause, which keeps its lease and stays
 * running, since it still delivers, and {@code stop()} throws. The second is a subscription the user resumes with
 * {@link #resumeSubscription(String)}. It competes for its lease and runs once this node wins it, whether that happens
 * straight away or on a later grant. The third is described below. A {@code subscribe(..)} on another thread whose
 * registration is under way when {@code stop()} runs, or whose subscription the wrapped model already runs, can still
 * hold a lease after {@code stop()} has returned. It gives the lease up at its next step, as {@code stop()} would.
 * <br>
 * <br>
 * A pause in the wrapped model that throws after this node has lost a lease is tried again, on a thread of its own,
 * until the wrapped model no longer runs the subscription or this node holds the lease again. A wrapped model that
 * returns from {@code pauseSubscription} normally but keeps running the subscription goes on delivering without the
 * lease.
 * <br>
 * <br>
 * A competing subscription made while this model is stopped goes to the wrapped model straight away, through
 * {@link SubscriptionModel#subscribePaused}, which holds it paused whether or not the wrapped model runs. It starts from
 * the position the wrapped model gives a subscription made while that model is stopped. For a
 * {@code DurableSubscriptionModel} with no stored position, that is the position when {@code subscribe(..)} was called,
 * so an event written before {@link #start(boolean)} is delivered. The subscription competes for its lease once this
 * model is started, with or without resuming subscriptions automatically, and winning the lease resumes it, or
 * subscribes it in the wrapped model when that model does not have it.
 * <br>
 * <br>
 * A competing subscription made while this model runs goes to the wrapped model through {@code subscribePaused} too,
 * once this node has won its lease, and is resumed there only when this node still holds the lease after that. So one
 * whose lease goes to another node while the wrapped model makes it delivers nothing, and waits for a grant.
 * <br>
 * <br>
 * A wrapped model that refuses {@code subscribePaused} with {@link UnsupportedOperationException}, as its default
 * implementation does, makes the third exception. A subscription made while this model is stopped competes for its
 * lease straight away and, when this node wins it, is subscribed in that wrapped model without starting it. When the
 * wrapped model runs it, it delivers events before {@link #start(boolean)}. Waiting for {@code start()} instead would
 * subscribe it where that model starts a subscription at that moment, past every event written in between, and losing
 * no event ranks above what {@code stop()} promises. It still delivers only while this node holds its lease, and
 * {@code start()} finds it started. Once it has won the lease it competes as one the user resumed does, so losing the
 * lease pauses it and a later grant resumes it. One that loses the lease when it registers, or that the wrapped model
 * holds paused, waits for {@code start()} and competes from there, so a stopped node competes for nothing it does not
 * deliver.
 * <br>
 * <br>
 * The wrapped model is started, without resuming what it holds paused, before a subscription whose lease this node
 * holds is subscribed or resumed there, since a stopped model holds a new subscription paused. It is started only while
 * this model's monitor is held, which every lifecycle call and lease callback holds too, so a {@code stop()} that has
 * returned is never followed by a start for a subscription it overtook.
 */
@NullMarked
public class CompetingConsumerSubscriptionModel implements SubscriptionModelWrapper, SubscriptionModel, SubscriptionModelLifeCycle, IntrospectableSubscriptions, CompetingConsumerListener {
    private static final Logger log = LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);

    private final SubscriptionModel delegate;
    private final CompetingConsumerStrategy competingConsumerStrategy;

    private final AtomicBoolean stoppedByUser = new AtomicBoolean(false);
    // Consumers the user resumed since stop(), and ones made since that a wrapped model refusing subscribePaused runs,
    // which compete and run although this model is stopped
    private final Set<SubscriptionIdAndSubscriberId> mayRunWhileStopped = ConcurrentHashMap.newKeySet();

    private final ConcurrentMap<SubscriptionIdAndSubscriberId, CompetingConsumer> competingConsumers = new ConcurrentHashMap<>();
    // Subscriptions whose StartAt position indicated they should not use the competing consumer model
    private final Set<String> nonCompetingConsumersSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // Ids a subscribe is making right now, read and written under the monitor only. A second subscribe for one of them
    // is refused before the first records it.
    private final Map<String, BeingMade> subscriptionsBeingMade = new HashMap<>();
    // Set before the lease strategy is shut down, which happens outside the monitor
    private volatile boolean shutDown;
    // What the wrapped model holds for a subscribe that threw, written under the monitor only. The next subscribe of
    // the id takes it over, since cancelling it could delete the position a durable subscription stored. Until then
    // this model answers for the id as for one it does not know.
    private final Map<String, Kept> keptForTheNextSubscribe = new ConcurrentHashMap<>();
    // Consumers whose pause is tried again on a thread of its own, added and removed under the monitor only
    private final Set<SubscriptionIdAndSubscriberId> pausesTriedAgain = ConcurrentHashMap.newKeySet();
    private static final RetryStrategy.Retry PAUSE_RETRY_STRATEGY = RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0);
    // A pause that keeps failing warns on every fifth try, which is every ten seconds once the backoff has reached two
    private static final int PAUSE_TRIES_BETWEEN_WARNINGS = 5;

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
     * @throws IllegalStateException            If this model is shut down, or is shut down or has the subscription
     *                                          cancelled while it is being made. Also if the wrapped model holds a
     *                                          subscription of this id that a {@code subscribe} which threw made, and
     *                                          this call gives another filter or start position, or a start position
     *                                          that does not compete. {@link #cancelSubscription(String)} removes that
     *                                          subscription.
     *                                          <p>
     *                                          A {@code subscribe} that throws records nothing, and this model then
     *                                          answers for the id as for one it does not know. What the wrapped model
     *                                          has made for it by then is paused there, and the next
     *                                          {@code subscribe} of the same id with the same filter and start position
     *                                          takes it over, with the action that call gives, instead of making it
     *                                          again. When the wrapped model still runs it after the pause,
     *                                          {@code subscribe} returns instead of throwing, with the subscription
     *                                          recorded as running and the pause tried again. Two start
     *                                          positions are the same when both are {@link StartAt#now()}, both are
     *                                          {@link StartAt#subscriptionModelDefault()}, both hold equal checkpoints,
     *                                          or both are the same dynamic {@code StartAt} object.
     */
    // Registers with the lease strategy and subscribes in the wrapped model without holding the monitor, since a
    // registration retries through a whole database outage and a wrapped subscribe can take as long as opening a change
    // stream. Every other step runs under the monitor and decides from what holds at that moment.
    public Subscription subscribe(String subscriberId, String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriberId, "SubscriberId cannot be null");
        Objects.requireNonNull(subscriptionId, "SubscriptionId cannot be null");
        BeingMade beingMade = reserveSubscriptionId(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId));
        beingMade.filter = filter;
        beingMade.startAt = startAt;
        try {
            boolean competes = startAt.get(new SubscriptionModelContext(CompetingConsumerSubscriptionModel.class)) != null;
            takeOverWhatAFailedSubscribeKept(beingMade, competes, action);
            if (!competes) {
                // Not allowed to start the competing consumer subscription, delegate to parent instead. One case: a
                // non-durable in-memory subscription started on multiple nodes, where every node should receive every
                // event, so competing consumption is not wanted.
                beingMade.subscription = getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
                recordNonCompetingSubscription(beingMade);
                return beingMade.subscription;
            }
            return makeCompetingConsumerSubscription(beingMade, filter, startAt);
        } finally {
            releaseSubscriptionId(subscriptionId);
        }
    }

    private synchronized BeingMade reserveSubscriptionId(SubscriptionIdAndSubscriberId key) {
        if (shutDown) {
            throw new IllegalStateException("Subscription " + key.subscriptionId() + " was not made, since " + CompetingConsumerSubscriptionModel.class.getSimpleName() + " is shut down");
        }
        if (isSubscriptionIdInUse(key.subscriptionId()) || subscriptionsBeingMade.containsKey(key.subscriptionId())) {
            throw new DuplicateSubscriptionIdException(key.subscriptionId());
        }
        BeingMade beingMade = new BeingMade(key);
        subscriptionsBeingMade.put(key.subscriptionId(), beingMade);
        return beingMade;
    }

    private synchronized void releaseSubscriptionId(String subscriptionId) {
        subscriptionsBeingMade.remove(subscriptionId);
    }

    // Recorded only once the delegate has accepted it. Recording first would leave the id occupied by a subscription
    // that was refused, and the check in subscribe would then refuse it for good. A start() since the delegate got it
    // resumed only what it knew, so the subscription is resumed here.
    private synchronized void recordNonCompetingSubscription(BeingMade beingMade) {
        String subscriptionId = beingMade.key.subscriptionId();
        if (shutDown || beingMade.cancelled) {
            throw notMade(beingMade);
        }
        nonCompetingConsumersSubscriptions.add(subscriptionId);
        if (beingMade.startedMeanwhile && !stoppedByUser.get() && delegate.isPaused(subscriptionId)) {
            startTheWrappedModelIfStopped();
            delegate.resumeSubscription(subscriptionId);
        }
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
        // A subscribe making this id right now takes back what it made and throws once it finds this
        BeingMade beingMade = subscriptionsBeingMade.get(subscriptionId);
        if (beingMade != null) {
            beingMade.cancelled = true;
        }
        delegate.cancelSubscription(subscriptionId);
        keptForTheNextSubscribe.remove(subscriptionId);
        // Forgotten here too, not only in the competing consumer map, so the id is free for a new subscription
        // afterwards. Remembering a cancelled one also made start() resume a subscription the delegate no longer has.
        nonCompetingConsumersSubscriptions.remove(subscriptionId);
        findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId))
                .ifPresent(cc -> unregisterCompetingConsumer(cc, __ -> {
                    competingConsumers.remove(cc.subscriptionIdAndSubscriberId);
                    mayRunWhileStopped.remove(cc.subscriptionIdAndSubscriberId);
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
        mayRunWhileStopped.clear();
        try {
            if (delegate.isRunning()) {
                delegate.stop();
            }
        } catch (RuntimeException e) {
            // The consumers are stopped also when stopping the wrapped model throws, or this node would keep leases
            // for subscriptions it no longer means to serve
            List<String> paused = new ArrayList<>();
            RuntimeException consumerFailure = stopEveryConsumer(paused);
            String outcome = paused.isEmpty()
                    ? "it had no running subscription to pause."
                    : "subscriptions " + paused + " are paused in the wrapped model and gave up their lease. start(true) resumes them, while start(false) keeps them paused until each one is resumed.";
            IllegalStateException failure = new IllegalStateException("Stopping the wrapped subscription model failed. This model is stopped anyway, and " + outcome, e);
            if (consumerFailure != null) {
                failure.addSuppressed(consumerFailure);
            }
            throw failure;
        } catch (Throwable e) {
            RuntimeException consumerFailure = stopEveryConsumer(new ArrayList<>());
            if (consumerFailure != null) {
                e.addSuppressed(consumerFailure);
            }
            throw e;
        }
        RuntimeException consumerFailure = stopEveryConsumer(new ArrayList<>());
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
     * any later one attached as suppressed. The id of each consumer this pauses is added to {@code paused}.
     * <p>
     * A subscribe on another thread whose registration has returned gives it up too, unless the wrapped model already
     * runs its subscription. That one, and one whose registration is under way, give it up at their next step.
     */
    private @Nullable RuntimeException stopEveryConsumer(List<String> paused) {
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
                        paused.add(c.getSubscriptionId());
                    }
                });
            } catch (RuntimeException e) {
                firstFailure = withSuppressed(firstFailure, e);
            }
        }
        // A subscribe on another thread gives up its registration at its next step too, but that step can take as long
        // as the wrapped model takes to make the subscription. One the wrapped model already runs is left to that step,
        // which pauses it first and keeps the lease when it cannot, as above.
        for (BeingMade beingMade : subscriptionsBeingMade.values()) {
            if (beingMade.registrationReturned && !delegate.isRunning(beingMade.key.subscriptionId())) {
                try {
                    // Forgotten too, so its next step registers again once this model has been started
                    giveUpTheRegistration(beingMade);
                } catch (RuntimeException e) {
                    firstFailure = withSuppressed(firstFailure, e);
                }
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
        for (BeingMade beingMade : subscriptionsBeingMade.values()) {
            beingMade.startedMeanwhile = true;
        }
        stoppedByUser.set(false);
        mayRunWhileStopped.clear();
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
                // made while this model was stopped. So does one that lost its lease before the stop, since no user
                // paused it either. One the user or stop() paused is resumed only when asked to.
                if (cc.isWaiting()) {
                    logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={}, state={})", cc.getSubscriberId(), cc.getSubscriptionId(), cc.state.getClass().getSimpleName());
                    registerAndStartIfGranted(cc);
                } else if (cc.isPaused() && (resumeSubscriptionsAutomatically || cc.isPausedByTheLossOfItsLease())) {
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
    // lock yet only when it has made the subscription. One a subscribe that threw kept there for the next subscribe of
    // its id is not reported, as pausing or resuming it answers that the id is unknown.
    @Override
    public Set<String> subscriptionIds() {
        Set<String> ids = competingConsumers.keySet().stream()
                .map(SubscriptionIdAndSubscriberId::subscriptionId)
                .collect(Collectors.toCollection(HashSet::new));
        IntrospectableSubscriptions.findIn(getWrappedSubscriptionModel())
                .map(IntrospectableSubscriptions::subscriptionIds)
                .ifPresent(ids::addAll);
        keptForTheNextSubscribe.keySet().forEach(id -> {
            if (!isSubscriptionIdInUse(id)) {
                ids.remove(id);
            }
        });
        return Set.copyOf(ids);
    }

    /**
     * @see SubscriptionModelLifeCycle#isPaused(String)
     */
    @Override
    public boolean isPaused(String subscriptionId) {
        // A consumer paused before the wrapped model made its subscription is known only here. The wrapped model reports
        // one it holds paused, which is one made while this model was stopped and one whose lease went to another node
        // while the wrapped model made it. subscriptionIds() merges both sources for the same reason. One a subscribe
        // that threw kept there is paused there too, but unknown here until the next subscribe of its id.
        boolean pausedHere = findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId) && cc.isPaused()).isPresent();
        return pausedHere || (!keptForTheNextSubscribe.containsKey(subscriptionId) && delegate.isPaused(subscriptionId));
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
                        mayRunWhileStopped.add(competingConsumer.subscriptionIdAndSubscriberId);
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

    private CompetingConsumerSubscription makeCompetingConsumerSubscription(BeingMade beingMade, SubscriptionFilter filter, StartAt startAt) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
        AtomicReference<Consumer<CloudEvent>> actionToCall = requireNonNull(beingMade.action);
        // Forwards, so a subscribe that takes this subscription over later hands the wrapped model its own action
        Consumer<CloudEvent> forwardingAction = event -> actionToCall.get().accept(event);
        beingMade.waiting = new CompetingConsumerState.Waiting(() -> {
            logDebug("Starting delegated CompetingConsumer subscription after waiting (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
            if (delegate.isPaused(subscriptionId)) {
                return resumeInTheWrappedModel(subscriptionId);
            }
            startTheWrappedModelIfStopped();
            return delegate.subscribe(subscriptionId, filter, startAt, forwardingAction);
        });
        while (true) {
            Step step = nextStep(beingMade);
            try {
                switch (step) {
                    case DONE -> {
                        return new CompetingConsumerSubscription(subscriptionId, key.subscriberId(), beingMade.subscription);
                    }
                    case REGISTER -> {
                        beingMade.registered = true;
                        registerCompetingConsumer(subscriptionId, key.subscriberId());
                        beingMade.registrationReturned = true;
                    }
                    case SUBSCRIBE_PAUSED -> {
                        try {
                            beingMade.subscription = delegate.subscribePaused(subscriptionId, filter, startAt, forwardingAction);
                        } catch (UnsupportedOperationException e) {
                            logDebug("Wrapped model cannot hold the subscription paused, competing for the lease straight away (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
                            beingMade.refusesSubscribePaused = true;
                        }
                    }
                    case SUBSCRIBE -> beingMade.subscription = delegate.subscribe(subscriptionId, filter, startAt, forwardingAction);
                }
            } catch (Throwable e) {
                if (takeBackAfterAFailedStep(beingMade, e)) {
                    return new CompetingConsumerSubscription(subscriptionId, key.subscriberId(), beingMade.subscription);
                }
                throw e;
            }
        }
    }

    // What one subscribe has made so far. Only the subscribing thread writes it, except for cancelled and
    // startedMeanwhile, which cancelSubscription(..) and start(..) set under the monitor. stop() reads
    // registrationReturned, set once the registration has returned, to give up a lease it won, and then clears it and
    // registered under the monitor, while the subscribing thread is in a step that touches neither.
    private static final class BeingMade {
        private final SubscriptionIdAndSubscriberId key;
        private CompetingConsumerState.@Nullable Waiting waiting;
        private @Nullable AtomicReference<Consumer<CloudEvent>> action;
        private @Nullable SubscriptionFilter filter;
        private @Nullable StartAt startAt;
        private boolean registered;
        private volatile boolean registrationReturned;
        private boolean refusesSubscribePaused;
        private @Nullable Subscription subscription;
        private boolean cancelled;
        private boolean startedMeanwhile;

        private BeingMade(SubscriptionIdAndSubscriberId key) {
            this.key = key;
        }
    }

    // A step subscribe takes outside the monitor, or DONE once the subscription is recorded
    private enum Step {
        REGISTER, SUBSCRIBE_PAUSED, SUBSCRIBE, DONE
    }

    // A subscription the wrapped model holds for a subscribe that threw, the action it forwards to, and the filter and
    // start position it was made with
    private record Kept(Subscription subscription, AtomicReference<Consumer<CloudEvent>> action, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt) {
    }

    /**
     * Takes over what the wrapped model holds for a subscribe of this id that threw, and makes it forward to
     * {@code action}. It does so only for a competing subscribe with the filter and start position the subscription was
     * made with, since the wrapped model would go on using those. Any other subscribe of the id throws, and what the
     * wrapped model holds stays kept.
     */
    private synchronized void takeOverWhatAFailedSubscribeKept(BeingMade beingMade, boolean competes, Consumer<CloudEvent> action) {
        String subscriptionId = beingMade.key.subscriptionId();
        beingMade.action = new AtomicReference<>(action);
        Kept kept = keptForTheNextSubscribe.get(subscriptionId);
        if (kept == null) {
            return;
        }
        if (!delegate.isPaused(subscriptionId) && !delegate.isRunning(subscriptionId)) {
            // Cancelled in the wrapped model itself since then
            keptForTheNextSubscribe.remove(subscriptionId);
            return;
        }
        if (!competes || !Objects.equals(kept.filter, beingMade.filter) || !sameStartPosition(kept.startAt, beingMade.startAt)) {
            throw new IllegalStateException("Subscription " + subscriptionId + " was not made, since the wrapped subscription model holds a subscription of that id from a subscribe that failed, made with "
                    + (competes ? "another filter or start position" : "a start position that competes for it") + ". Subscribe with the filter and start position of that call to take it over, or cancel it first with cancelSubscription(\"" + subscriptionId + "\").");
        }
        keptForTheNextSubscribe.remove(subscriptionId);
        kept.action.set(action);
        beingMade.subscription = kept.subscription;
        beingMade.action = kept.action;
    }

    // StartAt has no equals of its own, and a dynamic one is the same only as itself, since two functions cannot be
    // compared
    private static boolean sameStartPosition(@Nullable StartAt a, @Nullable StartAt b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        } else if (a instanceof StartAt.StartAtCheckpoint x && b instanceof StartAt.StartAtCheckpoint y) {
            return x.checkpoint.equals(y.checkpoint);
        }
        return (a.isNow() && b.isNow()) || (a.isDefault() && b.isDefault());
    }

    /**
     * Decides the next step of a subscribe from what holds now, and records the subscription once nothing is left to
     * do. A lease callback for it before then finds nothing recorded and does nothing, so the lease is asked for here.
     * <p>
     * The wrapped model gets the subscription through {@code subscribePaused}, and it is resumed there only here, once
     * this node holds its lease. While this model is stopped that happens without competing, since a lease won while
     * stopped would lock every other node out of a subscription this node does not serve. A wrapped model that refuses
     * {@code subscribePaused} gets it through {@code subscribe} once this node wins the lease, and one it runs while
     * stopped may keep running.
     * <p>
     * Nothing here cancels a subscription in the wrapped model unless the user cancelled it, since cancelling can delete
     * the position a durable subscription has stored. Anything made for a state that no longer holds, or that a step
     * here fails to resume, is paused in the wrapped model and waits for a grant.
     */
    private synchronized Step nextStep(BeingMade beingMade) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        if (shutDown || beingMade.cancelled) {
            throw notMade(beingMade);
        }
        boolean stopped = stoppedByUser.get();
        if (beingMade.subscription == null) {
            try {
                if (stopped && !beingMade.refusesSubscribePaused) {
                    // A registration that a stop() overtook may have won a lease this node must not hold while stopped
                    giveUpTheRegistration(beingMade);
                    return Step.SUBSCRIBE_PAUSED;
                } else if (!beingMade.registered) {
                    return Step.REGISTER;
                } else if (!hasLock(subscriptionId, key.subscriberId())) {
                    if (stopped) {
                        giveUpTheRegistration(beingMade);
                    }
                    return recordWaiting(beingMade);
                } else if (stopped) {
                    mayRunWhileStopped.add(key);
                    return Step.SUBSCRIBE;
                }
                startTheWrappedModelIfStopped();
                return beingMade.refusesSubscribePaused ? Step.SUBSCRIBE : Step.SUBSCRIBE_PAUSED;
            } catch (RuntimeException e) {
                // Nothing is made yet, so giving up the registration frees the id
                takeBack(beingMade, e);
                throw e;
            }
        }
        boolean waits;
        try {
            if (stopped) {
                // Only a subscription the wrapped model runs may run while stopped, so one it holds paused gives up that
                if (!mayRunWhileStopped.contains(key) || !delegate.isRunning(subscriptionId)) {
                    mayRunWhileStopped.remove(key);
                    waits = true;
                } else {
                    waits = !hasLock(subscriptionId, key.subscriberId());
                }
            } else if (!beingMade.registered) {
                return Step.REGISTER;
            } else if (!hasLock(subscriptionId, key.subscriberId())) {
                waits = true;
            } else {
                if (delegate.isPaused(subscriptionId)) {
                    resumeInTheWrappedModel(subscriptionId);
                }
                waits = false;
            }
        } catch (RuntimeException e) {
            log.warn("Could not run CompetingConsumer in the wrapped subscription model, so it waits for a grant of its lease, which tries it again (subscriberId={}, subscriptionId={})",
                    key.subscriberId(), subscriptionId, e);
            waits = true;
        }
        return waits ? waitForAGrant(beingMade) : recordRunning(key);
    }

    private Step recordRunning(SubscriptionIdAndSubscriberId key) {
        competingConsumers.put(key, new CompetingConsumer(key, new CompetingConsumerState.Running()));
        return Step.DONE;
    }

    /**
     * Pauses what the subscribe made in the wrapped model, and records it as waiting for a grant of its lease, which
     * resumes it. It keeps its registration while it may compete, and gives up a lease it holds, so another node or a
     * later grant here runs it. While this model is stopped it gives up the registration instead, and {@code start()}
     * registers it again.
     * <p>
     * One the wrapped model still runs after the pause is recorded as running and keeps its registration, since it still
     * delivers. It keeps a lease it holds, as one that {@code stop()} cannot pause does, and competes for one it lost,
     * as one whose lease-loss callback could not pause it does. One whose registration a {@code stop()} gave up before
     * the wrapped model ran it registers again first. A pause that threw is then tried again until the wrapped model no
     * longer runs it or this node holds the lease again.
     * <p>
     * Giving up the lease or the registration can throw. When this node no longer holds the lease after that, which is
     * how the MongoDB strategies fail, the subscription waits for a grant all the same. When it still holds the lease,
     * no grant would come, so the subscribe throws and records nothing, as it does when anything else here throws. It
     * returns all the same when the wrapped model still runs the subscription then.
     */
    private Step waitForAGrant(BeingMade beingMade) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        try {
            if (delegate.isRunning(subscriptionId)) {
                boolean pauseFailed = false;
                try {
                    delegate.pauseSubscription(subscriptionId);
                } catch (RuntimeException e) {
                    log.warn("Could not pause CompetingConsumer in the wrapped subscription model (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId, e);
                    pauseFailed = true;
                }
                if (delegate.isRunning(subscriptionId)) {
                    logDebug("Wrapped model still runs the CompetingConsumer after it was paused there, so it stays registered (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
                    if (!beingMade.registered) {
                        return Step.REGISTER;
                    }
                    recordRunning(key);
                    if (pauseFailed && !hasLock(subscriptionId, key.subscriberId())) {
                        tryThePauseAgain(key);
                    }
                    return Step.DONE;
                }
            }
            recordWaiting(beingMade);
            try {
                if (stoppedByUser.get() && !mayRunWhileStopped.contains(key)) {
                    giveUpTheRegistration(beingMade);
                } else if (beingMade.registered && hasLock(subscriptionId, key.subscriberId())) {
                    competingConsumerStrategy.releaseCompetingConsumer(subscriptionId, key.subscriberId());
                }
            } catch (RuntimeException e) {
                if (hasLock(subscriptionId, key.subscriberId())) {
                    throw e;
                }
                log.warn("Could not give up the lease of CompetingConsumer, but this node no longer holds it, so the subscription waits for a grant (subscriberId={}, subscriptionId={})",
                        key.subscriberId(), subscriptionId, e);
            }
            return Step.DONE;
        } catch (RuntimeException e) {
            if (keepForTheNextSubscribe(beingMade, e)) {
                return Step.DONE;
            }
            throw e;
        }
    }

    // Registered while this model runs, so the grant starts it. Unregistered while stopped, so start() registers it.
    private Step recordWaiting(BeingMade beingMade) {
        competingConsumers.put(beingMade.key, new CompetingConsumer(beingMade.key, requireNonNull(beingMade.waiting)));
        return Step.DONE;
    }

    // An unregister that throws once this node no longer holds the lease counts as given up, so the next step registers
    // again. The MongoDB strategies fail that way, since they forget the consumer before they call MongoDB.
    private void giveUpTheRegistration(BeingMade beingMade) {
        if (beingMade.registered) {
            try {
                competingConsumerStrategy.unregisterCompetingConsumer(beingMade.key.subscriptionId(), beingMade.key.subscriberId());
            } catch (RuntimeException e) {
                if (!hasLock(beingMade.key.subscriptionId(), beingMade.key.subscriberId())) {
                    beingMade.registered = false;
                    beingMade.registrationReturned = false;
                }
                throw e;
            }
            beingMade.registered = false;
            beingMade.registrationReturned = false;
        }
    }

    // Takes back what the subscribe made and gives up its registration first
    private IllegalStateException notMade(BeingMade beingMade) {
        IllegalStateException failure = new IllegalStateException("Subscription " + beingMade.key.subscriptionId() + " was not made, since " + CompetingConsumerSubscriptionModel.class.getSimpleName()
                + " was shut down or had the subscription cancelled while it was being made");
        takeBack(beingMade, failure);
        return failure;
    }

    /**
     * Takes back what a subscribe has made when it throws, and gives up its registration. Only a subscription the user
     * cancelled is cancelled in the wrapped model, since cancelling can delete the position a durable subscription has
     * stored. Anything else it made is paused there, and one made before a shutdown is left for the wrapped model's own
     * {@code shutdown()}. Every failure on the way is added to {@code failure} as suppressed.
     */
    private void takeBack(BeingMade beingMade, Throwable failure) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        mayRunWhileStopped.remove(key);
        if (beingMade.subscription != null) {
            try {
                if (beingMade.cancelled) {
                    delegate.cancelSubscription(subscriptionId);
                } else if (delegate.isRunning(subscriptionId)) {
                    delegate.pauseSubscription(subscriptionId);
                }
            } catch (Throwable e) {
                failure.addSuppressed(e);
            }
        }
        try {
            giveUpTheRegistration(beingMade);
        } catch (Throwable e) {
            failure.addSuppressed(e);
        }
    }

    // A step outside the monitor failed. Answers whether the subscribe returns all the same, and it throws otherwise.
    private synchronized boolean takeBackAfterAFailedStep(BeingMade beingMade, Throwable failure) {
        if (shutDown || beingMade.cancelled) {
            takeBack(beingMade, failure);
            return false;
        }
        return keepForTheNextSubscribe(beingMade, failure);
    }

    /**
     * Forgets a subscription whose subscribe throws, and gives up its registration. What the wrapped model has made for
     * it is paused there, and the next subscribe of the id with the same filter and start position takes
     * it over, since cancelling it could delete the position a durable subscription has stored. Every failure on the
     * way is added to {@code failure} as suppressed.
     * <p>
     * One the wrapped model still runs after the pause is not forgotten, since it delivers. It is recorded as running,
     * its pause is tried again as after a lost lease, and this answers {@code true}, so the subscribe returns.
     */
    private boolean keepForTheNextSubscribe(BeingMade beingMade, Throwable failure) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        competingConsumers.remove(key);
        takeBack(beingMade, failure);
        if (beingMade.subscription != null && delegate.isRunning(key.subscriptionId())) {
            log.warn("Could not make CompetingConsumer, and the wrapped subscription model still runs it, so it is recorded as running and its pause is tried again (subscriberId={}, subscriptionId={})",
                    key.subscriberId(), key.subscriptionId(), failure);
            recordRunning(key);
            // Pauses only when this node does not hold the lease by then
            tryThePauseAgain(key);
            return true;
        }
        if (beingMade.subscription != null && beingMade.action != null) {
            keptForTheNextSubscribe.put(beingMade.key.subscriptionId(), new Kept(beingMade.subscription, beingMade.action, beingMade.filter, beingMade.startAt));
            log.warn("Could not make CompetingConsumer, so the wrapped subscription model holds it paused until the next subscribe of the same id, filter and start position takes it over, or until it is cancelled (subscriberId={}, subscriptionId={})",
                    beingMade.key.subscriberId(), beingMade.key.subscriptionId(), failure);
        }
        return false;
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
    public void shutdown() {
        logDebug("Trying to shutdown CompetingConsumer subscription model");
        shutDown = true;
        // Before taking the monitor, since this ends a registration waiting between two attempts on any thread, also on
        // one that holds the monitor in start(..) or resumeSubscription(..). An attempt blocked on a MongoDB read keeps
        // the monitor until the read returns, which with no socket timeout is when MongoDB answers again.
        competingConsumerStrategy.shutdown();
        synchronized (this) {
            delegate.shutdown();
            nonCompetingConsumersSubscriptions.clear();
            keptForTheNextSubscribe.clear();
            unregisterAllCompetingConsumers(cc -> competingConsumers.remove(cc.subscriptionIdAndSubscriberId));
            competingConsumerStrategy.removeListener(this);
        }
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
        boolean mayRun = !stoppedByUser.get() || mayRunWhileStopped.contains(competingConsumer.subscriptionIdAndSubscriberId);
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
            try {
                pauseSubscription(subscriptionId, false);
            } catch (RuntimeException e) {
                log.warn("Could not pause CompetingConsumer after this node lost its lease, so the pause is tried again (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId, e);
                tryThePauseAgain(subscriptionIdAndSubscriberId);
            }
        } else if (competingConsumer.isPaused()) {
            logDebug("CompetingConsumer is already paused, won't do anything (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        } else {
            logDebug("CompetingConsumer is neither running nor paused, won't do anything (subscriberId={}, subscriptionId={}, state={})", subscriberId, subscriptionId, competingConsumer.state.getClass().getSimpleName());
        }
    }

    /**
     * Tries a pause that failed after this node lost the lease again, on a thread of its own, until the wrapped model no
     * longer runs the subscription or this node holds the lease again. Until then the subscription delivers events that
     * the node holding the lease delivers too, so every fifth try that fails is logged as a warning. Called under the
     * monitor.
     */
    private void tryThePauseAgain(SubscriptionIdAndSubscriberId key) {
        if (!pausesTriedAgain.add(key)) {
            return;
        }
        RetryStrategy retryStrategy = PAUSE_RETRY_STRATEGY.onError((info, e) -> {
            if (info.getAttemptNumber() % PAUSE_TRIES_BETWEEN_WARNINGS == 0) {
                log.warn("Still could not pause CompetingConsumer after {} tries since this node lost its lease, so it delivers events that the node holding the lease delivers too (subscriberId={}, subscriptionId={})",
                        info.getAttemptNumber(), key.subscriberId(), key.subscriptionId(), e);
            }
        });
        Thread.ofPlatform().daemon().name("occurrent-competing-consumer-pause-" + key.subscriptionId()).start(() -> {
            try {
                retryStrategy.execute(() -> pauseWithoutTheLease(key), __ -> !shutDown);
            } catch (Throwable e) {
                logDebug("Stopped trying to pause CompetingConsumer, since the model is shut down (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId(), e);
            } finally {
                if (shutDown) {
                    pausesTriedAgain.remove(key);
                }
            }
        });
    }

    // One try, which throws when the pause fails again. Removing the key under the monitor once nothing is left to do
    // lets a later failure start a new thread.
    private synchronized void pauseWithoutTheLease(SubscriptionIdAndSubscriberId key) {
        CompetingConsumer competingConsumer = competingConsumers.get(key);
        if (!shutDown && competingConsumer != null && competingConsumer.isRunning() && !hasLock(key.subscriptionId(), key.subscriberId())) {
            pauseSubscription(key.subscriptionId(), false);
        }
        pausesTriedAgain.remove(key);
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

        boolean isPausedByTheLossOfItsLease() {
            return state instanceof CompetingConsumerState.Paused paused && !paused.pausedByUser;
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