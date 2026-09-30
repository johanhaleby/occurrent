/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.MaxAttempts;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;
import org.occurrent.retry.internal.RetryImpl;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Common operations for MongoDB lease-based competing consumer strategies
 */
@NullMarked
public class MongoLeaseCompetingConsumerStrategySupport {
    private static final Logger log = LoggerFactory.getLogger(MongoLeaseCompetingConsumerStrategySupport.class);

    public static final String DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";
    public static final Duration DEFAULT_LEASE_TIME = Duration.ofSeconds(20);

    /**
     * Enough that the handful of subscriptions one instance runs rarely collide, small enough to be worth no thought.
     */
    private static final int CONSUMER_LOCKS = 16;

    /**
     * How many attempts a single MongoDB call gets on the two paths where giving up on it is safe.
     * <p>
     * {@code scheduleRefresh} runs on a single-thread scheduler that starts the next round only once the current
     * one returns, so a call that retries without limit, against a strategy configured with the default
     * {@link Retry#infiniteAttempts()}, never lets a later round run at all, and a subscriber that would otherwise
     * have taken over the lease by then never gets the chance.
     * <p>
     * Unregistering and releasing a consumer give up a lease this node has stopped refreshing, and that lease
     * expires on its own after {@code leaseTime} whether or not the call gets through, after which any node can
     * take the subscription over. Retrying that removal keeps a closing application waiting for a database it
     * cannot reach, to delete a document that is about to stop mattering anyway.
     * <p>
     * Registering is the one call with nothing covering a failure, so it keeps retrying exactly as configured.
     * 5 attempts matches the cap this codebase already uses elsewhere for a MongoDB call that is expected to
     * occasionally fail and recover (see {@code MongoEventStore.reservePositions}), and is small next to the
     * half-a-lease-time gap before the next refresh round runs regardless of the backoff configured.
     */
    private static final int CAPPED_MAX_ATTEMPTS = 5;

    private final Duration leaseTime;
    private final ScheduledRefresh scheduledRefresh;
    private final ConcurrentMap<CompetingConsumer, Status> competingConsumers;
    /**
     * The token of the last lease this instance was granted, per subscription id. Kept after the lease is lost,
     * released or unregistered, since that token is the one a handler still running from that lease has to write
     * with. See {@link #fencingToken(String)}.
     */
    private final ConcurrentMap<String, Long> lastHeldFencingTokens = new ConcurrentHashMap<>();
    private final Set<CompetingConsumerListener> competingConsumerListeners;
    private final RetryStrategy retryStrategy;
    /**
     * {@link #retryStrategy}, allowing at most {@link #CAPPED_MAX_ATTEMPTS} attempts per MongoDB call. A strategy
     * already configured with fewer keeps its own limit, since this lowers a limit and never raises one. Used by
     * {@link #scheduleRefresh}, by the calls a refresh round makes through {@link #refreshOne}, and by
     * {@link #giveUpLease}. Registering uses {@link #retryStrategy} itself.
     */
    private final RetryStrategy cappedRetryStrategy;
    // Reading a consumer's status, making the MongoDB call that status decides, and writing the result back is one
    // step per consumer, and this is what makes it one. Striped rather than a map from consumer to lock, which has no
    // safe moment to drop an entry from, and rather than one lock for the whole instance, which would make
    // registering one subscription wait for another subscription's round trip.
    //
    // None of this coordinates nodes. The lease document does that. This keeps one node's view of its own consumers
    // honest about the calls that node made.
    private final ReentrantLock[] consumerLocks;

    private volatile boolean running;

    /**
     * Handed to every retried MongoDB call as its shutdown predicate, so a call that is backing off between
     * attempts stops as soon as {@link #shutdown()} runs instead of sleeping out the rest of its backoff first.
     */
    private final Predicate<Throwable> whileRunning = __ -> running;

    public MongoLeaseCompetingConsumerStrategySupport(Duration leaseTime, RetryStrategy retryStrategy) {
        this(leaseTime, retryStrategy, ScheduledRefresh.auto());
    }

    /**
     * Takes the {@link ScheduledRefresh} rather than building one, so that a test can hold the refresh itself and run
     * it when it chooses, which is what makes the scheduled refresh itself testable without any real time passing.
     * A lease's own timing is a different matter: {@code expiresAt} is judged against the database's clock, not an
     * injectable one, so a test that needs a lease to look expired seeds the lock document directly instead.
     * <p>
     * Package-private on purpose. {@code ScheduledRefresh} is not public, and neither strategy's builder exposes a
     * refresh schedule, so this widens nothing a user can reach.
     */
    MongoLeaseCompetingConsumerStrategySupport(Duration leaseTime, RetryStrategy retryStrategy, ScheduledRefresh scheduledRefresh) {
        this.leaseTime = leaseTime;
        this.scheduledRefresh = scheduledRefresh;
        this.running = true;
        this.competingConsumerListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.competingConsumers = new ConcurrentHashMap<>();
        this.consumerLocks = new ReentrantLock[CONSUMER_LOCKS];
        for (int i = 0; i < CONSUMER_LOCKS; i++) {
            this.consumerLocks[i] = new ReentrantLock();
        }

        this.retryStrategy = retryStrategy;
        this.cappedRetryStrategy = allowingAtMost(retryStrategy, CAPPED_MAX_ATTEMPTS);
        if (!(retryStrategy instanceof RetryImpl) && !(retryStrategy instanceof RetryStrategy.DontRetry)) {
            log.warn("{} runs its own retry loop, so shutdown() cannot stop a MongoDB call that is between attempts "
                    + "and neither the attempt cap nor the shutdown check below applies to it.", retryStrategy.getClass().getName());
        }
    }


    /**
     * {@code retryStrategy} allowing at most {@code maxAttempts} attempts, or {@code retryStrategy} unchanged when
     * it already allows fewer. Calling {@code maxAttempts} outright would raise a caller's own lower limit, which
     * would make a shutdown wait for attempts the caller asked not to make.
     */
    private static RetryStrategy allowingAtMost(RetryStrategy retryStrategy, int maxAttempts) {
        if (!(retryStrategy instanceof RetryImpl retry)) {
            return retryStrategy;
        }
        boolean alreadyLower = retry.configuredMaxAttempts() instanceof MaxAttempts.Limit limit && limit.limit() <= maxAttempts;
        return alreadyLower ? retryStrategy : retry.maxAttempts(maxAttempts);
    }

    public MongoLeaseCompetingConsumerStrategySupport scheduleRefresh(Function<Consumer<MongoCollection<BsonDocument>>, Runnable> fn) {
        final RetryStrategy retryStrategyToUse;
        if (cappedRetryStrategy instanceof Retry retry) {
            retryStrategyToUse = retry.onError((info, t) -> {
                final String retryMessage;
                if (info.isRetryable()) {
                    long millisToNextRetry = info.getBackoffBeforeNextRetryAttempt().orElse(Duration.ZERO).toMillis();
                    retryMessage = "will retry in %d ms".formatted(millisToNextRetry);
                } else {
                    retryMessage = "will not retry again";
                }
                logDebug("Failed to execute scheduleRefresh due to {} - {} ({})", t.getClass().getName(), t.getMessage(), retryMessage, t);
            });
        } else {
            retryStrategyToUse = cappedRetryStrategy;
        }

        scheduledRefresh.scheduleInBackground(() -> {
            // Empty until an attempt of this round fails, and then the consumers the next attempt refreshes again
            AtomicReference<@Nullable Set<CompetingConsumer>> failedInThisRound = new AtomicReference<>();
            try {
                retryStrategyToUse.execute(() -> fn.apply(collection -> refreshOrAcquireLease(collection, failedInThisRound)).run(), whileRunning);
            } catch (Exception e) {
                // scheduleAtFixedRate cancels every later execution once one throws, so a round that exhausted
                // cappedRetryStrategy is caught here instead of taking the whole schedule down with it.
                log.warn("Refresh round gave up due to {} - {}. The next scheduled round will try again.",
                        e.getClass().getName(), e.getMessage(), e);
            }
        }, leaseTime);
        return this;
    }

    public boolean registerCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");

        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        Outcome outcome = inConsumerLock(competingConsumer, () -> acquireLease(collection, competingConsumer, competingConsumers.get(competingConsumer), retryStrategy));
        notifyListeners(outcome, subscriptionId, subscriberId);
        return outcome.acquired();
    }

    public void unregisterCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        logDebug("Unregistering consumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);

        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        Outcome outcome = inConsumerLock(competingConsumer, () -> giveUpLease(collection, competingConsumer, competingConsumers.remove(competingConsumer)));
        notifyListeners(outcome, subscriptionId, subscriberId);
    }

    /**
     * Give up the lease this subscriber holds, while keeping it a candidate for the lease. The scheduled refresh takes
     * it back on its own if nobody else has taken it in the meantime, which is what a subscription paused by the system
     * rather than by a user rests on, since nothing will explicitly resume it.
     */
    public void releaseCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        logDebug("Releasing consumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);

        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        Outcome outcome = inConsumerLock(competingConsumer, () -> {
            Status status = competingConsumers.get(competingConsumer);
            if (status != null && status.isLockAcquired()) {
                // A release keeps the consumer in the map, so it stays a candidate, but the lease below is about to go
                // and it does not have it any more. Leaving the status alone would make hasLock answer yes for a
                // subscriber that no longer receives events, and would make the next refresh find its commit rejected
                // and report the loss a second time.
                //
                // LOCK_RELEASED rather than LOCK_NOT_ACQUIRED so that the next refresh stands this consumer down for
                // one round instead of racing straight back for the lease it just gave up. Nothing guarantees a rival
                // wins that race, since both refresh on their own schedule, but a consumer that gave the lease up and
                // took it back before anybody else had a chance to look has not given it up in any useful sense.
                competingConsumers.put(competingConsumer, Status.LOCK_RELEASED);
            }
            return giveUpLease(collection, competingConsumer, status);
        });
        notifyListeners(outcome, subscriptionId, subscriberId);
    }

    /**
     * Take the lease, or refresh one already held, and work out what that changed. The status the consumer had comes
     * from the caller, which has already read it under the same lock. {@code retryStrategyToUse} is
     * {@link #retryStrategy} from {@link #registerCompetingConsumer} and {@link #cappedRetryStrategy} from a
     * refresh round competing for a lease nobody holds yet, since the two callers keep different retry behaviour.
     */
    private Outcome acquireLease(MongoCollection<BsonDocument> collection, CompetingConsumer competingConsumer, @Nullable Status oldStatus, RetryStrategy retryStrategyToUse) {
        String subscriptionId = competingConsumer.subscriptionId;
        String subscriberId = competingConsumer.subscriberId;
        Optional<ListenerLock> lock = MongoListenerLockService.acquireOrRefreshFor(collection, retryStrategyToUse, whileRunning, leaseTime, subscriptionId, subscriberId);
        boolean acquired = lock.isPresent();
        boolean oldStatusWasAcquired = oldStatus != null && oldStatus.isLockAcquired();
        logDebug("acquireLease: oldStatus={} acquired lock={} (subscriberId={}, subscriptionId={})", oldStatus, acquired, subscriberId, subscriptionId);
        competingConsumers.put(competingConsumer, acquired ? Status.lockAcquired(lock.get().version()) : Status.LOCK_NOT_ACQUIRED);
        lock.ifPresent(l -> lastHeldFencingTokens.merge(subscriptionId, l.version(), Math::max));
        if (!oldStatusWasAcquired && acquired) {
            return new Outcome(true, Notification.GRANTED);
        } else if (oldStatusWasAcquired && !acquired) {
            return new Outcome(false, Notification.PROHIBITED);
        }
        return new Outcome(acquired, Notification.NONE);
    }

    /**
     * Drop the lease in MongoDB and work out what that changed. What happens to the consumer's own entry differs
     * between unregistering and releasing and has been decided by the caller.
     * <p>
     * Uses {@link #cappedRetryStrategy}, so a removal that keeps failing gives up instead of holding the caller
     * open. That constant says why giving up is safe here.
     */
    private Outcome giveUpLease(MongoCollection<BsonDocument> collection, CompetingConsumer competingConsumer, @Nullable Status status) {
        String subscriptionId = competingConsumer.subscriptionId;
        String subscriberId = competingConsumer.subscriberId;
        if (status == null) {
            logDebug("Failed to find consumer status (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return Outcome.NOTHING;
        }
        MongoListenerLockService.remove(collection, cappedRetryStrategy, whileRunning, subscriptionId, subscriberId);
        if (status.isLockAcquired()) {
            logDebug("Lock status was {}, will invoke onConsumeProhibited for listeners (subscriberId={}, subscriptionId={})", status, subscriberId, subscriptionId);
            return new Outcome(false, Notification.PROHIBITED);
        }
        logDebug("Lock status was {}, will NOT invoke onConsumeProhibited for listeners (subscriberId={}, subscriptionId={})", status, subscriberId, subscriptionId);
        return Outcome.NOTHING;
    }

    public boolean hasLock(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        Status status = competingConsumers.get(new CompetingConsumer(subscriptionId, subscriberId));
        boolean hasLock = status != null && status.isLockAcquired();
        logDebug("hasLock={} (subscriberId={}, subscriptionId={})", hasLock, subscriberId, subscriptionId);
        return hasLock;
    }

    /**
     * The fencing token for the given subscription. Answers empty while more than one consumer is registered for
     * {@code subscriptionId} in this instance, whatever their status. Otherwise it answers with the token
     * of the lease the one registered consumer holds, or, when this instance holds no lease for the subscription
     * right now, with the token of the last lease it held. A handler that started under a lease and finishes after
     * this instance lost, released or unregistered it writes with that older token, which a write from the next
     * holder has already moved past. Empty when this instance never held a lease for the subscription.
     * <p>
     * Reads the in-memory maps only, so this neither blocks nor reaches MongoDB, which a call on the per-event
     * write path requires.
     */
    public OptionalLong fencingToken(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Status onlyStatus = null;
        int registered = 0;
        for (Map.Entry<CompetingConsumer, Status> entry : competingConsumers.entrySet()) {
            if (entry.getKey().subscriptionId.equals(subscriptionId)) {
                registered++;
                if (registered > 1) {
                    return OptionalLong.empty();
                }
                onlyStatus = entry.getValue();
            }
        }
        if (registered == 1 && onlyStatus.isLockAcquired()) {
            return onlyStatus.fencingToken();
        }
        Long lastHeld = lastHeldFencingTokens.get(subscriptionId);
        return lastHeld == null ? OptionalLong.empty() : OptionalLong.of(lastHeld);
    }

    public void addListener(CompetingConsumerListener listenerConsumer) {
        Objects.requireNonNull(listenerConsumer, CompetingConsumerListener.class.getSimpleName() + " cannot be null");
        competingConsumerListeners.add(listenerConsumer);
    }

    public void removeListener(CompetingConsumerListener listenerConsumer) {
        Objects.requireNonNull(listenerConsumer, CompetingConsumerListener.class.getSimpleName() + " cannot be null");
        competingConsumerListeners.remove(listenerConsumer);
    }

    public void shutdown() {
        logDebug("Shutting down");
        running = false;
        scheduledRefresh.close();
    }

    /**
     * One attempt at a refresh round. The first attempt refreshes every consumer, and a retry of the round refreshes
     * only the consumers in {@code failedInThisRound}. A consumer whose refresh fails does not stop the others from
     * being refreshed, since one lease MongoDB refuses to write must not let every other lease on this instance
     * expire. Once every consumer has had its turn, the first failure is thrown with the others attached as
     * suppressed, so the round's retry strategy gives a failing consumer the same attempts it gave the whole round
     * before. What a round changed reaches the listeners through {@link ScheduledRefresh#notifyInBackground}, so a
     * listener that blocks, which pausing or resuming a subscription while the database does not answer does, holds up
     * the later notifications for that subscription, but never those for another one or the next refresh round.
     */
    private void refreshOrAcquireLease(MongoCollection<BsonDocument> collection, AtomicReference<@Nullable Set<CompetingConsumer>> failedInThisRound) {
        logDebug("In refreshOrAcquireLease with {} competing consumers", competingConsumers.size());
        Set<CompetingConsumer> toRefresh = failedInThisRound.get();
        Set<CompetingConsumer> failed = new HashSet<>();
        @Nullable RuntimeException firstFailure = null;
        for (CompetingConsumer cc : competingConsumers.keySet()) {
            if (toRefresh != null && !toRefresh.contains(cc)) {
                continue;
            }
            final Outcome outcome;
            try {
                outcome = inConsumerLock(cc, () -> refreshOne(collection, cc));
            } catch (RuntimeException e) {
                logDebug("Failed to refresh the lease due to {} - {}, refreshing the other consumers before this one is tried again (subscriberId={}, subscriptionId={})",
                        e.getClass().getName(), e.getMessage(), cc.subscriberId, cc.subscriptionId);
                failed.add(cc);
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
                continue;
            }
            if (outcome.notification() != Notification.NONE) {
                scheduledRefresh.notifyInBackground(cc.subscriptionId, () -> notifyListenersIfStillTrue(outcome, cc));
            }
        }
        failedInThisRound.set(failed);
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    /**
     * Runs later than the round that decided it, on the notifier, and by then the consumer may have moved on.
     * A grant for a consumer that no longer holds the lock is dropped, since starting the subscription would leave it
     * running on an instance without its lease. A prohibition is always delivered, also to a consumer that holds the
     * lock again. The listener then pauses the subscription and gives the lease up, and a later round grants it again.
     * Dropping it could leave this instance refreshing a lease for a subscription that has stopped delivering, which
     * no grant would restart, since the listener finds the consumer running already, and which no other instance
     * could take over. A listener that throws is logged, and the other listeners are told anyway.
     */
    private void notifyListenersIfStillTrue(Outcome outcome, CompetingConsumer cc) {
        if (!running) {
            return;
        }
        boolean holdsTheLockNow = hasLock(cc.subscriptionId, cc.subscriberId);
        boolean stillTrue = switch (outcome.notification()) {
            case GRANTED -> holdsTheLockNow;
            case PROHIBITED -> true;
            case NONE -> false;
        };
        if (!stillTrue) {
            logDebug("Dropping {} since the lock status changed before it was delivered (subscriberId={}, subscriptionId={})", outcome.notification(), cc.subscriberId, cc.subscriptionId);
            return;
        }
        for (CompetingConsumerListener listener : competingConsumerListeners) {
            try {
                if (outcome.notification() == Notification.GRANTED) {
                    listener.onConsumeGranted(cc.subscriptionId, cc.subscriberId);
                } else {
                    listener.onConsumeProhibited(cc.subscriptionId, cc.subscriberId);
                }
            } catch (RuntimeException e) {
                log.warn("Listener {} failed on {} due to {} - {} (subscriberId={}, subscriptionId={})",
                        listener, outcome.notification(), e.getClass().getName(), e.getMessage(), cc.subscriberId, cc.subscriptionId, e);
            }
        }
    }

    private Outcome refreshOne(MongoCollection<BsonDocument> collection, CompetingConsumer cc) {
        // Read again rather than trust what the iteration handed over, which it read without this lock. A consumer
        // unregistered in between would otherwise be written back here, and then nothing is left to unregister it
        // and no node can take that subscription over.
        Status status = competingConsumers.get(cc);
        logDebug("Status {} (subscriberId={}, subscriptionId={})", status, cc.subscriberId, cc.subscriptionId);
        if (status == null) {
            logDebug("Consumer is no longer registered, skipping it this round (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
            return Outcome.NOTHING;
        }
        return switch (status.kind()) {
            case LOCK_ACQUIRED -> {
                // Uses cappedRetryStrategy, so a commit that keeps failing gives up here instead of holding this
                // round open until MongoDB answers again. The lock document is untouched by a call that never got
                // through, so the consumer keeps its lease and the next round commits what it missed.
                boolean stillHasLock = MongoListenerLockService.commit(collection, cappedRetryStrategy, whileRunning, leaseTime, cc.subscriptionId, cc.subscriberId);
                if (stillHasLock) {
                    yield Outcome.NOTHING;
                }
                logDebug("Lost lock! (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
                competingConsumers.put(cc, Status.LOCK_NOT_ACQUIRED);
                yield new Outcome(false, Notification.PROHIBITED);
            }
            // The round this consumer stands down for after releasing. It is an ordinary candidate again from the
            // next round on, and nothing is reported here. It neither holds the lease nor has just stopped holding
            // it, and the release already told the listeners about that.
            case LOCK_RELEASED -> {
                logDebug("Consumer stood down for this round after releasing (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
                competingConsumers.put(cc, Status.LOCK_NOT_ACQUIRED);
                yield Outcome.NOTHING;
            }
            case LOCK_NOT_ACQUIRED -> acquireLease(collection, cc, status, cappedRetryStrategy);
        };
    }

    private Outcome inConsumerLock(CompetingConsumer competingConsumer, Supplier<Outcome> action) {
        ReentrantLock lock = consumerLocks[Math.floorMod(competingConsumer.hashCode(), consumerLocks.length)];
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Tell the listeners what the last step changed, and never while holding that consumer's lock. A listener runs
     * straight into the subscription model, which is synchronized on itself and calls back into this class from those
     * callbacks, while an application thread pausing or registering holds that same monitor before it arrives here.
     * Notifying under the lock closes that cycle, and the refresh thread and the application thread deadlock.
     * <p>
     * Every listener is told, also when one before it throws. The first failure is thrown once they all have been,
     * with any later one attached as suppressed.
     */
    private void notifyListeners(Outcome outcome, String subscriptionId, String subscriberId) {
        if (outcome.notification() == Notification.NONE) {
            return;
        }
        logDebug("Consumption {} (subscriberId={}, subscriptionId={})", outcome.notification(), subscriberId, subscriptionId);
        @Nullable RuntimeException firstFailure = null;
        for (CompetingConsumerListener listener : competingConsumerListeners) {
            try {
                if (outcome.notification() == Notification.GRANTED) {
                    listener.onConsumeGranted(subscriptionId, subscriberId);
                } else {
                    listener.onConsumeProhibited(subscriptionId, subscriberId);
                }
            } catch (RuntimeException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        logDebug("Completed telling every listener of {} (subscriberId={}, subscriptionId={})", outcome.notification(), subscriberId, subscriptionId);
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private record CompetingConsumer(String subscriptionId, String subscriberId) {
    }

    /**
     * What a step did to the lease, and what the listeners have to be told about it once the consumer's lock is out
     * of the way. Only registering has a use for {@code acquired}, and it has to come from inside the lock, since
     * reading the status again afterwards is the very thing the lock is here to stop.
     */
    private record Outcome(boolean acquired, Notification notification) {
        private static final Outcome NOTHING = new Outcome(false, Notification.NONE);
    }

    private enum Notification {
        GRANTED, PROHIBITED, NONE
    }

    /**
     * A consumer's status, with its fencing token for the acquired case. The token stays exactly as it was
     * while this status remains {@code LOCK_ACQUIRED}, since a refresh (see {@code refreshOne}) commits
     * without touching the map entry, and a lost commit replaces the whole status with {@code LOCK_NOT_ACQUIRED}
     * rather than updating the token in place. The token itself outlives the status in
     * {@link #lastHeldFencingTokens}, and that stale token is what a fence built on {@link #fencingToken(String)}
     * refuses once the next holder has written.
     */
    private record Status(Kind kind, OptionalLong fencingToken) {
        private static final Status LOCK_NOT_ACQUIRED = new Status(Kind.LOCK_NOT_ACQUIRED, OptionalLong.empty());
        private static final Status LOCK_RELEASED = new Status(Kind.LOCK_RELEASED, OptionalLong.empty());

        private static Status lockAcquired(long fencingToken) {
            return new Status(Kind.LOCK_ACQUIRED, OptionalLong.of(fencingToken));
        }

        private boolean isLockAcquired() {
            return kind == Kind.LOCK_ACQUIRED;
        }

        private enum Kind {
            LOCK_ACQUIRED, LOCK_NOT_ACQUIRED, LOCK_RELEASED
        }
    }

    private static void logDebug(String message, @Nullable Object... params) {
        if (log.isDebugEnabled()) {
            log.debug(message, params);
        }
    }
}