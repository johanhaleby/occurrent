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
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.Retry;
import org.occurrent.retry.internal.RetryImpl;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
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

    private final Duration leaseTime;
    private final ScheduledRefresh scheduledRefresh;
    private final ConcurrentMap<CompetingConsumer, Status> competingConsumers;
    private final Set<CompetingConsumerListener> competingConsumerListeners;
    private final RetryStrategy retryStrategy;
    // Reading a consumer's status, making the MongoDB call that status decides, and writing the result back is one
    // step per consumer, and this is what makes it one. Striped rather than a map from consumer to lock, which has no
    // safe moment to drop an entry from, and rather than one lock for the whole instance, which would make
    // registering one subscription wait for another subscription's round trip.
    //
    // None of this coordinates nodes. The lease document does that. This keeps one node's view of its own consumers
    // honest about the calls that node made.
    private final ReentrantLock[] consumerLocks;

    private volatile boolean running;


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

        if (retryStrategy instanceof RetryImpl retry) {
            this.retryStrategy = retry.mapRetryPredicate(currentPredicate -> currentPredicate.and(__ -> running));
        } else {
            this.retryStrategy = retryStrategy;
        }
    }


    public MongoLeaseCompetingConsumerStrategySupport scheduleRefresh(Function<Consumer<MongoCollection<BsonDocument>>, Runnable> fn) {
        final RetryStrategy retryStrategyToUse;
        if (retryStrategy instanceof Retry retry) {
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
            retryStrategyToUse = retryStrategy;
        }

        scheduledRefresh.scheduleInBackground(() -> retryStrategyToUse.execute(() -> fn.apply(this::refreshOrAcquireLease).run()), leaseTime);
        return this;
    }

    public boolean registerCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");

        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        Outcome outcome = inConsumerLock(competingConsumer, () -> acquireLease(collection, competingConsumer, competingConsumers.get(competingConsumer)));
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
     * from the caller, which has already read it under the same lock.
     */
    private Outcome acquireLease(MongoCollection<BsonDocument> collection, CompetingConsumer competingConsumer, @Nullable Status oldStatus) {
        String subscriptionId = competingConsumer.subscriptionId;
        String subscriberId = competingConsumer.subscriberId;
        Optional<ListenerLock> lock = MongoListenerLockService.acquireOrRefreshFor(collection, retryStrategy, leaseTime, subscriptionId, subscriberId);
        boolean acquired = lock.isPresent();
        boolean oldStatusWasAcquired = oldStatus != null && oldStatus.isLockAcquired();
        logDebug("acquireLease: oldStatus={} acquired lock={} (subscriberId={}, subscriptionId={})", oldStatus, acquired, subscriberId, subscriptionId);
        competingConsumers.put(competingConsumer, acquired ? Status.lockAcquired(lock.get().version()) : Status.LOCK_NOT_ACQUIRED);
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
     */
    private Outcome giveUpLease(MongoCollection<BsonDocument> collection, CompetingConsumer competingConsumer, @Nullable Status status) {
        String subscriptionId = competingConsumer.subscriptionId;
        String subscriberId = competingConsumer.subscriberId;
        if (status == null) {
            logDebug("Failed to find consumer status (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return Outcome.NOTHING;
        }
        MongoListenerLockService.remove(collection, retryStrategy, subscriptionId, subscriberId);
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
     * The fencing token for the given subscription. Answers with a value only when exactly one consumer is
     * registered for {@code subscriptionId} in this instance and that consumer holds the lock, whatever its
     * status otherwise is (ADR 116). {@code LOCK_RELEASED} counts as not holding, since its token belongs to
     * the lease it just gave up.
     * <p>
     * Reads the in-memory map only, so this neither blocks nor reaches MongoDB, which a call on the per-event
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
        return registered == 1 && onlyStatus.isLockAcquired() ? onlyStatus.fencingToken() : OptionalLong.empty();
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

    private void refreshOrAcquireLease(MongoCollection<BsonDocument> collection) {
        logDebug("In refreshOrAcquireLease with {} competing consumers", competingConsumers.size());
        competingConsumers.forEach((cc, __) -> {
            Outcome outcome = inConsumerLock(cc, () -> refreshOne(collection, cc));
            notifyListeners(outcome, cc.subscriptionId, cc.subscriberId);
        });
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
                boolean stillHasLock = MongoListenerLockService.commit(collection, retryStrategy, leaseTime, cc.subscriptionId, cc.subscriberId);
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
            case LOCK_NOT_ACQUIRED -> acquireLease(collection, cc, status);
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
     */
    private void notifyListeners(Outcome outcome, String subscriptionId, String subscriberId) {
        switch (outcome.notification()) {
            case GRANTED -> {
                logDebug("Consumption granted (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                competingConsumerListeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
                logDebug("Completed calling onConsumeGranted for all listeners (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            }
            case PROHIBITED -> {
                logDebug("Consumption prohibited (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
                logDebug("Completed calling onConsumeProhibited for all listeners (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            }
            case NONE -> {
            }
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
     * rather than updating the token in place. That staleness is deliberate. The stale token is what a fence
     * built on {@link #fencingToken(String)} refuses.
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