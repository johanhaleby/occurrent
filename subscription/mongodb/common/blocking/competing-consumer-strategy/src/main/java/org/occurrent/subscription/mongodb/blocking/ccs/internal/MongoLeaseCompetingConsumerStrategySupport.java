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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Common operations for MongoDB lease-based competing consumer strategies.
 * <p>
 * Each consumer keeps a record of what its listeners were last told, next to the status of its lease. The record
 * changes only once the listeners have been told, so a throwable anywhere in a refresh round or in telling the
 * listeners, an {@link OutOfMemoryError} included, leaves the record behind the status. Every refresh round compares
 * the two for each consumer and has the listeners told the difference, so a grant or a prohibition that a throwable
 * cut short is told by a later round instead of being lost.
 */
@NullMarked
public class MongoLeaseCompetingConsumerStrategySupport {
    private static final Logger log = LoggerFactory.getLogger(MongoLeaseCompetingConsumerStrategySupport.class);
    private static final FailureLogging listenerThrewError = new FailureLogging(log, "Listener {} failed on {} (subscriberId={}, subscriptionId={}) due to {} - {}. The next refresh round tells it again.");
    private static final FailureLogging listenerThrew = new FailureLogging(log, "Listener {} failed on {} (subscriberId={}, subscriptionId={}) due to {} - {}");
    private static final FailureLogging tellingFailed = new FailureLogging(log, "Telling the listeners of subscription {} failed due to {} - {}. The next refresh round tells them again.");
    private static final FailureLogging comparingFailed = new FailureLogging(log, "Working out what to tell the listeners of subscription {} failed due to {} - {}. The next refresh round tries again.");

    // What the locked step of a register, release or unregister changed, as a long so it allocates nothing. A grant is
    // the consumer's lapse count after the step and a prohibition is -3 less its loss count, so no two ranges meet.
    static final long OWN_NOTHING = -1;
    static final long OWN_STILL_HELD = -2;

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
     * cannot reach, to delete a document that is about to stop mattering anyway. An unregister whose removal gives
     * up has the next refresh round try it again, while the instance still runs.
     * <p>
     * Registering is the one call with nothing covering a failure, so it keeps retrying exactly as configured.
     * 5 attempts matches the cap this codebase already uses elsewhere for a MongoDB call that is expected to
     * occasionally fail and recover (see {@code MongoEventStore.reservePositions}), and is small next to the
     * half-a-lease-time gap before the next refresh round runs regardless of the backoff configured.
     */
    private static final int CAPPED_MAX_ATTEMPTS = 5;

    /**
     * The part of the lease time this node gives up at the end of each lease, so it stops treating a lease as its own a
     * quarter of the lease time before MongoDB can expire it, 5 seconds with the default lease time.
     * <p>
     * MongoDB decides that a lease has expired by its own clock, while this node can only count the time since it sent
     * the request that set the lease by its own. The two disagree when the clocks tick at different rates, when the
     * primary that set the lease fails over to a member whose clock is ahead of it, and when a server's clock is set
     * forward. A difference in rate is small, since a clock that NTP is correcting runs at most 500 parts per million fast,
     * 10 milliseconds over the default 20 seconds. The other two depend only on how closely the deployment keeps
     * its clocks in step, and that is what the margin is for.
     * <p>
     * A quarter splits the half lease time between a refresh round and the end of the lease evenly. A round starts every
     * half lease time, so a node whose refreshes go through stays a quarter of the lease time clear of the margin, which
     * a slow round, a round retrying a failed call, or a round held up behind another consumer's call can use up before
     * delivery pauses. The other quarter is the margin. A larger margin would pause delivery on a node whose rounds are
     * merely slow, and a smaller one would rely on the clocks agreeing more closely.
     */
    private static final int LEASE_TIME_PARTS_GIVEN_UP = 4;

    private final Duration leaseTime;
    /**
     * How long after {@link ListenerLock#sentAt()} this node treats a lease as its own, the lease time less
     * {@link #LEASE_TIME_PARTS_GIVEN_UP its margin}, in the units of {@link #clock}.
     */
    private final long heldForNanos;
    private final LongSupplier clock;
    private final ScheduledRefresh scheduledRefresh;
    private final ConcurrentMap<CompetingConsumer, ConsumerState> competingConsumers;
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
        this(leaseTime, retryStrategy, scheduledRefresh, System::nanoTime);
    }

    /**
     * Takes the clock that times each lease from when the request that set it was sent, so a test can let a lease
     * run out on this node without waiting for it. Package-private for the same reason as the constructor above.
     */
    MongoLeaseCompetingConsumerStrategySupport(Duration leaseTime, RetryStrategy retryStrategy, ScheduledRefresh scheduledRefresh, LongSupplier clock) {
        this.leaseTime = leaseTime;
        this.heldForNanos = leaseTime.toNanos() - leaseTime.toNanos() / LEASE_TIME_PARTS_GIVEN_UP;
        this.clock = clock;
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
                // A round that exhausted cappedRetryStrategy. Anything else it throws, an Error for instance, is logged
                // by ScheduledRefresh, which also keeps the later rounds running.
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
        ConsumerState[] registered = new ConsumerState[1];
        long own = inConsumerLock(competingConsumer, () -> {
            ConsumerState mapped = competingConsumers.get(competingConsumer);
            Optional<ListenerLock> lock = MongoListenerLockService.acquireOrRefreshFor(collection, retryStrategy, whileRunning, leaseTime, subscriptionId, subscriberId, clock);
            // Reused when mapped, so a prohibition its record still owes is not lost with it. Put only once its status is
            // set, since fencingToken reads the status of every consumer in the map.
            ConsumerState c = mapped == null ? new ConsumerState(competingConsumer) : mapped;
            registered[0] = c;
            long step = recordLease(c, mapped == null ? null : mapped.status, lock);
            if (mapped == null) {
                competingConsumers.put(competingConsumer, c);
            }
            return step;
        });
        tellOwnStep(registered[0], own);
        return own >= 0 || own == OWN_STILL_HELD;
    }

    public void unregisterCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        logDebug("Unregistering consumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);

        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        ConsumerState[] unregistered = new ConsumerState[1];
        long own = inConsumerLock(competingConsumer, () -> {
            ConsumerState c = competingConsumers.get(competingConsumer);
            if (c == null) {
                return giveUpLease(collection, competingConsumer, null, null);
            }
            unregistered[0] = c;
            // Kept as UNREGISTERING, which hasLock answers no for and which no round extends, until the lease is
            // removed and the listeners have been told the prohibition. Forgetting it first would leave the lease in
            // place until it expired, or a prohibition the listeners were never told.
            Status status = c.status;
            if (status.isLockAcquired()) {
                c.lose();
            }
            c.status = Status.UNREGISTERING;
            long step = giveUpLease(collection, competingConsumer, c, status);
            forgetIfNothingIsOwed(c);
            return step;
        });
        ConsumerState c = unregistered[0];
        if (c != null) {
            tellOwnStep(c, own);
            inConsumerLock(competingConsumer, () -> {
                forgetIfNothingIsOwed(c);
                return OWN_NOTHING;
            });
        }
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
        ConsumerState[] released = new ConsumerState[1];
        long own = inConsumerLock(competingConsumer, () -> {
            ConsumerState c = competingConsumers.get(competingConsumer);
            released[0] = c;
            Status status = c == null ? null : c.status;
            if (c != null && status.isLockAcquired()) {
                // A release keeps the consumer in the map, so it stays a candidate, but the lease below is about to go
                // and it does not have it any more. Leaving the status alone would make hasLock answer yes for a
                // subscriber that no longer receives events.
                //
                // LOCK_RELEASED rather than LOCK_NOT_ACQUIRED so that the next refresh stands this consumer down for
                // one round instead of racing straight back for the lease it just gave up. Nothing guarantees a rival
                // wins that race, since both refresh on their own schedule, but a consumer that gave the lease up and
                // took it back before anybody else had a chance to look has not given it up in any useful sense.
                c.lose();
                c.status = Status.LOCK_RELEASED;
            }
            return giveUpLease(collection, competingConsumer, c, status);
        });
        if (released[0] != null) {
            tellOwnStep(released[0], own);
        }
    }

    /**
     * Record what a lease call for {@code c} returned, and what that changed. {@code oldStatus} is the status {@code c}
     * had, read under the same lock, and {@code null} for a consumer that is not in the map yet.
     */
    private long recordLease(ConsumerState c, @Nullable Status oldStatus, Optional<ListenerLock> lock) {
        boolean oldStatusWasAcquired = oldStatus != null && oldStatus.isLockAcquired();
        logDebug("acquireLease: oldStatus={} acquired lock={} (subscriberId={}, subscriptionId={})", oldStatus, lock.isPresent(), c.key.subscriberId, c.key.subscriptionId);
        if (lock.isPresent()) {
            long token = lock.get().version();
            // Before the status, so fencingToken never answers with an older token once this lease is lost
            lastHeldFencingTokens.merge(c.key.subscriptionId, token, Math::max);
            if (oldStatusWasAcquired && oldStatus.fencingToken().getAsLong() != token) {
                c.lose();
            }
            c.status = Status.lockAcquired(token, lock.get().sentAt());
            return oldStatusWasAcquired ? OWN_STILL_HELD : c.lapses;
        }
        if (oldStatusWasAcquired) {
            c.lose();
            c.status = Status.LOCK_NOT_ACQUIRED;
            return -3 - c.losses;
        }
        c.status = Status.LOCK_NOT_ACQUIRED;
        return OWN_NOTHING;
    }

    /**
     * Drop the lease in MongoDB and work out what that changed. What happens to the consumer's own entry differs
     * between unregistering and releasing and has been decided by the caller.
     * <p>
     * Uses {@link #cappedRetryStrategy}, so a removal that keeps failing gives up instead of holding the caller
     * open. That constant says why giving up is safe here.
     */
    private long giveUpLease(MongoCollection<BsonDocument> collection, CompetingConsumer competingConsumer, @Nullable ConsumerState c, @Nullable Status status) {
        String subscriptionId = competingConsumer.subscriptionId;
        String subscriberId = competingConsumer.subscriberId;
        if (c == null || status == null) {
            logDebug("Failed to find consumer status (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return OWN_NOTHING;
        }
        MongoListenerLockService.remove(collection, cappedRetryStrategy, whileRunning, subscriptionId, subscriberId);
        logDebug("Gave up the lease, status was {} (subscriberId={}, subscriptionId={})", status, subscriberId, subscriptionId);
        return status.isLockAcquired() ? -3 - c.losses : OWN_NOTHING;
    }

    // Under the consumer's lock. Unregistering forgets a consumer only once the listeners have nothing more to be told
    // about it.
    private void forgetIfNothingIsOwed(ConsumerState c) {
        if (c.status.kind() == Status.Kind.UNREGISTERING && c.told instanceof NotGranted notGranted && notGranted.lossesSeen() == c.losses && !c.telling) {
            competingConsumers.remove(c.key, c);
        }
    }

    /**
     * Whether this node holds the lease and the lease cannot have expired yet. That is the case for
     * {@link #LEASE_TIME_PARTS_GIVEN_UP three quarters of the lease time} after the request that last acquired or
     * refreshed it was sent, and no longer, also while every refresh since has failed, since a refresh that never got
     * through tells this node nothing about whether another node has taken the lease since. Reads memory only, so it
     * neither blocks nor reaches MongoDB.
     * <p>
     * This node times the lease by its monotonic clock and MongoDB expires it by its own, so the answer is right only
     * while MongoDB's clock gains less than a quarter of the lease time on this node's over one lease. After a failover
     * to a primary whose clock is further ahead than that, or a server clock set forward by more than that, this can
     * answer {@code true} after another node has taken the lease.
     */
    public boolean hasLock(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        CompetingConsumer competingConsumer = new CompetingConsumer(subscriptionId, subscriberId);
        ConsumerState c = competingConsumers.get(competingConsumer);
        Status status = c == null ? null : c.status;
        boolean hasLock = status != null && status.isLockAcquired() && cannotHaveExpired(c, status);
        logDebug("hasLock={} (subscriberId={}, subscriptionId={})", hasLock, subscriberId, subscriptionId);
        return hasLock;
    }

    // A listener may act on a no from here, such as pausing the subscription, so counting it as a lapse makes the
    // refresh that extends the lease again tell the listeners it is granted
    private boolean cannotHaveExpired(ConsumerState c, Status acquired) {
        if (isFresh(acquired)) {
            return true;
        }
        c.lapse();
        return false;
    }

    private boolean isFresh(@Nullable Status status) {
        return status != null && status.isLockAcquired() && clock.getAsLong() - status.sentAt() < heldForNanos;
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
        for (ConsumerState c : competingConsumers.values()) {
            Status status = c.status;
            if (c.key.subscriptionId.equals(subscriptionId) && status.kind() != Status.Kind.UNREGISTERING) {
                registered++;
                if (registered > 1) {
                    return OptionalLong.empty();
                }
                onlyStatus = status;
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
     * expire. That holds for an {@link Error} too. Once every consumer has had its turn, the first Error, or the first
     * failure when none was one, is thrown with the others attached as suppressed, so the round's retry strategy gives a failing consumer the same attempts it gave the whole round
     * before.
     * <p>
     * After each consumer's refresh, also one that threw, the round compares what that consumer's listeners were last
     * told with its status, and when the two differ it hands a tell over through
     * {@link ScheduledRefresh#notifyInBackground}. A listener that blocks, which pausing or resuming a subscription
     * while the database does not answer does, holds up the later tells for its own consumer only, never those for
     * another one or the next refresh round.
     */
    private void refreshOrAcquireLease(MongoCollection<BsonDocument> collection, AtomicReference<@Nullable Set<CompetingConsumer>> failedInThisRound) {
        logDebug("In refreshOrAcquireLease with {} competing consumers", competingConsumers.size());
        Set<CompetingConsumer> toRefresh = failedInThisRound.get();
        Set<CompetingConsumer> failed = new HashSet<>();
        List<Throwable> failures = new ArrayList<>();
        for (CompetingConsumer cc : competingConsumers.keySet()) {
            if (toRefresh != null && !toRefresh.contains(cc)) {
                continue;
            }
            try {
                inConsumerLock(cc, () -> {
                    refreshOne(collection, cc);
                    return OWN_NOTHING;
                });
            } catch (Throwable e) {
                // Bookkeeping before logging, since logging allocates and can throw again
                try {
                    failed.add(cc);
                    failures.add(e);
                    logDebug("Failed to refresh the lease due to {} - {}, refreshing the other consumers before this one is tried again (subscriberId={}, subscriptionId={})",
                            e.getClass().getName(), e.getMessage(), cc.subscriberId, cc.subscriptionId);
                } catch (Throwable ignored) {
                    // The next round refreshes it
                }
            }
            handOverWhatIsDue(cc);
        }
        failedInThisRound.set(failed);
        throwTheFirstErrorOrFailure(failures);
    }

    private void handOverWhatIsDue(CompetingConsumer cc) {
        try {
            ConsumerState c = competingConsumers.get(cc);
            if (c == null) {
                return;
            }
            long lapses = c.lapses;
            long losses = c.losses;
            Status status = statusOf(c);
            if (whatToTell(c.told, status, lapses, losses, isFresh(status)) != Change.NOTHING) {
                // Set before telling is read, so a tell under way finds it once it lets go of the guard
                c.tellAgain = true;
                if (!c.telling) {
                    scheduledRefresh.notifyInBackground(cc.subscriptionId, c.tellInBackground);
                }
            }
        } catch (Throwable e) {
            comparingFailed.error(cc.subscriptionId, e);
        }
    }

    private void refreshOne(MongoCollection<BsonDocument> collection, CompetingConsumer cc) {
        // Read again rather than trust what the iteration handed over, which it read without this lock. A consumer
        // unregistered in between would otherwise be written back here, and then nothing is left to unregister it
        // and no node can take that subscription over.
        ConsumerState c = competingConsumers.get(cc);
        if (c == null) {
            logDebug("Consumer is no longer registered, skipping it this round (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
            return;
        }
        Status status = c.status;
        logDebug("Status {} (subscriberId={}, subscriptionId={})", status, cc.subscriberId, cc.subscriptionId);
        switch (status.kind()) {
            case LOCK_ACQUIRED -> {
                // Uses cappedRetryStrategy, so a commit that keeps failing gives up here instead of holding this
                // round open until MongoDB answers again. The lock document is untouched by a call that never got
                // through, so the consumer keeps its status and the next round commits what it missed. hasLock says
                // no once the lease could have expired meanwhile, see cannotHaveExpired.
                OptionalLong extended = MongoListenerLockService.commit(collection, cappedRetryStrategy, whileRunning, leaseTime, cc.subscriptionId, cc.subscriberId, clock);
                if (extended.isPresent()) {
                    c.status = Status.lockAcquired(status.fencingToken().getAsLong(), extended.getAsLong());
                } else {
                    logDebug("Lost lock! (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
                    c.lose();
                    c.status = Status.LOCK_NOT_ACQUIRED;
                }
            }
            // An unregister whose removal of the lease failed, or whose prohibition the listeners have not all heard
            case UNREGISTERING -> {
                MongoListenerLockService.remove(collection, cappedRetryStrategy, whileRunning, cc.subscriptionId, cc.subscriberId);
                forgetIfNothingIsOwed(c);
            }
            // The round this consumer stands down for after releasing. It is an ordinary candidate again from the
            // next round on.
            case LOCK_RELEASED -> {
                logDebug("Consumer stood down for this round after releasing (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
                c.status = Status.LOCK_NOT_ACQUIRED;
            }
            case LOCK_NOT_ACQUIRED -> recordLease(c, status,
                    MongoListenerLockService.acquireOrRefreshFor(collection, cappedRetryStrategy, whileRunning, leaseTime, cc.subscriptionId, cc.subscriberId, clock));
        }
    }

    private long inConsumerLock(CompetingConsumer competingConsumer, LongSupplier action) {
        ReentrantLock lock = consumerLocks[Math.floorMod(competingConsumer.hashCode(), consumerLocks.length)];
        lock.lock();
        try {
            return action.getAsLong();
        } finally {
            lock.unlock();
        }
    }

    // The status of c, or null once c is no longer the consumer mapped for its key
    private @Nullable Status statusOf(ConsumerState c) {
        return competingConsumers.get(c.key) == c ? c.status : null;
    }

    /**
     * Tell the listeners what the locked step of a register, release or unregister changed, and never while holding
     * that consumer's lock. A listener runs straight into the subscription model, which is synchronized on itself and
     * calls back into this class from those callbacks, while an application thread pausing or registering holds that
     * same monitor before it arrives here. Notifying under the lock closes that cycle, and the refresh thread and the
     * application thread deadlock.
     * <p>
     * Tells the listeners only the change {@code own} describes, and only while no other tell for the consumer is
     * under way. Anything else they have to be told, that change too when another tell is under way, goes to the
     * tell that is under way or to a tell on the notifier. Never waits for a tell under way, since that one may be
     * inside a listener that waits for the subscription model's monitor, which the caller may hold.
     * <p>
     * Every listener is told, also when one before it throws, an {@link Error} included. Once they all have been, the
     * first {@code Error} is thrown, or the first failure when none was an {@code Error}, with every other failure
     * attached as suppressed.
     */
    private void tellOwnStep(ConsumerState c, long own) {
        if (!TELLING.compareAndSet(c, false, true)) {
            c.tellAgain = true;
            if (c.telling || !TELLING.compareAndSet(c, false, true)) {
                return;
            }
        }
        try {
            c.tellAgain = false;
            tellAtMostOwnStep(c, own);
        } finally {
            c.telling = false;
            if (c.tellAgain) {
                scheduledRefresh.notifyInBackground(c.key.subscriptionId, c.tellInBackground);
            }
        }
    }

    private void tellAtMostOwnStep(ConsumerState c, long own) {
        long seenLapses = c.lapses;
        long seenLosses = c.losses;
        Status status = statusOf(c);
        Told told = c.told;
        Change change = whatToTell(told, status, seenLapses, seenLosses, isFresh(status));
        // Read again after the status. A lost lease and a new one can both land between the first read of the counts
        // and the status read, and only counts read after the status show that the change is no longer this call's.
        long lapsesNow = c.lapses;
        long lossesNow = c.losses;
        @Nullable Told dirty = null;
        @Nullable List<Throwable> failures = null;
        if (isOwn(change, own, lapsesNow, lossesNow)) {
            dirty = markTelling(c, change, status, told);
            failures = new ArrayList<>();
            tellListeners(c, change, failures);
            c.told = change == Change.GRANT ? new Granted(tokenOf(status), seenLapses) : new NotGranted(seenLosses);
            seenLapses = c.lapses;
            seenLosses = c.losses;
            status = statusOf(c);
            change = whatToTell(c.told, status, seenLapses, seenLosses, isFresh(status));
        }
        if (change != Change.NOTHING) {
            c.tellAgain = true;
        }
        if (failures != null) {
            // After the second read, so the repeat is left to the next round instead of handed over at once
            if (dirty != null && containsAnError(failures)) {
                c.told = dirty;
            }
            throwTheFirstErrorOrFailure(failures);
        }
    }

    // The tell a round, or a register, release or unregister that could not tell everything, hands to the notifier
    private void tellInBackground(ConsumerState c) {
        while (c.tellAgain && TELLING.compareAndSet(c, false, true)) {
            try {
                // Only close() is meant to interrupt this thread. An interrupt that a listener sets and does not clear
                // would make the first blocking call of the next one throw.
                Thread.interrupted();
                if (!running) {
                    return;
                }
                c.tellAgain = false;
                tellUntilNothingIsDue(c);
            } catch (Throwable t) {
                // What was left to this tell waits for the next round, which compares again
                tellingFailed.error(c.key.subscriptionId, t);
                return;
            } finally {
                c.telling = false;
            }
        }
    }

    /**
     * Tells one change at a time and works out the next one from the status after it, until nothing is due. What a
     * listener throws is logged at most once, an {@link Error} at ERROR and anything else at WARN, and not rethrown,
     * since the notifier would only log it a second time. After a listener threw an {@code Error}, the record is left
     * asking for the last change again, so the next round tells the listeners the current answer once more.
     */
    private void tellUntilNothingIsDue(ConsumerState c) {
        @Nullable Told lastDirty = null;
        boolean errorSeen = false;
        while (true) {
            long seenLapses = c.lapses;
            long seenLosses = c.losses;
            Status status = statusOf(c);
            Told told = c.told;
            Change change = whatToTell(told, status, seenLapses, seenLosses, isFresh(status));
            if (change == Change.NOTHING) {
                break;
            }
            lastDirty = markTelling(c, change, status, told);
            errorSeen |= tellListeners(c, change, null);
            c.told = change == Change.GRANT ? new Granted(tokenOf(status), seenLapses) : new NotGranted(seenLosses);
        }
        if (errorSeen && lastDirty != null) {
            c.told = lastDirty;
        }
    }

    // Leaves the record asking for this change until the listeners have all been told it, and returns that record
    private static Told markTelling(ConsumerState c, Change change, @Nullable Status status, Told told) {
        if (change == Change.PROHIBIT && told instanceof NotGranted) {
            return told;
        }
        Told dirty = new Granted(change == Change.GRANT ? tokenOf(status) : ((Granted) told).token(), UNSEEN);
        c.told = dirty;
        return dirty;
    }

    // Collects what a listener throws into failures, or logs it when there is no list. True when one threw an Error.
    private boolean tellListeners(ConsumerState c, Change change, @Nullable List<Throwable> failures) {
        String subscriptionId = c.key.subscriptionId;
        String subscriberId = c.key.subscriberId;
        logDebug("Consumption {} (subscriberId={}, subscriptionId={})", change, subscriberId, subscriptionId);
        boolean errorSeen = false;
        for (CompetingConsumerListener listener : competingConsumerListeners) {
            try {
                if (change == Change.GRANT) {
                    listener.onConsumeGranted(subscriptionId, subscriberId);
                } else {
                    listener.onConsumeProhibited(subscriptionId, subscriberId);
                }
            } catch (Throwable e) {
                errorSeen |= e instanceof Error;
                if (failures != null) {
                    failures.add(e);
                } else if (e instanceof Error) {
                    listenerThrewError.error(listener, change, subscriberId, subscriptionId, e);
                } else {
                    listenerThrew.warn(listener, change, subscriberId, subscriptionId, e);
                }
            }
        }
        return errorSeen;
    }

    private static long tokenOf(@Nullable Status acquired) {
        return Objects.requireNonNull(acquired).fencingToken().getAsLong();
    }

    private static boolean containsAnError(List<Throwable> failures) {
        for (Throwable failure : failures) {
            if (failure instanceof Error) {
                return true;
            }
        }
        return false;
    }

    // The first Error, or the first failure when none was one, with the rest attached as suppressed. Nothing when the
    // list is empty. A checked exception a listener threw without declaring it is thrown as it is.
    private static void throwTheFirstErrorOrFailure(List<Throwable> failures) {
        if (failures.isEmpty()) {
            return;
        }
        Throwable thrown = failures.stream().filter(Error.class::isInstance).findFirst().orElse(failures.getFirst());
        for (Throwable failure : failures) {
            if (failure != thrown) {
                thrown.addSuppressed(failure);
            }
        }
        MongoLeaseCompetingConsumerStrategySupport.<RuntimeException>throwAsItIs(thrown);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwAsItIs(Throwable thrown) throws T {
        throw (T) thrown;
    }

    /**
     * What the listeners have to be told about a consumer whose listeners were last told {@code told}, given its
     * {@code status}, which is {@code null} once it is no longer registered, and its two counts. {@code fresh} is
     * whether the lease cannot have expired yet. A lease lost since the listeners were told it was not granted is
     * prohibited even when its grant was never told, since a caller may have learned of it from
     * {@link #registerCompetingConsumer} or {@link #hasLock}.
     */
    static Change whatToTell(Told told, @Nullable Status status, long lapses, long losses, boolean fresh) {
        boolean acquired = status != null && status.isLockAcquired();
        if (told instanceof NotGranted notGranted) {
            if (notGranted.lossesSeen() != losses) {
                return Change.PROHIBIT;
            }
            return acquired && fresh ? Change.GRANT : Change.NOTHING;
        }
        Granted granted = (Granted) told;
        if (!acquired || status.fencingToken().getAsLong() != granted.token()) {
            return Change.PROHIBIT;
        }
        return granted.lapsesSeen() != lapses && fresh ? Change.GRANT : Change.NOTHING;
    }

    /**
     * Whether {@code change} is the one the locked step that returned {@code own} made, given the two counts read
     * after the status that {@code change} was worked out from. A grant is the step's while no lapse has happened
     * since it, and a prohibition while no lease has been lost since it.
     */
    static boolean isOwn(Change change, long own, long lapsesNow, long lossesNow) {
        return change == Change.GRANT && lapsesNow == own || change == Change.PROHIBIT && lossesNow == -3 - own;
    }

    private record CompetingConsumer(String subscriptionId, String subscriberId) {
    }

    /**
     * What the listeners of a consumer were last told. {@link Granted} with a lapse count of {@code UNSEEN} is written before the
     * first listener is told a change, so a tell that a throwable cuts short leaves a record that asks for that change
     * again.
     */
    sealed interface Told permits NotGranted, Granted {
    }

    /**
     * Told that the consumer may not consume, or never told anything, when its loss count was {@code lossesSeen}.
     */
    record NotGranted(long lossesSeen) implements Told {
    }

    /**
     * Told that the consumer may consume under the lease with this {@code token}, when its lapse count was
     * {@code lapsesSeen}.
     */
    record Granted(long token, long lapsesSeen) implements Told {
    }

    // A lapse count no consumer reaches, for a tell under way
    static final long UNSEEN = -1;

    enum Change {
        NOTHING, GRANT, PROHIBIT
    }

    private static final VarHandle LAPSES;
    private static final VarHandle TELLING;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            LAPSES = lookup.findVarHandle(ConsumerState.class, "lapses", long.class);
            TELLING = lookup.findVarHandle(ConsumerState.class, "telling", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * A registered consumer. {@link #status} is written only under the consumer's lock, and {@link #told} only by the
     * one tell that holds {@link #telling}.
     */
    private final class ConsumerState {
        private final CompetingConsumer key;
        private volatile Status status = Status.LOCK_NOT_ACQUIRED;
        private volatile Told told = new NotGranted(0);
        /**
         * Moves each time {@link #hasLock} answers no for an acquired status and each time this consumer loses a
         * lease, so a grant is told again after either.
         */
        private volatile long lapses;
        /**
         * Moves each time this consumer loses a lease, in the same locked step that replaces its acquired status.
         */
        private volatile long losses;
        private volatile boolean telling;
        private volatile boolean tellAgain;
        private final Runnable tellInBackground = () -> MongoLeaseCompetingConsumerStrategySupport.this.tellInBackground(this);

        private ConsumerState(CompetingConsumer key) {
            this.key = key;
        }

        // Under the consumer's lock, before the status that replaces the acquired one is written
        private void lose() {
            lapse();
            losses = losses + 1;
        }

        private void lapse() {
            LAPSES.getAndAdd(this, 1L);
        }
    }

    /**
     * A consumer's status, with its fencing token and {@link ListenerLock#sentAt()} for the acquired case. The token
     * stays exactly as it was while this status remains {@code LOCK_ACQUIRED}, since a refresh (see
     * {@code refreshOne}) that goes through keeps it in the status it records, and a lost commit replaces the
     * whole status with {@code LOCK_NOT_ACQUIRED}. The token itself outlives the status in
     * {@link #lastHeldFencingTokens}, and that stale token is what a fence built on {@link #fencingToken(String)}
     * refuses once the next holder has written.
     */
    record Status(Kind kind, OptionalLong fencingToken, long sentAt) {
        static final Status LOCK_NOT_ACQUIRED = new Status(Kind.LOCK_NOT_ACQUIRED, OptionalLong.empty(), 0);
        static final Status LOCK_RELEASED = new Status(Kind.LOCK_RELEASED, OptionalLong.empty(), 0);
        static final Status UNREGISTERING = new Status(Kind.UNREGISTERING, OptionalLong.empty(), 0);

        static Status lockAcquired(long fencingToken, long sentAt) {
            return new Status(Kind.LOCK_ACQUIRED, OptionalLong.of(fencingToken), sentAt);
        }

        boolean isLockAcquired() {
            return kind == Kind.LOCK_ACQUIRED;
        }

        enum Kind {
            LOCK_ACQUIRED, LOCK_NOT_ACQUIRED, LOCK_RELEASED, UNREGISTERING
        }
    }

    private static void logDebug(String message, @Nullable Object... params) {
        if (log.isDebugEnabled()) {
            log.debug(message, params);
        }
    }
}