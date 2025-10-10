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

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Common operations for MongoDB lease-based competing consumer strategies
 */
@NullMarked
public class MongoLeaseCompetingConsumerStrategySupport {
    private static final Logger log = LoggerFactory.getLogger(MongoLeaseCompetingConsumerStrategySupport.class);

    public static final String DEFAULT_COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";
    public static final Duration DEFAULT_LEASE_TIME = Duration.ofSeconds(20);

    private final Clock clock;
    private final Duration leaseTime;
    private final ScheduledRefresh scheduledRefresh;
    private final ConcurrentMap<CompetingConsumer, Status> competingConsumers;
    private final Set<CompetingConsumerListener> competingConsumerListeners;
    private final RetryStrategy retryStrategy;

    private volatile boolean running;


    public MongoLeaseCompetingConsumerStrategySupport(Duration leaseTime, Clock clock, RetryStrategy retryStrategy) {
        this.clock = clock;
        this.leaseTime = leaseTime;
        this.scheduledRefresh = ScheduledRefresh.auto();
        this.running = true;
        this.competingConsumerListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.competingConsumers = new ConcurrentHashMap<>();

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
        Status oldStatus = competingConsumers.get(competingConsumer);
        boolean acquired = MongoListenerLockService.acquireOrRefreshFor(collection, clock, retryStrategy, leaseTime, subscriptionId, subscriberId).isPresent();
        logDebug("registerCompetingConsumer: oldStatus={} acquired lock={} (subscriberId={}, subscriptionId={})", oldStatus, acquired, subscriberId, subscriptionId);
        competingConsumers.put(competingConsumer, acquired ? Status.LOCK_ACQUIRED : Status.LOCK_NOT_ACQUIRED);
        if (oldStatus != Status.LOCK_ACQUIRED && acquired) {
            logDebug("registerCompetingConsumer: Consumption granted (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            competingConsumerListeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
        } else if (oldStatus == Status.LOCK_ACQUIRED && !acquired) {
            logDebug("registerCompetingConsumer: Consumption prohibited (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        }
        return acquired;
    }

    public void unregisterCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        logDebug("Unregistering consumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        Status status = competingConsumers.remove(new CompetingConsumer(subscriptionId, subscriberId));
        releaseCompetingConsumer(collection, subscriptionId, subscriberId, status);
    }

    /**
     * Remove the lock use by this
     */
    public void releaseCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId) {
        releaseCompetingConsumer(collection, subscriptionId, subscriberId, null);
    }

    private void releaseCompetingConsumer(MongoCollection<BsonDocument> collection, String subscriptionId, String subscriberId, @Nullable Status suppliedStatus) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        logDebug("Releasing consumer (subscriberId={}, subscriptionId={}, suppliedStatus={})", subscriberId, subscriptionId, suppliedStatus);

        final Status status;
        if (suppliedStatus == null) {
            status = competingConsumers.get(new CompetingConsumer(subscriptionId, subscriberId));
        } else {
            status = suppliedStatus;
        }

        if (status == null) {
            logDebug("Failed to find consumer status (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return;
        }
        MongoListenerLockService.remove(collection, retryStrategy, subscriptionId, subscriberId);
        if (status == Status.LOCK_ACQUIRED) {
            logDebug("Lock status was {}, will invoke onConsumeProhibited for listeners (subscriberId={}, subscriptionId={})", status, subscriberId, subscriptionId);
            competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
        } else {
            logDebug("Lock status was {}, will NOT invoke onConsumeProhibited for listeners (subscriberId={}, subscriptionId={})", status, subscriberId, subscriptionId);
        }
    }

    public boolean hasLock(String subscriptionId, String subscriberId) {
        Objects.requireNonNull(subscriptionId, "Subscription id cannot be null");
        Objects.requireNonNull(subscriberId, "Subscriber id cannot be null");
        Status status = competingConsumers.get(new CompetingConsumer(subscriptionId, subscriberId));
        boolean hasLock = status == Status.LOCK_ACQUIRED;
        logDebug("hasLock={} (subscriberId={}, subscriptionId={})", hasLock, subscriberId, subscriptionId);
        return hasLock;
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
        competingConsumers.forEach((cc, status) -> {
            logDebug("Status {} (subscriberId={}, subscriptionId={})", status, cc.subscriberId, cc.subscriptionId);
            if (status == Status.LOCK_ACQUIRED) {
                boolean stillHasLock = MongoListenerLockService.commit(collection, clock, retryStrategy, leaseTime, cc.subscriptionId, cc.subscriberId);
                if (!stillHasLock) {
                    logDebug("Lost lock! (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
                    // Lock was lost!
                    competingConsumers.put(cc, Status.LOCK_NOT_ACQUIRED);
                    competingConsumerListeners.forEach(listener -> listener.onConsumeProhibited(cc.subscriptionId, cc.subscriberId));
                    logDebug("Completed calling onConsumeProhibited for all listeners (subscriberId={}, subscriptionId={})", cc.subscriberId, cc.subscriptionId);
                }
            } else {
                registerCompetingConsumer(collection, cc.subscriptionId, cc.subscriberId);
            }
        });
    }

    private record CompetingConsumer(String subscriptionId, String subscriberId) {
    }

    private enum Status {
        LOCK_ACQUIRED, LOCK_NOT_ACQUIRED
    }

    private static void logDebug(String message, @Nullable Object... params) {
        if (log.isDebugEnabled()) {
            log.debug(message, params);
        }
    }
}