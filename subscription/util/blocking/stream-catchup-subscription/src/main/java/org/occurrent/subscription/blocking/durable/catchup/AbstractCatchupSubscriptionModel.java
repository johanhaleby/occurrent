/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.blocking.durable.catchup;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.UseSubscriptionPositionInStorage;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Shared plumbing for the mode-specific catch-up subscription models
 * ({@link StreamCatchupSubscriptionModel} and the DCB catch-up model): the wrapped live delegate, the config, the
 * running-catch-up bookkeeping, the shutdown flag, and the lifecycle delegation to the wrapped subscription model.
 * The mode-specific replay and {@code subscribe(...)} routing stays in each subclass. This base is DCB-free so it can
 * live in the stream module that both modes build against.
 */
@NullMarked
public abstract class AbstractCatchupSubscriptionModel implements SubscriptionModel, DelegatingSubscriptionModel {

    protected final PositionAwareSubscriptionModel subscriptionModel;
    protected final CatchupSubscriptionModelConfig config;
    protected final Class<?> subscriptionModelContextType;
    protected final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
    protected volatile boolean shuttingDown = false;

    protected AbstractCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.subscriptionModelContextType = Objects.requireNonNull(subscriptionModelContextType, "subscriptionModelContextType cannot be null");
    }

    // Reports subscriptionModelContextType (CatchupSubscriptionModel when wrapped by the dispatcher) so a
    // StartAt.dynamic supplied by a caller that pattern matches on the public dispatcher type keeps working
    // regardless of which mode-specific class ends up running the catch-up underneath it.
    protected SubscriptionModelContext generateSubscriptionModelContext() {
        return new SubscriptionModelContext(subscriptionModelContextType);
    }

    @Override
    public void stop() {
        getDelegatedSubscriptionModel().stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        getDelegatedSubscriptionModel().start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return getDelegatedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return getDelegatedSubscriptionModel().isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return getDelegatedSubscriptionModel().isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return getDelegatedSubscriptionModel().resumeSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        getDelegatedSubscriptionModel().pauseSubscription(subscriptionId);
    }

    /**
     * Cancel a catch-up running for {@code subscriptionId}. A no-op if this class has no catch-up running for that id
     * (for example because it belongs to the other path in a dual-mode dispatcher). Does not touch the shared live
     * delegate or position storage; the dispatcher owns those since both paths share the same delegate.
     */
    public void cancelRunningCatchup(String subscriptionId) {
        runningCatchupSubscriptions.remove(subscriptionId);
    }

    /**
     * Mark this model as shutting down so any in-flight catch-up stops as soon as possible. Does not touch the shared
     * live delegate; the dispatcher owns that.
     */
    public void markShuttingDown() {
        shuttingDown = true;
        runningCatchupSubscriptions.clear();
    }

    /**
     * Delete {@code subscriptionId}'s position from the configured position storage, if any. Exposed so the
     * dispatcher can delete it exactly once when cancelling a subscription that could belong to either mode, since
     * the position storage config (and the storage instance it wraps) is shared, not owned per mode.
     */
    public void deletePositionFromStorage(String subscriptionId) {
        doIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage().delete(subscriptionId));
    }

    @Override
    public SubscriptionModel getDelegatedSubscriptionModel() {
        return subscriptionModel;
    }

    protected <T, C extends SubscriptionPositionStorageConfig> Optional<T> returnIfSubscriptionPositionStorageConfigIs(Class<C> cls, Function<C, @Nullable T> fn) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            return Optional.ofNullable(fn.apply(cls.cast(config.subscriptionStorageConfig)));
        }
        return Optional.empty();
    }

    protected <C extends SubscriptionPositionStorageConfig> void doIfSubscriptionPositionStorageConfigIs(Class<C> cls, Consumer<C> consumer) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            consumer.accept(cls.cast(config.subscriptionStorageConfig));
        }
    }
}
