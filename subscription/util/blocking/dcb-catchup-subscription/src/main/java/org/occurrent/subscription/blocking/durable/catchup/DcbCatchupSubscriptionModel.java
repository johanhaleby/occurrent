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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.subscription.GlobalSubscriptionPosition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtSubscriptionPosition;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.UseSubscriptionPositionInStorage;

import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The blocking DCB catch-up path (ADR 20): replays historic DCB events ordered by {@code position} then hands over to
 * a live subscription. Split out of {@code CatchupSubscriptionModel} (see ADR 25 / Wave 2b) alongside
 * {@code StreamCatchupSubscriptionModel} so the two paths can be depended on independently; this class is the one that
 * still needs {@code eventstore-api-dcb} on the classpath.
 * <p>
 * Delivery is at-least-once, with the same catch-up-to-live handover guarantee documented on the dispatcher: the live
 * resume token is captured before the bulk replay, and a replay longer than the change stream history fails loudly at
 * handover instead of silently dropping events.
 */
@NullMarked
public class DcbCatchupSubscriptionModel implements SubscriptionModel, DelegatingSubscriptionModel {

    private final PositionAwareSubscriptionModel subscriptionModel;
    private final DcbEventStore dcbEventStore;
    private final DcbQuery dcbQuery;
    private final CatchupSubscriptionModelConfig config;
    private final Class<?> subscriptionModelContextType;
    private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown = false;

    public DcbCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbQuery dcbQuery, CatchupSubscriptionModelConfig config) {
        this(subscriptionModel, dcbEventStore, dcbQuery, config, DcbCatchupSubscriptionModel.class);
    }

    /**
     * @param subscriptionModelContextType The class a caller-supplied {@code StartAt.dynamic} sees as
     *                                      {@code SubscriptionModelContext#subscriptionModelType()} when it is first
     *                                      resolved. The {@code CatchupSubscriptionModel} dispatcher passes its own
     *                                      class here so a caller that pattern-matches on the public dispatcher type
     *                                      keeps working regardless of which mode-specific class runs the catch-up.
     */
    public DcbCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, DcbQuery dcbQuery, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.dcbEventStore = Objects.requireNonNull(dcbEventStore, "dcbEventStore cannot be null");
        this.dcbQuery = Objects.requireNonNull(dcbQuery, "dcbQuery cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.subscriptionModelContextType = Objects.requireNonNull(subscriptionModelContextType, "subscriptionModelContextType cannot be null");
    }

    private SubscriptionModelContext generateSubscriptionModelContext() {
        return new SubscriptionModelContext(subscriptionModelContextType);
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(startAt, "Start at supplier cannot be null");
        final StartAt firstStartAt;
        if (startAt.isDefault()) {
            // Resume from the stored position if there is one, otherwise subscribe live (with the DCB query post-filter).
            SubscriptionPosition subscriptionPosition = returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> cfg.storage().read(subscriptionId)).orElse(null);
            if (subscriptionPosition == null) {
                return startLiveDcbSubscription(subscriptionId, filter, startAt, action, null);
            } else {
                firstStartAt = StartAt.subscriptionPosition(subscriptionPosition);
            }
        } else if (startAt.isDynamic()) {
            StartAt startAtGeneratedByDynamic = startAt.get(generateSubscriptionModelContext());
            if (startAtGeneratedByDynamic == null) {
                return startLiveDcbSubscription(subscriptionId, filter, startAt, action, null);
            } else {
                firstStartAt = startAtGeneratedByDynamic;
            }
        } else {
            firstStartAt = startAt;
        }

        // A non-DCB position means the catch-up already handed over and the live subscription stored a change-stream
        // token (or the caller asked to start live directly). Subscribe live, still applying the DCB query post-filter.
        if (!isDcbCatchupPosition(firstStartAt)) {
            return startLiveDcbSubscription(subscriptionId, filter, firstStartAt, action, null);
        }

        Future<Subscription> subscriptionCompletableFuture = CompletableFuture.supplyAsync(() -> startDcbCatchupSubscription(subscriptionId, filter, startAt, action, firstStartAt));
        return new CatchupSubscription(subscriptionId, subscriptionCompletableFuture);
    }

    private Subscription startLiveDcbSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAtToUse, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        return subscriptionModel.subscribe(subscriptionId, filter, startAtToUse, dcbLiveConsumer(action, cache));
    }

    private Consumer<CloudEvent> dcbLiveConsumer(Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        return cloudEvent -> {
            // The live change stream sees every event, so keep only DCB events matching the query and skip those
            // already delivered during catch-up. DCB events are identified by isDcbEvent (the tags extension), not by
            // position, since stream events now carry a position too.
            if (DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, dcbQuery)
                    && (cache == null || !cache.isCached(cloudEvent.getId()))) {
                action.accept(cloudEvent);
            }
        };
    }

    private Subscription startDcbCatchupSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, StartAt firstStartAt) {
        runningCatchupSubscriptions.put(subscriptionId, true);
        long windowSize = config.dcbCatchupPositionWindowSize;

        StartAt nextStartAt = firstStartAt.get(generateSubscriptionModelContext());
        SubscriptionPosition subscriptionPosition = ((StartAtSubscriptionPosition) Objects.requireNonNull(nextStartAt)).subscriptionPosition;
        long startPosition = GlobalSubscriptionPosition.positionOf(subscriptionPosition);

        // Capture the live resume token before the bulk replay so an event committed during the replay is still
        // delivered live. On a replay longer than the change stream history the token ages out and the handover fails
        // loudly instead of dropping the event.
        Class<? extends SubscriptionModel> delegatedSubscriptionModelType = getDelegatedSubscriptionModel().getClass();
        StartAt delegatedStartAt = startAt.get(new SubscriptionModelContext(delegatedSubscriptionModelType));
        final SubscriptionPosition globalSubscriptionPosition = delegatedStartAt == null ? null : subscriptionModel.globalSubscriptionPosition();

        // Page through the DCB sequence from the resume position to the head seen at the start, in windows so a large
        // rebuild does not load the whole matched set at once. position is monotonic and server-assigned, so this needs
        // no count and no time sort.
        long bulkHead = dcbEventStore.read(dcbQuery, DcbReadOptions.between(startPosition, startPosition)).lastSequencePosition();
        long cursor = deliverDcbWindows(startPosition, bulkHead, windowSize, subscriptionId, action, null);

        FixedSizeCache catchupPhaseCache = new FixedSizeCache(config.cacheSize);

        // Reconcile events written during the replay by paging until the head stops advancing. Overlapping re-reads
        // are deduped by the cache. Anything written after the loop is newer than the live resume position and arrives
        // live.
        long head = dcbEventStore.read(dcbQuery, DcbReadOptions.between(cursor, cursor)).lastSequencePosition();
        while (head > cursor && !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            cursor = deliverDcbWindows(cursor, head, windowSize, subscriptionId, action, catchupPhaseCache);
            head = dcbEventStore.read(dcbQuery, DcbReadOptions.between(cursor, cursor)).lastSequencePosition();
        }

        if (delegatedStartAt == null) {
            returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> {
                cfg.storage().delete(subscriptionId);
                return null;
            });
        }

        final boolean subscriptionsWasCancelledOrShutdown;
        if (!shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            subscriptionsWasCancelledOrShutdown = false;
            runningCatchupSubscriptions.remove(subscriptionId);
        } else {
            subscriptionsWasCancelledOrShutdown = true;
        }

        StartAt startAtToUse = StartAt.dynamic(this.<Supplier<StartAt>, UseSubscriptionPositionInStorage>returnIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class,
                        cfg -> () -> {
                            SubscriptionPosition position = cfg.storage().read(subscriptionId);
                            // If nothing is stored, or the stored position is a DCB position (written by this catch-up),
                            // save the live change-stream position so the wrapped subscription resumes from there.
                            if ((position == null || GlobalSubscriptionPosition.isGlobalSubscriptionPosition(position)) && globalSubscriptionPosition != null) {
                                position = cfg.storage().save(subscriptionId, globalSubscriptionPosition);
                            } else if (position == null) {
                                return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                            }
                            return StartAt.subscriptionPosition(position);
                        })
                .orElse(() -> {
                    if (globalSubscriptionPosition == null) {
                        return delegatedStartAt == null ? startAt : StartAt.subscriptionModelDefault();
                    } else {
                        return StartAt.subscriptionPosition(globalSubscriptionPosition);
                    }
                }));

        final Subscription subscription;
        if (subscriptionsWasCancelledOrShutdown) {
            doIfSubscriptionPositionStorageConfigIs(UseSubscriptionPositionInStorage.class, cfg -> {
                if (!cfg.storage().exists(subscriptionId)) {
                    startAtToUse.get(generateSubscriptionModelContext());
                }
            });
            subscription = new CancelledSubscription(subscriptionId);
        } else {
            subscription = startLiveDcbSubscription(subscriptionId, filter, startAtToUse, action, catchupPhaseCache);
        }
        return subscription;
    }

    /**
     * Delivers DCB events in {@code (fromExclusive, toInclusive]} by paging through position windows, and returns the
     * position the cursor reached. Stops early on shutdown or cancellation.
     */
    private long deliverDcbWindows(long fromExclusive, long toInclusive, long windowSize, String subscriptionId, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        long cursor = fromExclusive;
        while (cursor < toInclusive && !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId)) {
            long upTo = Math.min(cursor + windowSize, toInclusive);
            DcbEventStream slice = dcbEventStore.read(dcbQuery, DcbReadOptions.between(cursor, upTo));
            deliverCatchupEvents(slice.stream(), subscriptionId, action, cache);
            cursor = upTo;
        }
        return cursor;
    }

    /**
     * Delivers catch-up events to {@code action}, optionally deduping against {@code cache}, and persists the DCB
     * subscription position for events matching the catch-up persist predicate.
     */
    private void deliverCatchupEvents(Stream<CloudEvent> cloudEvents, String subscriptionId, Consumer<CloudEvent> action, @Nullable FixedSizeCache cache) {
        // try-with-resources closes the source stream even when takeWhile short-circuits on shutdown, so a
        // resource-backed read does not leak its cursor.
        try (cloudEvents) {
            Stream<CloudEvent> takeWhile = cloudEvents.takeWhile(__ -> !shuttingDown && runningCatchupSubscriptions.containsKey(subscriptionId));
            if (cache != null) {
                // Skip events already delivered in an earlier reconciliation pass (the delta is re-read until it
                // stabilises, so passes overlap) and record the rest so the live subscription can skip them at the
                // handover seam. Without the filter the overlapping re-reads would deliver duplicates.
                takeWhile = takeWhile.filter(e -> !cache.isCached(e.getId())).peek(e -> cache.put(e.getId()));
            }
            takeWhile
                    .peek(action)
                    .filter(returnIfSubscriptionPositionStorageConfigIs(SubscriptionPositionStorageConfig.PersistSubscriptionPositionDuringCatchupPhase.class, SubscriptionPositionStorageConfig.PersistSubscriptionPositionDuringCatchupPhase::persistCloudEventPositionPredicate).orElse(__ -> false))
                    .forEach(e -> doIfSubscriptionPositionStorageConfigIs(SubscriptionPositionStorageConfig.PersistSubscriptionPositionDuringCatchupPhase.class,
                            cfg -> cfg.storage().save(subscriptionId, GlobalSubscriptionPosition.of(OccurrentCloudEventExtension.getPosition(e)))));
        }
    }

    // firstStartAt is already resolved (non-dynamic) by the time this runs, so the context class used to call get()
    // again is a no-op; generateSubscriptionModelContext() is used anyway for consistency with the other call sites.
    private boolean isDcbCatchupPosition(StartAt startAt) {
        StartAt start = startAt.get(generateSubscriptionModelContext());
        if (!(start instanceof StartAtSubscriptionPosition)) {
            return false;
        }
        return GlobalSubscriptionPosition.isGlobalSubscriptionPosition(((StartAtSubscriptionPosition) start).subscriptionPosition);
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
     * Cancel a DCB catch-up running for {@code subscriptionId}. A no-op if this class has no catch-up running for that
     * id (for example because it belongs to the stream path in a dual-mode dispatcher). Does not touch the shared live
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

    @Override
    public void cancelSubscription(String subscriptionId) {
        cancelRunningCatchup(subscriptionId);
        subscriptionModel.cancelSubscription(subscriptionId);
        deletePositionFromStorage(subscriptionId);
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
    public void shutdown() {
        markShuttingDown();
        subscriptionModel.shutdown();
    }

    @Override
    public SubscriptionModel getDelegatedSubscriptionModel() {
        return subscriptionModel;
    }

    private <T, C extends SubscriptionPositionStorageConfig> Optional<T> returnIfSubscriptionPositionStorageConfigIs(Class<C> cls, Function<C, @Nullable T> fn) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            return Optional.ofNullable(fn.apply(cls.cast(config.subscriptionStorageConfig)));
        }
        return Optional.empty();
    }

    private <C extends SubscriptionPositionStorageConfig> void doIfSubscriptionPositionStorageConfigIs(Class<C> cls, Consumer<C> consumer) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            consumer.accept(cls.cast(config.subscriptionStorageConfig));
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DcbCatchupSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("subscriptionModel=" + subscriptionModel)
                .add("dcbEventStore=" + dcbEventStore)
                .add("dcbQuery=" + dcbQuery)
                .add("config=" + config)
                .add("runningCatchupSubscriptions=" + runningCatchupSubscriptions)
                .add("shuttingDown=" + shuttingDown)
                .toString();
    }
}
