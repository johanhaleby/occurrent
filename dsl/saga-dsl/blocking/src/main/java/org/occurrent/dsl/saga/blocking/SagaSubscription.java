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

package org.occurrent.dsl.saga.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A running saga: the underlying event {@link Subscription} plus the timer poller. Closing it stops the poller and, when
 * the poller was lease-gated, releases the timer lease so another instance can take over. The event subscription is
 * cancelled through the subscription model the way any subscription is (this handle only owns the poller it started).
 * <p>
 * {@link #instances()} is read-only, as ADR 70 intended. {@link #release(String)} brings a quarantined instance back.
 */
public final class SagaSubscription implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SagaSubscription.class);

    private final Subscription subscription;
    private final ExecutorService timerPoller;
    private final SagaInstances instances;
    private final @Nullable CompetingConsumerStrategy competingConsumerStrategy;
    private final @Nullable String leaseKey;
    private final @Nullable String holderId;
    private final Function<String, ReleaseResult> release;

    /** What {@link #release(String)} did. */
    public enum ReleaseResult {
        /**
         * The instance replayed the input it was quarantined on and every input it recorded, and is
         * {@link org.occurrent.dsl.saga.SagaStatus#ACTIVE}, or {@link org.occurrent.dsl.saga.SagaStatus#COMPLETED} when
         * one of them completed it.
         */
        RELEASED,
        /**
         * An input failed again during the release. The failure is recorded the way any failure is, the instance stays
         * releasing, and the timer poller tries the input again until it succeeds or its budget runs out. It also means
         * that the store could not take any write that moves the release on, see {@link SagaSubscription#release(String)}.
         */
        STILL_RELEASING,
        /**
         * The instance is quarantined again and not releasing, because an input kept failing during the release until
         * its budget ran out, or because the store could not write the instance with that input applied or with its
         * failure recorded, see
         * {@link SagaSubscription#release(String)}. That input is the failure record's input now, and the inputs after
         * it stay recorded, so a further release replays it first and then those.
         */
        QUARANTINED_AGAIN,
        /** The instance is not quarantined, so there was nothing to release. */
        NOT_QUARANTINED,
        /** No instance has that id. */
        NOT_FOUND
    }

    SagaSubscription(Subscription subscription, ExecutorService timerPoller, SagaInstances instances,
                     @Nullable CompetingConsumerStrategy competingConsumerStrategy,
                     @Nullable String leaseKey, @Nullable String holderId) {
        this(subscription, timerPoller, instances, competingConsumerStrategy, leaseKey, holderId, sagaId -> {
            throw new IllegalStateException("This saga subscription cannot release instances");
        });
    }

    SagaSubscription(Subscription subscription, ExecutorService timerPoller, SagaInstances instances,
                     @Nullable CompetingConsumerStrategy competingConsumerStrategy,
                     @Nullable String leaseKey, @Nullable String holderId, Function<String, ReleaseResult> release) {
        this.release = requireNonNull(release, "release cannot be null");
        this.subscription = requireNonNull(subscription, "subscription cannot be null");
        this.timerPoller = requireNonNull(timerPoller, "timerPoller cannot be null");
        this.instances = requireNonNull(instances, "instances cannot be null");
        this.competingConsumerStrategy = competingConsumerStrategy;
        this.leaseKey = leaseKey;
        this.holderId = holderId;
    }

    /** The id of the underlying event subscription. */
    public String id() {
        return subscription.id();
    }

    /**
     * Read-only access to this saga's instances, for observing their lifecycle. Backed by the same
     * {@code SagaStateStore} the saga runs against, so it stays usable after {@link #close()}: closing stops this
     * instance's poller, it does not close the store.
     */
    public SagaInstances instances() {
        return instances;
    }

    /**
     * Release the quarantined instance {@code sagaId}. It replays the input the instance was quarantined on and then
     * every input it recorded while quarantined, in the order they were recorded, each from the event its record holds
     * and applied the way a delivery is, deduplicated by the event's own stream id and stream version. The instance
     * stays {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} while that runs, so an input that arrives meanwhile
     * is recorded and replayed too, and becomes {@link org.occurrent.dsl.saga.SagaStatus#ACTIVE} once no recorded input
     * is left.
     * <p>
     * A replayed input's commands are dispatched before the write that applies it, so like a live input's they are
     * dispatched at least once. A write that loses only to inputs recorded meanwhile is written again without
     * dispatching them a second time.
     * <p>
     * The instance keeps every stream version the release applies in its stream marks until the release ends, so a
     * long backlog can grow the instance past what its store can write. The release asks the store before every write
     * whether it can take it, see {@link org.occurrent.dsl.saga.SagaStateStoreSkippedInputs#fits}, and for the write
     * that applies an input it asks before it dispatches the input's commands. When the store can't take the instance
     * with the input applied, or with its failure recorded, the release doesn't apply that input. An attempt the store
     * refuses that way dispatches none of the input's commands, but an earlier attempt can have dispatched them already
     * with a write that then lost to another writer, and the ERROR says how many. The instance is quarantined again on that input with its marks trimmed, the release returns
     * {@link ReleaseResult#QUARANTINED_AGAIN}, and an ERROR names the instance and says which of two cases it is. When
     * the store can write the input applied to the trimmed instance, the ERROR says to release the instance again, and a
     * further release goes on from that input. Otherwise a further release stops on that input too, and the ERROR says
     * to delete the instance or handle the input and the ones after it by hand.
     * <p>
     * When the store can't take the write that quarantines the instance again either, even with no pending entry for the
     * input and no failure message, the release writes nothing and returns {@link ReleaseResult#STILL_RELEASING}, and an
     * ERROR names the instance and says to delete it, after exporting or handling the inputs it recorded, since deleting
     * it deletes them. The timer poller tries the instance again on every poll and logs that ERROR again. The same holds
     * when the store can't take the write that releases the instance, or the one that deletes the record of an input the
     * instance applied already. When the store can't take the write that starts the release, the release starts with
     * the marks trimmed to the reorder window instead. When the store can't take that either, this throws an {@link IllegalStateException} that
     * names the instance and the way out, and logs it as an ERROR.
     * {@code SpringMongoSagaStateStore} refuses an instance that takes more than 16 MiB of BSON, and a write that takes
     * more than 16 MiB and 16 KiB with its compare-and-set filter, which holds the saga id. A store whose {@code fits}
     * answers {@code true} to every instance refuses none, so a release on it dispatches the commands of an input whose
     * write the store then fails.
     * <p>
     * It takes no lease and pauses nothing, so it can be called on any node. Calling it again on an instance that is
     * releasing goes on where the release stands. The timer poller also goes on with a release, on the node that holds
     * the timer lease when there is a competing-consumer strategy, but only once the instance's {@code updatedAt} is a
     * whole {@code timerPollInterval} old. A running release writes the instance for every input it applies, so the
     * poller skips it, on this node or another, and goes on with one that failed on an input or was interrupted. The
     * poller and a release, or two polls, can still replay the same input and both dispatch its commands, in five
     * cases: the release takes longer than {@code timerPollInterval} over one input, a poller's clock is that far
     * ahead of the writer's, this call comes while the poller replays the instance, a release spends longer than that
     * on inputs the stream marks show as applied, which it deletes in writes that leave the instance's
     * {@code updatedAt} as it is, and two polls go on with the instance at the same time. Without a competing-consumer
     * strategy every node polls, and with one, two nodes can poll at once while the timer lease moves from one to the
     * other. For inputs the stream marks show as applied neither dispatches anything, so only the first input that
     * isn't applied can be dispatched twice.
     * <p>
     * The input the instance was quarantined on gets a full quarantine budget again, measured from the start of the
     * release. An exception that the state store throws while reading the instance or its records, or while starting
     * the release, propagates unwrapped.
     * <p>
     * The records are the only copy the saga keeps of the inputs the instance skipped, so deleting a quarantined
     * instance with {@code SagaStateStore.delete(sagaId)} deletes them. Release the instance first to apply them.
     *
     * @return what the release did, see {@link ReleaseResult}
     * @throws IllegalStateException when the instance was quarantined by 0.34.0, which recorded none of the inputs it
     *                               skipped. The message names the way out, {@code SagaStateStore.delete(sagaId)}. Also
     *                               thrown when the state store does not implement {@link org.occurrent.dsl.saga.SagaStateStoreSkippedInputs}, and
     *                               when the store gives back an instance stored as {@code QUARANTINED_RECORDING}
     *                               without its failure input, quarantine id or skip count. The message names the
     *                               missing field. Also thrown when the store holds no record of the input the
     *                               instance was quarantined on, as for an instance restored without its records,
     *                               since a release would make it {@code ACTIVE} without ever applying that input.
     *                               Also thrown, before the release reads or writes anything and even for an
     *                               instance the store does not hold, when the store's
     *                               {@link org.occurrent.dsl.saga.SagaStateStoreSkippedInputs#requireAtomicTransitions()}
     *                               throws, as {@code SpringMongoSagaStateStore} on a standalone server, since the
     *                               store would refuse the write that applies a replayed input after the release
     *                               dispatched its commands. {@code SpringMongoSagaStateStore} throws a
     *                               {@link org.occurrent.dsl.saga.SagaReadRefusedException} inside a MongoDB
     *                               transaction of the caller's, and inside a Spring transaction of another resource
     *                               that synchronizes its resources, as one does by default, and the release throws it
     *                               as it is, before it reads or writes anything
     * @throws SagaConcurrencyException when the release loses its compare-and-set {@code maxCasAttempts} times in a row,
     *                                  to start the release or to write a replayed input
     */
    public ReleaseResult release(String sagaId) {
        return release.apply(requireNonNull(sagaId, "sagaId cannot be null"));
    }

    /** The underlying event subscription. */
    public Subscription subscription() {
        return subscription;
    }

    /** Block until the underlying subscription has started. */
    public void waitUntilStarted() {
        subscription.waitUntilStarted();
    }

    /** Block until the underlying subscription has started, up to {@code timeout}. */
    public boolean waitUntilStarted(Duration timeout) {
        return subscription.waitUntilStarted(timeout);
    }

    /** Release the timer lease (if gated) and stop the poller, letting an in-flight poll finish before interrupting. */
    @Override
    public void close() {
        if (competingConsumerStrategy != null && leaseKey != null && holderId != null) {
            // Best-effort release so another instance takes over promptly. A failure here only delays the handover until
            // the lease expires on its own, so it must not stop the poller from shutting down.
            try {
                competingConsumerStrategy.unregisterCompetingConsumer(leaseKey, holderId);
            } catch (Exception e) {
                // Exception rather than RuntimeException because a RetryStrategy configured with mapError can map a
                // failure to a checked exception, which the retry loop rethrows unwrapped. One of those escaping here
                // would skip the poller shutdown below, which is the whole thing this catch exists to prevent.
                log.warn("Failed to release the timer lease '{}' for saga subscription '{}'", leaseKey, subscription.id(), e);
            }
        }
        ExecutorShutdown.shutdownSafely(timerPoller, 5, TimeUnit.SECONDS);
    }
}
