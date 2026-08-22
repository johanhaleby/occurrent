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
import org.occurrent.subscription.internal.BoundedIdCache;

import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Bulk-then-reconcile paging shared by every position-ordered blocking catch-up replay. A {@link Reader} supplies
 * the window and head reads so this pipeline is store-agnostic, reused by both the stream and DCB catch-up models
 * (blocking counterpart of the reactor {@code PositionCatchupPipeline}).
 * <p>
 * The replay pages the sequence in {@code windowSize} windows, then reconciles once, draining up to a head
 * snapshotted at reconcile start so writes during the replay are delivered in order. It does not chase a moving
 * head, which would never terminate under sustained writes. Anything committed after the snapshot is left to the
 * live subscription, deduped by the caller's cache. The caller supplies delivery (dedup, checkpoint persistence,
 * cancellation), keeping this a pure paging loop.
 */
@NullMarked
final class PositionCatchupPipeline {

    /**
     * The store-specific head and window reads a position catch-up needs. Implemented once per store kind
     * ({@code PositionOrderedReader} for streams, {@code DcbEventStore} for DCB).
     */
    interface Reader {
        /**
         * The current position at the head of the sequence this reader reads.
         */
        long currentHead();

        /**
         * Reads events in {@code (fromExclusive, toInclusive]}.
         */
        Stream<CloudEvent> readWindow(long fromExclusive, long toInclusive);
    }

    private final Reader reader;
    private final long windowSize;

    PositionCatchupPipeline(Reader reader, long windowSize) {
        this.reader = requireNonNull(reader, Reader.class.getSimpleName() + " cannot be null");
        if (windowSize <= 0) {
            throw new IllegalArgumentException("Window size must be greater than zero");
        }
        this.windowSize = windowSize;
    }

    /**
     * Replays from {@code startPosition} to the current head, then reconciles once up to a head snapshotted at
     * reconcile start (it does not chase a moving head), handing each read window to {@code deliver}. Returns the
     * position the replay reached, which is the resume boundary the caller hands over to live delivery.
     *
     * @param keepRunning       Checked before every window read; stops the replay early on cancellation or shutdown.
     * @param deliver           Called with each window's events (bulk windows get a {@code null} cache,
     *                          reconciliation windows get {@code cache}) so the caller can dedupe, persist
     *                          checkpoints and deliver.
     * @param reconcileStarting Run once the history windows have all been delivered and before the reconciliation
     *                          reads anything.
     */
    long replay(long startPosition, BooleanSupplier keepRunning, BiConsumer<Stream<CloudEvent>, @Nullable BoundedIdCache> deliver, BoundedIdCache cache, Runnable reconcileStarting) {
        long bulkHead = reader.currentHead();
        long cursor = windows(startPosition, bulkHead, keepRunning, deliver, null);

        // Run after the history windows rather than before them, so a caller that tracks which part of the catch-up
        // it is in moves only once every history event has been delivered. Delivery here is synchronous, so the
        // windows call above returning means exactly that. Skipped when the replay was truncated, since windows also
        // returns early then and a history that stopped part way through is not a history that was read.
        if (!keepRunning.getAsBoolean()) {
            return cursor;
        }
        reconcileStarting.run();

        // Snapshot the head once and reconcile up to it. Re-reading a moving head would advance forever under
        // sustained writes and never hand over to live (livelock). Anything after the snapshot is covered by the
        // live subscription (resumes from the pre-bulk token); the bulk-tail overlap is deduped by the cache.
        long snapshotHead = reader.currentHead();
        cursor = windows(cursor, snapshotHead, keepRunning, deliver, cache);
        return cursor;
    }

    // Delivers events in (fromExclusive, toInclusive], paging in position windows. Stops early when keepRunning
    // reports false, for example on shutdown or cancellation.
    private long windows(long fromExclusive, long toInclusive, BooleanSupplier keepRunning, BiConsumer<Stream<CloudEvent>, @Nullable BoundedIdCache> deliver, @Nullable BoundedIdCache cache) {
        long cursor = fromExclusive;
        while (cursor < toInclusive && keepRunning.getAsBoolean()) {
            long upTo = Math.min(cursor + windowSize, toInclusive);
            deliver.accept(reader.readWindow(cursor, upTo), cache);
            cursor = upTo;
        }
        return cursor;
    }
}
