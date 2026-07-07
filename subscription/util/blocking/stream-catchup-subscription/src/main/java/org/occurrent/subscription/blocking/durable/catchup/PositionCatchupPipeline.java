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

import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * The bulk-then-reconcile paging shared by every position-ordered blocking catch-up replay. A {@link Reader} supplies
 * the window and head reads, so this pipeline is free of any specific store or query type, and is reused by both the
 * stream and the DCB catch-up models (the blocking counterpart of the reactor {@code PositionCatchupPipeline}).
 * <p>
 * The replay pages the sequence in {@code windowSize} windows, then a reconciliation pass keeps paging until the head
 * stops advancing so events written during the replay are delivered in order. The caller supplies the delivery
 * (dedup cache, checkpoint persistence, cancellation) so this class stays a pure paging loop.
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
     * Replays from {@code startPosition} to the current head, then reconciles until the head stops advancing,
     * handing each read window to {@code deliver}. Returns the position the replay reached, which is the resume
     * boundary the caller hands over to live delivery.
     *
     * @param keepRunning Checked before every window read; stops the replay early on cancellation or shutdown.
     * @param deliver     Called with each window's events (bulk windows get a {@code null} cache, reconciliation
     *                    windows get {@code cache}) so the caller can dedupe, persist checkpoints and deliver.
     */
    long replay(long startPosition, BooleanSupplier keepRunning, BiConsumer<Stream<CloudEvent>, @Nullable FixedSizeCache> deliver, FixedSizeCache cache) {
        long bulkHead = reader.currentHead();
        long cursor = windows(startPosition, bulkHead, keepRunning, deliver, null);

        // Reconcile events written during the bulk replay by continuing to page until the head stops advancing.
        // Re-reads of overlapping windows are deduped by the cache (delivery is at-least-once).
        long head = reader.currentHead();
        while (head > cursor && keepRunning.getAsBoolean()) {
            cursor = windows(cursor, head, keepRunning, deliver, cache);
            head = reader.currentHead();
        }
        return cursor;
    }

    // Delivers events in (fromExclusive, toInclusive], paging in position windows. Stops early when keepRunning
    // reports false, for example on shutdown or cancellation.
    private long windows(long fromExclusive, long toInclusive, BooleanSupplier keepRunning, BiConsumer<Stream<CloudEvent>, @Nullable FixedSizeCache> deliver, @Nullable FixedSizeCache cache) {
        long cursor = fromExclusive;
        while (cursor < toInclusive && keepRunning.getAsBoolean()) {
            long upTo = Math.min(cursor + windowSize, toInclusive);
            deliver.accept(reader.readWindow(cursor, upTo), cache);
            cursor = upTo;
        }
        return cursor;
    }
}
