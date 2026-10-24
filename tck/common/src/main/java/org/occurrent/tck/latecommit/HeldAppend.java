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

package org.occurrent.tck.latecommit;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Stores whose next append holds after the store has assigned its position and before it commits, until the suite
 * releases it.
 * <p>
 * Only the first append or write made through either store holds, the same {@link AppendHold} covers both. Reads and
 * every later append run normally. The stores see the same data as the fixture's own store, so the suite appends
 * through {@link #dcbEventStore()} or {@link #eventStore()} on one thread, appends through the fixture's store on
 * another, and releases the first when it is ready.
 * <p>
 * A hold is built per call to {@link LateCommits#holdNextAppend()}, so a fixture creates a fresh one every time and
 * the suite closes it.
 */
@NullMarked
public final class HeldAppend implements AutoCloseable {

    private final AppendHold hold;
    private final DcbEventStore dcbEventStore;
    private final EventStore eventStore;
    private final Runnable onClose;

    private HeldAppend(AppendHold hold, DcbEventStore dcbEventStore, EventStore eventStore, Runnable onClose) {
        this.hold = requireNonNull(hold, "hold cannot be null");
        this.dcbEventStore = requireNonNull(dcbEventStore, "dcbEventStore cannot be null");
        this.eventStore = requireNonNull(eventStore, "eventStore cannot be null");
        this.onClose = requireNonNull(onClose, "onClose cannot be null");
    }

    /**
     * @param hold          the gate the stores' hook closes on
     * @param dcbEventStore the store whose next DCB append holds
     * @param eventStore    the store whose next stream write holds
     * @param onClose       releases whatever the fixture opened for these stores, such as a client
     */
    public static HeldAppend of(AppendHold hold, DcbEventStore dcbEventStore, EventStore eventStore, Runnable onClose) {
        return new HeldAppend(hold, dcbEventStore, eventStore, onClose);
    }

    /**
     * As {@link #of(AppendHold, DcbEventStore, EventStore, Runnable)} for one object that is both stores.
     */
    public static <T extends DcbEventStore & EventStore> HeldAppend of(AppendHold hold, T store, Runnable onClose) {
        return new HeldAppend(hold, store, store, onClose);
    }

    /**
     * The store whose next DCB append holds.
     */
    public DcbEventStore dcbEventStore() {
        return dcbEventStore;
    }

    /**
     * The store whose next stream write holds.
     */
    public EventStore eventStore() {
        return eventStore;
    }

    /**
     * Waits for an append to be held, and answers whether one was within {@code timeout}.
     */
    public boolean awaitHeld(Duration timeout) throws InterruptedException {
        return hold.awaitHeld(timeout);
    }

    /**
     * Lets the held append continue to its commit.
     */
    public void release() {
        hold.release();
    }

    /**
     * Releases the hold, so no thread stays parked, and then whatever the fixture opened. Safe to call more than once.
     */
    @Override
    public void close() {
        hold.release();
        onClose.run();
    }
}
