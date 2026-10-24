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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * One append that commits late, started on its own thread so the suite can do other work while it is held.
 * <p>
 * For a store that holds, {@code start} returns once the append is held after its position was assigned, and
 * {@link #release()} lets it commit and joins it. For a store that commits in position order there is nothing to hold,
 * so {@code start} returns once the append has committed. Either way the suite then does the same thing, which is what
 * makes a passing run on such a store a real one.
 * <p>
 * Two checks live here and are the reason a hold cannot quietly test nothing. Both throw {@link IllegalStateException}
 * and never a failed guarantee, so a store that is wrongly declared fails loudly instead of looking like one that
 * keeps the guarantee:
 * <ul>
 *     <li>A store that holds must have the append invisible while it is held, and its position below the position of
 *     the append that overtook it, see {@link #requireWindowEntered(long, long)}.</li>
 *     <li>A store that declared it commits in position order must have the append visible as soon as it has returned.</li>
 * </ul>
 * A hold that never engages times out with a message, and the append's own failure is rethrown unchanged by
 * {@link #release()}.
 * <p>
 * Shared by the TCK suites that hold an append, and not meant for an implementation under test.
 */
@NullMarked
public final class LateAppend<T> implements AutoCloseable {

    private static final Duration HOLD_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration JOIN_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL = Duration.ofMillis(50);

    private final @Nullable HeldAppend held;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, "late-commit-append");
        thread.setDaemon(true);
        return thread;
    });
    private final Future<T> result;

    private LateAppend(@Nullable HeldAppend held, Callable<T> append) {
        this.held = held;
        this.result = executor.submit(append);
    }

    /**
     * Starts a DCB append that commits late. {@code ownStore} is the fixture's store, used when the store commits in
     * position order and there is nothing to hold. {@code visibleNow} answers whether the event is visible to a reader.
     */
    public static LateAppend<DcbAppendResult> dcbAppend(LateCommits lateCommits, DcbEventStore ownStore,
                                                 List<CloudEvent> events, BooleanSupplier visibleNow) {
        return start(lateCommits, held -> () -> held.dcbEventStore().append(events),
                () -> ownStore.append(events), held -> visibleNow.getAsBoolean());
    }

    /**
     * Starts a stream write that commits late, as {@link #dcbAppend}.
     */
    public static LateAppend<WriteResult> streamWrite(LateCommits lateCommits, EventStore ownStore, String streamId,
                                               List<CloudEvent> events, BooleanSupplier visibleNow) {
        return start(lateCommits, held -> () -> held.eventStore().write(streamId, events),
                () -> ownStore.write(streamId, events), held -> visibleNow.getAsBoolean());
    }

    /**
     * Starts an append that commits late, for a suite whose append is neither a DCB append nor a stream write.
     *
     * @param heldAppend builds the append from the hold, for a store that holds
     * @param ownAppend  the same append through the fixture's own path, for a store that commits in position order
     * @param visibleNow whether the appended events are visible to a reader. Given the hold for a store that holds, so
     *                   it can read through the store the hold handed out, and {@code null} for a store that commits in
     *                   position order
     */
    public static <T> LateAppend<T> start(LateCommits lateCommits, Function<HeldAppend, Callable<T>> heldAppend,
                                          Callable<T> ownAppend, Predicate<@Nullable HeldAppend> visibleNow) {
        HeldAppend held = lateCommits.commitsInPositionOrder() ? null : lateCommits.holdNextAppend();
        LateAppend<T> late = new LateAppend<>(held, held == null ? ownAppend : heldAppend.apply(held));
        try {
            if (held == null) {
                late.awaitCommit(visibleNow);
            } else {
                late.awaitHold(held, visibleNow);
            }
        } catch (RuntimeException | Error e) {
            late.close();
            throw e;
        }
        return late;
    }

    /**
     * Whether the store declared that it commits in position order.
     */
    public boolean commitsInPositionOrder() {
        return held == null;
    }

    /**
     * Lets the append commit, joins it and returns its result. The append's own failure is rethrown unchanged.
     */
    public T release() {
        if (held != null) {
            held.release();
        }
        return join();
    }

    /**
     * The check that the hold entered the window. A store that holds must have given the held append a position below
     * the append that overtook it, or the suite has tested a store that commits in order.
     */
    public void requireWindowEntered(long heldPosition, long laterPosition) {
        if (held != null && heldPosition >= laterPosition) {
            throw new IllegalStateException("the hold did not enter the window; declare commitsInPositionOrder if this "
                    + "store commits in position order. The held append got position " + heldPosition
                    + " and the append that overtook it got " + laterPosition + ", so the held one did not reserve "
                    + "its position before the other did");
        }
    }

    /**
     * The same check for a test with no append that overtakes: a store that holds must already have assigned the held
     * append a position, so the head a reader sees is above zero.
     */
    public void requirePositionReserved(long headSeenWhileHeld) {
        if (held != null && headSeenWhileHeld <= 0) {
            throw new IllegalStateException("the hold did not enter the window; declare commitsInPositionOrder if this "
                    + "store commits in position order. No position was assigned while the append was held");
        }
    }

    @Override
    public void close() {
        if (held != null) {
            held.close();
        }
        executor.shutdownNow();
    }

    private void awaitCommit(Predicate<@Nullable HeldAppend> visibleNow) {
        join();
        if (!visibleNow.test(null)) {
            throw new IllegalStateException("the fixture declares commitsInPositionOrder but the append was not "
                    + "visible once it had returned. Either the store does not commit in position order, or its "
                    + "fixture should hold the append instead");
        }
    }

    private void awaitHold(HeldAppend held, Predicate<@Nullable HeldAppend> visibleNow) {
        long deadline = System.nanoTime() + HOLD_TIMEOUT.toNanos();
        try {
            while (!held.awaitHeld(POLL)) {
                if (result.isDone()) {
                    join();
                    throw new IllegalStateException("the append completed without being held, so the hold did not "
                            + "engage. The fixture's hook must sit after the position is assigned and before the "
                            + "append commits");
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("no append was held within " + HOLD_TIMEOUT + ", so the hold did "
                            + "not engage");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the append to be held", e);
        }
        if (visibleNow.test(held)) {
            throw new IllegalStateException("the hold did not enter the window; declare commitsInPositionOrder if this "
                    + "store commits in position order. The append was visible while it was held");
        }
    }

    private T join() {
        try {
            return result.get(JOIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("The late append failed", cause);
        } catch (TimeoutException e) {
            throw new IllegalStateException("The late append did not finish within " + JOIN_TIMEOUT, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the late append", e);
        }
    }
}
