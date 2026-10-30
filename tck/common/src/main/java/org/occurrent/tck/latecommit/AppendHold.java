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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The gate a fixture's hook closes on one append, for a fixture that builds a {@link HeldAppend}.
 * <p>
 * The hook sits on the append's path after the store has assigned the position and before it commits. The first call
 * through it holds and every later call runs normally, so the append that is held is the first one made through the
 * store the fixture handed out, and nothing else in the test is slowed down.
 * <pre>{@code
 * if (hold.engage()) {
 *     hold.awaitRelease();
 * }
 * }</pre>
 * A hook that cannot park a thread, such as one in a reactive pipeline, subscribes to {@link #released()} instead.
 * <p>
 * A hold that is never released does not hang the build. {@link #awaitRelease()} gives up after
 * {@link #RELEASE_TIMEOUT} and throws, and the append that was held reports it when the suite joins it.
 */
@NullMarked
public final class AppendHold {

    /**
     * How long a held append waits to be released before it fails.
     */
    public static final Duration RELEASE_TIMEOUT = Duration.ofSeconds(30);

    private final CountDownLatch held = new CountDownLatch(1);
    private final CompletableFuture<Void> released = new CompletableFuture<>();
    private final AtomicBoolean armed = new AtomicBoolean(true);

    /**
     * Called by the hook. Returns {@code true} for the first call only, which is the append that must hold, and
     * records that an append is now held.
     */
    public boolean engage() {
        if (!armed.compareAndSet(true, false)) {
            return false;
        }
        held.countDown();
        return true;
    }

    /**
     * Parks the calling thread until {@link #release()} is called.
     *
     * @throws IllegalStateException if the hold is not released within {@link #RELEASE_TIMEOUT}, or the thread is
     *                               interrupted
     */
    public void awaitRelease() {
        try {
            released.get(RELEASE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while an append was held", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("A held append was not released within " + RELEASE_TIMEOUT, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Completes when the hold is released, for a hook that must not block a thread.
     */
    public CompletableFuture<Void> released() {
        return released;
    }

    boolean awaitHeld(Duration timeout) throws InterruptedException {
        return held.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    void release() {
        released.complete(null);
    }
}
