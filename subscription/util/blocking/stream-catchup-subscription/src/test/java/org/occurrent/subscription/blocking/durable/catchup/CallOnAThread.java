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

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * One call on a thread of its own. It has returned once the call returned or threw, and {@link #awaitEnded()} fails
 * when the call threw.
 */
final class CallOnAThread {
    private final Thread thread;
    private final AtomicBoolean returned = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

    CallOnAThread(Runnable body) {
        thread = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable e) {
                thrown.set(e);
            } finally {
                returned.set(true);
            }
        }, "call-on-a-thread");
        thread.setDaemon(true);
        thread.start();
    }

    // Returns once the call has returned or its thread waits or is blocked, as a call that waits for a lock or a latch does
    void awaitReturnedOrWaiting() {
        EnumSet<Thread.State> waiting = EnumSet.of(Thread.State.WAITING, Thread.State.TIMED_WAITING, Thread.State.BLOCKED);
        await().atMost(Duration.ofSeconds(10)).until(() -> returned.get() || waiting.contains(thread.getState()));
    }

    boolean hasReturned() {
        return returned.get();
    }

    void awaitEnded() {
        try {
            thread.join(15_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(thread.isAlive()).as("whether the call is still running 15 seconds after the test released what held it").isFalse();
        assertThat(thrown.get()).as("what the call threw").isNull();
    }
}
