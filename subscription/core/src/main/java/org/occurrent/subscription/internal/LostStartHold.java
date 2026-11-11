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

package org.occurrent.subscription.internal;

import org.jspecify.annotations.Nullable;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Asks a subscribe on the current thread to hold a subscription paused, instead of refusing it with
 * {@code CatchupCannotResumeException}, when the subscription model no longer has the history back to the position
 * stored for it. The Spring Boot starter asks it for a subscription with {@code startupMode = BACKGROUND}, so one
 * subscription with a lost position doesn't stop the application from starting. A blocking catch-up holds such a
 * subscription paused and logs the loss at {@code ERROR}. A competing-consumer model holds it paused as if the user
 * had paused it. A manual-start model keeps it with a subscribe it defers, and asks it again for the subscribe it makes
 * once the subscription is started, which can be on another thread. Nothing else reads it, so a subscribe that isn't
 * asked refuses as before.
 * <p>
 * Read on the thread that subscribes. A call that a model makes on a thread of its own never sees it.
 */
public final class LostStartHold {

    private static final ThreadLocal<Boolean> ASKED = new ThreadLocal<>();
    // Set by the model that holds a subscription paused, for the askedDuringCall under way on this thread
    private static final ThreadLocal<boolean[]> HELD = new ThreadLocal<>();

    private LostStartHold() {
    }

    /**
     * Runs {@code subscribe} with the hold asked for on this thread, and puts back what was asked before once
     * {@code subscribe} returns or throws.
     */
    public static void askedDuring(Runnable subscribe) {
        requireNonNull(subscribe, "subscribe cannot be null");
        runWith(Boolean.TRUE, () -> {
            subscribe.run();
            return Boolean.TRUE;
        });
    }

    /**
     * Runs {@code call} with the hold asked for on this thread, and puts back what was asked before once {@code call}
     * returns or throws. The answer says whether a model held a subscription paused on this thread during
     * {@code call}, see {@link #heldPaused()}.
     */
    public static <T> Answer<T> askedDuringCall(Supplier<T> call) {
        requireNonNull(call, "call cannot be null");
        boolean @Nullable [] before = HELD.get();
        boolean[] held = new boolean[1];
        HELD.set(held);
        try {
            T result = runWith(Boolean.TRUE, call);
            return new Answer<>(result, held[0]);
        } finally {
            if (before == null) {
                HELD.remove();
            } else {
                HELD.set(before);
            }
        }
    }

    /**
     * Records that the model calling it held a subscription paused because its position is lost, for the
     * {@link #askedDuringCall(Supplier)} under way on this thread. Does nothing outside one.
     */
    public static void heldPaused() {
        boolean @Nullable [] held = HELD.get();
        if (held != null) {
            held[0] = true;
        }
    }

    /**
     * Returned by {@link #askedDuringCall(Supplier)}, with what {@code call} returned and whether a model held a
     * subscription paused on this thread during it.
     */
    public record Answer<T>(T result, boolean heldPaused) {
    }

    /**
     * Runs {@code call} with the hold not asked for on this thread, for a model that holds the subscription itself and
     * needs the model it wraps to refuse.
     */
    public static <T> T suspendedDuring(Supplier<T> call) {
        return runWith(null, call);
    }

    /**
     * Whether a subscribe on this thread was asked to hold a subscription with a lost position paused.
     */
    public static boolean isAsked() {
        return ASKED.get() != null;
    }

    private static <T> T runWith(@Nullable Boolean asked, Supplier<T> call) {
        requireNonNull(call, "call cannot be null");
        @Nullable Boolean before = ASKED.get();
        set(asked);
        try {
            return call.get();
        } finally {
            set(before);
        }
    }

    private static void set(@Nullable Boolean asked) {
        if (asked == null) {
            ASKED.remove();
        } else {
            ASKED.set(asked);
        }
    }
}
