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

package org.occurrent.broker.rabbitmq.blocking;

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * What {@link RabbitMqCloudEventSource} and {@code RabbitMqDomainEventSource} share when they close. Public only
 * because the two sources live in different packages. Not part of the supported API.
 */
public final class RabbitMqTeardown {

    // Half of the longest span nanoTime differences can express, so adding it to System.nanoTime() never wraps past
    // the point where a deadline comparison by subtraction stops working.
    private static final long LONGEST_TIMEOUT_NANOS = Long.MAX_VALUE / 2;
    private static final Duration LONGEST_TIMEOUT = Duration.ofNanos(LONGEST_TIMEOUT_NANOS);

    private final @Nullable Throwable primary;
    private @Nullable Throwable firstFailure;

    /**
     * @param primary The failure already on its way out of the method that tears down, or {@code null} when there is
     *                none. A step that fails is added to it as suppressed, so it is the one the caller sees.
     */
    public RabbitMqTeardown(@Nullable Throwable primary) {
        this.primary = primary;
    }

    /**
     * Runs {@code step}, and every step after it, whatever happens. Whatever it throws, a checked exception thrown
     * from Kotlin included, is added as suppressed to the primary failure, or to the first failed step when there is
     * no primary one. After an {@link InterruptedException} the interrupt status of the thread is set again, so the
     * steps after it, and the caller, see that the thread was asked to stop.
     */
    public RabbitMqTeardown run(Runnable step) {
        try {
            step.run();
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (primary != null) {
                attachSuppressed(primary, failure);
            } else if (firstFailure == null) {
                firstFailure = failure;
            } else {
                attachSuppressed(firstFailure, failure);
            }
        }
        return this;
    }

    /**
     * Throws the first failed step when there was no primary failure, otherwise nothing. A checked exception is
     * thrown as it is, undeclared, the way the step threw it.
     */
    public void finish() {
        if (firstFailure != null) {
            throw RabbitMqTeardown.<RuntimeException>sneakyThrow(firstFailure);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    /**
     * Adds {@code secondary} to {@code primary} as suppressed, unless it is {@code primary}, is already suppressed by
     * it, or is in its chain of causes. {@link Throwable#addSuppressed(Throwable)} refuses the first of those.
     */
    public static void attachSuppressed(Throwable primary, Throwable secondary) {
        if (primary == secondary) {
            return;
        }
        for (Throwable suppressed : primary.getSuppressed()) {
            if (suppressed == secondary) {
                return;
            }
        }
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = primary.getCause(); cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause == secondary) {
                return;
            }
        }
        primary.addSuppressed(secondary);
    }

    /**
     * {@code timeout} in nanoseconds, or the longest span a deadline can be set that far ahead, about 146 years, when
     * {@code timeout} is longer than that. {@link Duration#toNanos()} throws instead.
     */
    public static long timeoutNanos(Duration timeout) {
        return timeout.compareTo(LONGEST_TIMEOUT) > 0 ? LONGEST_TIMEOUT_NANOS : timeout.toNanos();
    }
}
