/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription;

import org.jspecify.annotations.NullMarked;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A utility class that can safely convert a {@link java.time.Duration} to a {@code timeout} and {@link java.util.concurrent.TimeUnit}.
 */
@NullMarked
public class DurationToTimeoutConverter {

    public record Timeout(long timeout, TimeUnit timeUnit) {
    }

    /**
     * Convert any duration instance safely to a valid "Timeout" with the must appropriate TimeUnit
     *
     * @param duration The duration
     * @return The {@link Timeout}
     */
    public static Timeout convertDurationToTimeout(Duration duration) {
        if (duration.isNegative()) {
            return new Timeout(0, TimeUnit.MILLISECONDS);
        }

        long timeout;
        TimeUnit timeUnit;


        if (duration.equals(ChronoUnit.FOREVER.getDuration())) {
            timeout = Long.MAX_VALUE;
            timeUnit = TimeUnit.DAYS;
        } else if (checkDurationSafely(() -> duration.toMillis() < TimeUnit.SECONDS.toMillis(1))) {
            timeout = duration.toMillis();
            timeUnit = TimeUnit.MILLISECONDS;
        } else if (checkDurationSafely(() -> duration.toSeconds() < TimeUnit.MINUTES.toSeconds(1))) {
            timeout = duration.getSeconds();
            timeUnit = TimeUnit.SECONDS;
        } else if (checkDurationSafely(() -> duration.toMinutes() < TimeUnit.HOURS.toMinutes(1))) {
            timeout = duration.toMinutes();
            timeUnit = TimeUnit.MINUTES;
        } else if (checkDurationSafely(() -> duration.toHours() < TimeUnit.DAYS.toHours(1))) {
            timeout = duration.toHours();
            timeUnit = TimeUnit.HOURS;
        } else {
            timeout = duration.toDays();
            timeUnit = TimeUnit.DAYS;
        }

        return new Timeout(timeout, timeUnit);
    }

    private static boolean checkDurationSafely(Supplier<Boolean> check) {
        try {
            return check.get();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Convert any duration instance safely to a valid "Timeout", if the duration is too large to fit into the
     * supplied TimeUnit, the largest possible value for the TimeUnit will be used.
     *
     * @param duration The duration
     * @return The {@link Timeout}
     */
    public static Timeout convertDurationToTimeout(Duration duration, TimeUnit timeUnit) {
        if (duration.isNegative()) {
            return new Timeout(0, timeUnit);
        }

        Timeout durationTimeout = getDurationAsTimeout(duration);
        long timeout;
        try {
            // Convert the Duration to the specified TimeUnit safely
            timeout = timeUnit.convert(durationTimeout.timeout, durationTimeout.timeUnit);
        } catch (ArithmeticException ex) {
            timeout = switch (timeUnit) {
                case NANOSECONDS -> Long.MAX_VALUE;
                case MICROSECONDS -> Long.MAX_VALUE / 1_000;
                case MILLISECONDS -> Long.MAX_VALUE / 1_000_000;
                case SECONDS -> Long.MAX_VALUE / 1_000_000_000;
                case MINUTES -> Long.MAX_VALUE / (1_000_000_000 * 60L);
                case HOURS -> Long.MAX_VALUE / (1_000_000_000 * 60L * 60L);
                case DAYS -> Long.MAX_VALUE / (1_000_000_000 * 60L * 60L * 24L);
            };
        }

        return new Timeout(timeout, timeUnit);
    }

    public static Timeout getDurationAsTimeout(Duration duration) {
        try {
            long nanos = duration.toNanos();
            if (nanos < 1_000) {
                return new Timeout(nanos, TimeUnit.NANOSECONDS);
            } else if (nanos < 1_000_000) {
                return new Timeout(duration.toMillis() * 1000 + duration.getNano() / 1_000, TimeUnit.MICROSECONDS);
            } else if (nanos < 1_000_000_000) {
                return new Timeout(duration.toMillis(), TimeUnit.MILLISECONDS);
            } else {
                return getTimeoutFromDurationSeconds(duration);
            }
        } catch (ArithmeticException ex) {
            return getTimeoutFromDurationSeconds(duration);
        }
    }

    private static Timeout getTimeoutFromDurationSeconds(Duration duration) {
        long seconds = duration.getSeconds();
        if (seconds < 60) {
            return new Timeout(seconds, TimeUnit.SECONDS);
        } else if (seconds < 3600) {
            return new Timeout(duration.toMinutes(), TimeUnit.MINUTES);
        } else if (seconds < 86400) { // 24 * 60 * 60
            return new Timeout(duration.toHours(), TimeUnit.HOURS);
        } else {
            return new Timeout(duration.toDays(), TimeUnit.DAYS);
        }
    }
}