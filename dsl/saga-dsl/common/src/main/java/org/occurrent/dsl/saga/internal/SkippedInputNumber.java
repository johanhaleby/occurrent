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

package org.occurrent.dsl.saga.internal;

import org.slf4j.Logger;

import java.math.BigDecimal;

import static java.util.Objects.requireNonNull;

/**
 * The value a saga state store hands back for an extension of a skipped input that is a {@code Number} of a class the
 * store doesn't hold as that class, such as an {@code AtomicLong}. Every store works the value out here, so a release
 * hands back the same value as the same class from any of them, and every store logs the same warning.
 */
public final class SkippedInputNumber {

    private SkippedInputNumber() {
    }

    /**
     * The number a release hands back for {@code number}. Its value is the decimal its {@code toString} writes, which is
     * how Jackson writes such a number. It's a {@code Long} when that decimal is an integer that fits in one, and
     * otherwise a {@code BigDecimal} with the same value and scale. When {@code toString} doesn't write a decimal, as
     * for {@code NaN}, it's the {@code doubleValue} as a {@code Double}.
     */
    public static Number releasedAs(Number number) {
        requireNonNull(number, "number cannot be null");
        String decimal = number.toString();
        if (decimal == null) {
            return number.doubleValue();
        }
        BigDecimal value;
        try {
            value = new BigDecimal(decimal);
        } catch (NumberFormatException e) {
            return number.doubleValue();
        }
        if (value.signum() == 0 || value.stripTrailingZeros().scale() <= 0) {
            try {
                return value.longValueExact();
            } catch (ArithmeticException e) {
                // Past a Long
            }
        }
        return value;
    }

    /**
     * Logs a warning that saga instance {@code sagaId} holds the extension {@code extension} of the input
     * {@code inputKey} it skips as {@code released}, the number {@link #releasedAs(Number)} gives for {@code delivered}.
     * The warning names the instance, the extension, the input and both classes, and never the value.
     */
    public static void warnHeldAs(Logger log, String sagaId, String inputKey, String extension, Number delivered, Number released) {
        log.warn("Saga instance '{}' can't hold the extension '{}' of the input '{}' it skips as a {}, so a release hands it back as a {}.",
                sagaId, extension, inputKey, delivered.getClass().getName(), released.getClass().getName());
    }
}
