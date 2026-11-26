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
import java.math.BigInteger;

import static java.util.Objects.requireNonNull;

/**
 * An extension of a skipped input that is a {@code Number} of a class the saga state store doesn't hold as that class,
 * such as an {@code AtomicLong}, and the number a release hands back for it. {@code InMemorySagaStateStore} and
 * {@code SpringMongoSagaStateStore} work that number out here, so a release hands back the same value as the same class
 * from either, and both log the same warning.
 *
 * @param extension the name of the extension
 * @param delivered the class of the number the event was delivered with
 * @param released  the number a release hands back, of a class every store holds
 */
public record SkippedInputNumber(String extension, Class<? extends Number> delivered, Number released) {

    public SkippedInputNumber {
        requireNonNull(extension, "extension cannot be null");
        requireNonNull(delivered, "delivered cannot be null");
        requireNonNull(released, "released cannot be null");
    }

    /**
     * The extension {@code extension} delivered as {@code number}, whose value this reads once, with the number
     * {@link #releasedAs(Number)} gives for it.
     */
    public static SkippedInputNumber of(String extension, Number number) {
        requireNonNull(number, "number cannot be null");
        return new SkippedInputNumber(extension, number.getClass(), releasedAs(number));
    }

    /**
     * The number a release hands back for {@code number}. A subclass of {@code BigDecimal} or {@code BigInteger} comes
     * back as a plain {@code BigDecimal} or {@code BigInteger}, a class every store holds, with the value its
     * {@code BigDecimal} or {@code BigInteger} methods give, and a {@code BigDecimal} keeps the scale they give. Any
     * other number's value is the decimal its {@code toString} writes, which is how Jackson writes such a number. It's a
     * {@code Long} when that decimal is an integer that fits in one, and otherwise a {@code BigDecimal} with the same
     * value and scale. When {@code toString} doesn't write a decimal, as for {@code NaN} or an infinity, it's the
     * {@code doubleValue} as a {@code Double}.
     */
    public static Number releasedAs(Number number) {
        requireNonNull(number, "number cannot be null");
        return switch (number) {
            case BigDecimal decimal -> new BigDecimal(decimal.unscaledValue(), decimal.scale());
            case BigInteger integer -> new BigInteger(integer.toByteArray());
            default -> fromDecimal(number);
        };
    }

    private static Number fromDecimal(Number number) {
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
     * Whether the delivered class extends the class a release hands back, as a subclass of {@code BigDecimal} does.
     */
    public boolean releasedAsItsSuperclass() {
        return released.getClass().isAssignableFrom(delivered);
    }

    /**
     * Logs a warning that saga instance {@code sagaId} holds this extension of the input {@code inputKey} it skips as
     * the class a release hands back. The warning names the instance, the extension, the input and both classes, and
     * never the value. {@code InMemorySagaStateStore} and {@code SpringMongoSagaStateStore} log it only for an input
     * they write the record of, and inside a transaction of the caller's that is before the caller commits it.
     */
    public void warn(Logger log, String sagaId, String inputKey) {
        log.warn("Saga instance '{}' can't hold the extension '{}' of the input '{}' it skips as a {}, so a release hands it back as a {}.",
                sagaId, extension, inputKey, delivered.getName(), released.getClass().getName());
    }
}
