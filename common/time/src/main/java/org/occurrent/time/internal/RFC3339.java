/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.time.internal;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * Utilities for RFC3339 date/time conversions.
 */
public class RFC3339 {

    public static final DateTimeFormatter RFC_3339_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_DATE_TIME)
            .optionalStart()
            .appendOffset("+HH:MM", "Z")
            .optionalEnd()
            .toFormatter();

    /**
     * A canonical, fixed-width RFC 3339 formatter that always writes the fractional-second
     * component with exactly 9 digits (nanosecond precision), e.g. {@code 2026-07-28T12:00:00.000000000Z}.
     * <p>
     * This exists so that stored string representations of a time (under {@code TimeRepresentation.RFC_3339_STRING})
     * are always the same length for a given precision, which means they sort chronologically when compared
     * byte-by-byte (as MongoDB does for string comparisons). {@link #RFC_3339_DATE_TIME_FORMATTER} does not have
     * this property because it omits the fractional part entirely, or writes it in groups of 3, 6 or 9 digits,
     * depending on the value.
     * <p>
     * Do <b>not</b> use this formatter in place of {@link #RFC_3339_DATE_TIME_FORMATTER} for anything that reads
     * or writes durable state already serialized with the latter (for example {@code TimeBasedCheckpoint.asString()}),
     * since doing so would change the on-disk representation of already-persisted values.
     */
    public static final DateTimeFormatter RFC_3339_FIXED_WIDTH_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendFraction(ChronoField.NANO_OF_SECOND, 9, 9, true)
            .appendOffset("+HH:MM", "Z")
            .toFormatter();
}
