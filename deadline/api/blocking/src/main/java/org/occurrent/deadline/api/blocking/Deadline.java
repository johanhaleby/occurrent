/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.deadline.api.blocking;

import java.time.*;
import java.util.Date;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import static java.time.ZoneOffset.UTC;

/**
 * A deadline is a date/time in the future when a deadline is up.
 */
public sealed interface Deadline {
    /**
     * @return Return the deadline time as a {@link Date}.
     */
    default Date toDate() {
        if (this instanceof InstantDeadLine) {
            return Date.from(toInstant());
        } else if (this instanceof ZonedDateTimeDeadLine) {
            return Date.from(toInstant());
        } else if (this instanceof OffsetDateTimeDeadLine) {
            return Date.from(toInstant());
        } else if (this instanceof LocalDateTimeDeadLine) {
            return Date.from(toInstant());
        } else {
            throw new IllegalStateException("Internal error: Cannot convert " + this.getClass().getSimpleName() + " to a " + Date.class.getName());
        }
    }

    /**
     * @return Return the deadline time as an {@link Instant}.
     */
    default Instant toInstant() {
        if (this instanceof InstantDeadLine) {
            return ((InstantDeadLine) this).instant;
        } else if (this instanceof ZonedDateTimeDeadLine) {
            return ((ZonedDateTimeDeadLine) this).zonedDateTime.toInstant();
        } else if (this instanceof OffsetDateTimeDeadLine) {
            return ((OffsetDateTimeDeadLine) this).offsetDateTime.toInstant();
        } else if (this instanceof LocalDateTimeDeadLine) {
            return ((LocalDateTimeDeadLine) this).localDateTime.toInstant(UTC);
        } else {
            throw new IllegalStateException("Internal error: Cannot convert " + this.getClass().getSimpleName() + " to a " + Date.class.getName());
        }
    }

    /**
     * @return Return the deadline time as epoch millis.
     */
    default long toEpochMilli() {
        return toInstant().toEpochMilli();
    }

    /**
     * Create a deadline that'll take place at {@code epochMilli}.
     */
    static Deadline ofEpochMilli(long epochMilli) {
        return new InstantDeadLine(Instant.ofEpochMilli(epochMilli));
    }

    /**
     * Create a deadline that'll take place at the supplied {@code date}.
     */
    static Deadline of(Date date) {
        Objects.requireNonNull(date, Date.class.getSimpleName() + " cannot be null");
        return new InstantDeadLine(date.toInstant());
    }

    /**
     * Create a deadline that'll take place at the supplied {@code instant}.
     */
    static Deadline of(Instant instant) {
        Objects.requireNonNull(instant, Instant.class.getSimpleName() + " cannot be null");
        return new InstantDeadLine(instant);
    }

    /**
     * Create a deadline that'll take place at the supplied {@code zonedDateTime}.
     */
    static Deadline of(ZonedDateTime zonedDateTime) {
        Objects.requireNonNull(zonedDateTime, ZonedDateTime.class.getSimpleName() + " cannot be null");
        return new ZonedDateTimeDeadLine(zonedDateTime);
    }

    /**
     * Create a deadline that'll take place at the supplied {@code offsetDateTime}.
     */
    static Deadline of(OffsetDateTime offsetDateTime) {
        Objects.requireNonNull(offsetDateTime, OffsetDateTime.class.getSimpleName() + " cannot be null");
        return new OffsetDateTimeDeadLine(offsetDateTime);
    }

    /**
     * Create a deadline that'll take place at the supplied {@code localDateTime}.
     */
    static Deadline of(LocalDateTime localDateTime) {
        Objects.requireNonNull(localDateTime, LocalDateTime.class.getSimpleName() + " cannot be null");
        return new LocalDateTimeDeadLine(localDateTime);
    }

    /**
     * Create a deadline that'll take place as soon as possible
     */
    static Deadline asap() {
        return new InstantDeadLine(Instant.now());
    }

    /**
     * Create a deadline that'll take place as after the supplied number of {@code millis}
     */
    static Deadline afterMillis(long millis) {
        return new InstantDeadLine(Instant.now().plusMillis(millis));
    }

    /**
     * Create a deadline that'll take place as after the supplied number of {@code seconds}
     */
    static Deadline afterSeconds(long seconds) {
        return afterMillis(TimeUnit.SECONDS.toMillis(seconds));
    }

    /**
     * Create a deadline that'll take place as after the supplied number of {@code minutes}
     */
    static Deadline afterMinutes(long minutes) {
        return afterMillis(TimeUnit.MINUTES.toMillis(minutes));
    }

    /**
     * Create a deadline that'll take place as after the supplied number of {@code hours}
     */
    static Deadline afterHours(long hours) {
        return afterMillis(TimeUnit.HOURS.toMillis(hours));
    }

    /**
     * Create a deadline that'll take place as after the supplied number of {@code days}
     */
    static Deadline afterDays(long days) {
        return afterMillis(TimeUnit.DAYS.toMillis(days));
    }

    /**
     * Create a deadline that'll take place as after the supplied number of {@code weeks}
     */
    static Deadline afterWeeks(long weeks) {
        return afterMillis(TimeUnit.DAYS.toMillis(weeks * 7));
    }

    /**
     * Create a deadline that'll take place as after the supplied {@code duration}
     */
    static Deadline afterDuration(Duration duration) {
        return afterMillis(duration.toMillis());
    }

    /**
     * Create a deadline that'll take place as after the supplied {@code time} and {@code timeUnit}
     */
    static Deadline afterTime(long time, TimeUnit timeUnit) {
        return afterMillis(timeUnit.toMillis(time));
    }

    final class ZonedDateTimeDeadLine implements Deadline {
        final ZonedDateTime zonedDateTime;

        ZonedDateTimeDeadLine(ZonedDateTime zonedDateTime) {
            Objects.requireNonNull(zonedDateTime, ZonedDateTime.class.getSimpleName() + " cannot be null");
            this.zonedDateTime = zonedDateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ZonedDateTimeDeadLine that)) return false;
            return Objects.equals(zonedDateTime, that.zonedDateTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(zonedDateTime);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", ZonedDateTimeDeadLine.class.getSimpleName() + "[", "]")
                    .add("zonedDateTime=" + zonedDateTime)
                    .toString();
        }
    }

    final class OffsetDateTimeDeadLine implements Deadline {
        final OffsetDateTime offsetDateTime;

        OffsetDateTimeDeadLine(OffsetDateTime offsetDateTime) {
            Objects.requireNonNull(offsetDateTime, OffsetDateTime.class.getSimpleName() + " cannot be null");
            this.offsetDateTime = offsetDateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OffsetDateTimeDeadLine that)) return false;
            return Objects.equals(offsetDateTime, that.offsetDateTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offsetDateTime);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", OffsetDateTimeDeadLine.class.getSimpleName() + "[", "]")
                    .add("offsetDateTime=" + offsetDateTime)
                    .toString();
        }
    }

    final class LocalDateTimeDeadLine implements Deadline {
        final LocalDateTime localDateTime;

        LocalDateTimeDeadLine(LocalDateTime localDateTime) {
            Objects.requireNonNull(localDateTime, LocalDateTime.class.getSimpleName() + " cannot be null");
            this.localDateTime = localDateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LocalDateTimeDeadLine that)) return false;
            return Objects.equals(localDateTime, that.localDateTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(localDateTime);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", LocalDateTimeDeadLine.class.getSimpleName() + "[", "]")
                    .add("localDateTime=" + localDateTime)
                    .toString();
        }
    }

    final class InstantDeadLine implements Deadline {
        final Instant instant;

        InstantDeadLine(Instant instant) {
            Objects.requireNonNull(instant, Instant.class.getSimpleName() + " cannot be null");
            this.instant = instant;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof InstantDeadLine that)) return false;
            return Objects.equals(instant, that.instant);
        }

        @Override
        public int hashCode() {
            return Objects.hash(instant);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", InstantDeadLine.class.getSimpleName() + "[", "]")
                    .add("instant=" + instant)
                    .toString();
        }
    }
}