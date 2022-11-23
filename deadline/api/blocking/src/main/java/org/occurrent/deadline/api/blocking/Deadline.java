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

public abstract class Deadline {
    private Deadline() {
    }

    public Date toDate() {
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

    public Instant toInstant() {
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

    public long toEpochMilli() {
        return toInstant().toEpochMilli();
    }

    public static Deadline ofEpochMilli(long epochMilli) {
        return new InstantDeadLine(Instant.ofEpochMilli(epochMilli));
    }

    public static Deadline of(Date date) {
        Objects.requireNonNull(date, Date.class.getSimpleName() + " cannot be null");
        return new InstantDeadLine(date.toInstant());
    }

    public static Deadline of(Instant instant) {
        Objects.requireNonNull(instant, Instant.class.getSimpleName() + " cannot be null");
        return new InstantDeadLine(instant);
    }

    public static Deadline of(ZonedDateTime zonedDateTime) {
        Objects.requireNonNull(zonedDateTime, ZonedDateTime.class.getSimpleName() + " cannot be null");
        return new ZonedDateTimeDeadLine(zonedDateTime);
    }

    public static Deadline of(OffsetDateTime offsetDateTime) {
        Objects.requireNonNull(offsetDateTime, OffsetDateTime.class.getSimpleName() + " cannot be null");
        return new OffsetDateTimeDeadLine(offsetDateTime);
    }

    public static Deadline of(LocalDateTime localDateTime) {
        Objects.requireNonNull(localDateTime, LocalDateTime.class.getSimpleName() + " cannot be null");
        return new LocalDateTimeDeadLine(localDateTime);
    }

    public static Deadline asap() {
        return new InstantDeadLine(Instant.now());
    }

    public static Deadline afterMillis(long millis) {
        return new InstantDeadLine(Instant.now().plusMillis(millis));
    }

    public static Deadline afterSeconds(long seconds) {
        return afterMillis(TimeUnit.SECONDS.toMillis(seconds));
    }

    public static Deadline afterMinutes(long minutes) {
        return afterMillis(TimeUnit.MINUTES.toMillis(minutes));
    }

    public static Deadline afterHours(long hours) {
        return afterMillis(TimeUnit.HOURS.toMillis(hours));
    }

    public static Deadline afterDays(long days) {
        return afterMillis(TimeUnit.DAYS.toMillis(days));
    }

    public static Deadline afterDuration(Duration duration) {
        return afterMillis(duration.toMillis());
    }

    public static Deadline afterTime(long time, TimeUnit timeUnit) {
        return afterMillis(timeUnit.toMillis(time));
    }

    static class ZonedDateTimeDeadLine extends Deadline {
        final ZonedDateTime zonedDateTime;

        ZonedDateTimeDeadLine(ZonedDateTime zonedDateTime) {
            Objects.requireNonNull(zonedDateTime, ZonedDateTime.class.getSimpleName() + " cannot be null");
            this.zonedDateTime = zonedDateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ZonedDateTimeDeadLine)) return false;
            ZonedDateTimeDeadLine that = (ZonedDateTimeDeadLine) o;
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

    static class OffsetDateTimeDeadLine extends Deadline {
        final OffsetDateTime offsetDateTime;

        OffsetDateTimeDeadLine(OffsetDateTime offsetDateTime) {
            Objects.requireNonNull(offsetDateTime, OffsetDateTime.class.getSimpleName() + " cannot be null");
            this.offsetDateTime = offsetDateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OffsetDateTimeDeadLine)) return false;
            OffsetDateTimeDeadLine that = (OffsetDateTimeDeadLine) o;
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

    static class LocalDateTimeDeadLine extends Deadline {
        final LocalDateTime localDateTime;

        LocalDateTimeDeadLine(LocalDateTime localDateTime) {
            Objects.requireNonNull(localDateTime, LocalDateTime.class.getSimpleName() + " cannot be null");
            this.localDateTime = localDateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LocalDateTimeDeadLine)) return false;
            LocalDateTimeDeadLine that = (LocalDateTimeDeadLine) o;
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

    static class InstantDeadLine extends Deadline {
        final Instant instant;

        public InstantDeadLine(Instant instant) {
            Objects.requireNonNull(instant, Instant.class.getSimpleName() + " cannot be null");
            this.instant = instant;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof InstantDeadLine)) return false;
            InstantDeadLine that = (InstantDeadLine) o;
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