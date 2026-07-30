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

package org.occurrent.tck.eventstore.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.EventStoreCapability;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.tck.ConformanceEvents.eventAt;

/**
 * How precisely a store keeps a CloudEvent's {@code time} attribute, and what it does with a value it cannot represent.
 * <p>
 * Extend it from a test class per store, as described on {@link EventStoreConformance}. A store supporting more than
 * one time representation should extend it once per representation, since the answer differs per representation rather
 * than per store:
 * <pre>{@code
 * class PostgresqlEventStoreTimePrecisionTest extends EventStoreTimePrecisionConformance {
 *     @Override
 *     protected EventStoreFixture createFixture() { ... }
 * }
 * }</pre>
 * <p>
 * This exists because the other suites cannot see the problem. They all write the same fixed instant, which has zero
 * seconds and zero nanoseconds, so a store that quietly dropped every sub-second digit would pass all of them. A
 * timestamp is the record of when something happened, and a rounded one cannot be told apart afterwards from an
 * accurate one, so the contract is that a store refuses what it cannot hold rather than storing something else.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the event store time precision contract")
public abstract class EventStoreTimePrecisionConformance extends EventStoreConformance {

    private static final String STREAM_ID = "name";
    private static final String DEFINED = "NameDefined";

    /**
     * Deliberately not the fixed instant the other suites use. Every component is non-zero, so a store cannot pass by
     * rounding one away, and the millisecond, microsecond and nanosecond digits differ so a truncation shows up as a
     * wrong value rather than a coincidentally equal one.
     */
    private static final OffsetDateTime NANOSECOND_TIME =
            OffsetDateTime.of(2026, 7, 28, 12, 34, 56, 123_456_789, ZoneOffset.UTC);

    private static final OffsetDateTime MILLISECOND_TIME = NANOSECOND_TIME.truncatedTo(ChronoUnit.MILLIS);

    private static final OffsetDateTime NON_UTC_TIME =
            MILLISECOND_TIME.withOffsetSameInstant(ZoneOffset.ofHours(2));

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Test
    void round_trips_millisecond_precision() {
        // The floor. Every representation a store could reasonably pick holds at least milliseconds, so this needs no
        // declaration and applies to all of them.
        eventStore().write(STREAM_ID, List.of(eventAt("a", DEFINED, MILLISECOND_TIME)));

        CloudEvent read = eventStore().read(STREAM_ID).eventList().getFirst();

        assertThat(read.getTime()).isEqualTo(MILLISECOND_TIME);
    }

    @Test
    void keeps_or_refuses_a_nanosecond_time_according_to_what_the_fixture_declares() {
        CloudEvent event = eventAt("a", DEFINED, NANOSECOND_TIME);

        if (fixture().timePrecision() == ChronoUnit.NANOS) {
            eventStore().write(STREAM_ID, List.of(event));

            assertThat(eventStore().read(STREAM_ID).eventList().getFirst().getTime())
                    .describedAs("a store declaring nanosecond precision must give the instant back unchanged")
                    .isEqualTo(NANOSECOND_TIME);
        } else {
            Throwable thrown = catchThrowable(() -> eventStore().write(STREAM_ID, List.of(event)));

            assertAll(
                    () -> assertThat(thrown)
                            .describedAs("a store that cannot hold nanoseconds must refuse the write rather than "
                                    + "storing a rounded instant, which nothing downstream could detect")
                            .isInstanceOf(IllegalArgumentException.class),
                    () -> assertThat(eventStore().exists(STREAM_ID))
                            .describedAs("a refused write must leave nothing behind")
                            .isFalse()
            );
        }
    }

    @Test
    void keeps_or_refuses_a_non_utc_time_according_to_what_the_fixture_declares() {
        CloudEvent event = eventAt("a", DEFINED, NON_UTC_TIME);

        if (fixture().preservesTimeOffset()) {
            eventStore().write(STREAM_ID, List.of(event));

            OffsetDateTime read = eventStore().read(STREAM_ID).eventList().getFirst().getTime();

            // The offset is asserted on its own, because AssertJ compares an OffsetDateTime with
            // OffsetDateTime.timeLineOrder(), which only compares the instant. A store that normalised +02:00 to Z
            // would satisfy isEqualTo while having lost exactly what this test is about.
            assertAll(
                    () -> assertThat(read)
                            .describedAs("the instant must survive")
                            .isEqualTo(NON_UTC_TIME),
                    () -> assertThat(read.getOffset())
                            .describedAs("a store declaring that it preserves the offset must give back the same "
                                    + "offset, not the same instant rewritten to UTC")
                            .isEqualTo(NON_UTC_TIME.getOffset())
            );
        } else {
            Throwable thrown = catchThrowable(() -> eventStore().write(STREAM_ID, List.of(event)));

            assertAll(
                    () -> assertThat(thrown)
                            .describedAs("a store that cannot hold an offset must refuse the write rather than "
                                    + "silently rewriting it to UTC")
                            .isInstanceOf(IllegalArgumentException.class),
                    () -> assertThat(eventStore().exists(STREAM_ID))
                            .describedAs("a refused write must leave nothing behind")
                            .isFalse()
            );
        }
    }
}
