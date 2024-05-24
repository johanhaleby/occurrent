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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("DurationToTimeoutConverter")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurationToTimeoutConverterTest {

    @Nested
    @DisplayName("convertDurationToTimeout when not TimeUnit specified")
    class ConvertDurationToTimeoutWithoutTimeUnit {

        @Test
        void Milliseconds() {
            Duration duration = Duration.ofMillis(500);
            Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration);
            assertThat(result.timeout()).isEqualTo(500);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.MILLISECONDS);
        }

        @Test
        void Seconds() {
            Duration duration = Duration.ofSeconds(5);
            Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration);
            assertThat(result.timeout()).isEqualTo(5);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.SECONDS);
        }

        @Test
        void Minutes() {
            Duration duration = Duration.ofMinutes(2);
            Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration);
            assertThat(result.timeout()).isEqualTo(2);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.MINUTES);
        }

        @Test
        void Hours() {
            Duration duration = Duration.ofHours(3);
            Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration);
            assertThat(result.timeout()).isEqualTo(3);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.HOURS);
        }

        @Test
        void Days() {
            Duration duration = Duration.ofDays(10);
            Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration);
            assertThat(result.timeout()).isEqualTo(10);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.DAYS);
        }

        @Test
        void very_long_duration_that_is_not_forever() {
            Duration duration = Duration.of(Integer.MAX_VALUE, MINUTES);
            Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration);
            assertAll(
                    () -> assertThat(result.timeout()).isEqualTo(1491308),
                    () -> assertThat(result.timeUnit()).isEqualTo(TimeUnit.DAYS)
            );
        }

        @Test
        void Forever() {
            Duration duration = ChronoUnit.FOREVER.getDuration();
            Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration);
            assertThat(result.timeout()).isEqualTo(Long.MAX_VALUE);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.DAYS);
        }
    }

    @Nested
    @DisplayName("convertDurationToTimeout when TimeUnit specified")
    class ConvertDurationToTimeoutWithTimeUnit {

        @Test
        public void RequestedMilliseconds() {
            Duration duration = Duration.ofMillis(500);
            DurationToTimeoutConverter.Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, TimeUnit.MILLISECONDS);
            assertThat(result.timeout()).isEqualTo(500);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.MILLISECONDS);
        }

        @Test
        public void RequestedSeconds() {
            Duration duration = Duration.ofSeconds(5);
            DurationToTimeoutConverter.Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, TimeUnit.SECONDS);
            assertThat(result.timeout()).isEqualTo(5);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.SECONDS);
        }

        @Test
        public void RequestedMinutes() {
            Duration duration = Duration.ofMinutes(2);
            DurationToTimeoutConverter.Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, TimeUnit.MINUTES);
            assertThat(result.timeout()).isEqualTo(2);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.MINUTES);
        }

        @Test
        public void RequestedHours() {
            Duration duration = Duration.ofHours(3);
            DurationToTimeoutConverter.Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, TimeUnit.HOURS);
            assertThat(result.timeout()).isEqualTo(3);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.HOURS);
        }

        @Test
        public void RequestedDays() {
            Duration duration = Duration.ofDays(10);
            DurationToTimeoutConverter.Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, TimeUnit.DAYS);
            assertThat(result.timeout()).isEqualTo(10);
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.DAYS);
        }

        @Test
        public void Forever() {
            Duration duration = ChronoUnit.FOREVER.getDuration();
            DurationToTimeoutConverter.Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, TimeUnit.DAYS);
            assertThat(result.timeout()).isEqualTo(duration.toDays());
            assertThat(result.timeUnit()).isEqualTo(TimeUnit.DAYS);
        }

        @Test
        public void LongDuration() {
            Duration duration = Duration.of(Long.MAX_VALUE, ChronoUnit.MILLIS);
            DurationToTimeoutConverter.Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, TimeUnit.DAYS);
            assertAll(
                    () -> assertThat(result.timeout()).isEqualTo(duration.toDays()),
                    () -> assertThat(result.timeUnit()).isEqualTo(TimeUnit.DAYS)
            );
        }
    }
}