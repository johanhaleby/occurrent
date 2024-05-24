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

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class DurationToTimeoutConverterPropertyBasedTest {

    @Property
    public void timeout_is_always_greater_than_zero_regardless_of_supplied_time_unit(@ForAll Duration duration, @ForAll TimeUnit timeUnit) {
        Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, timeUnit);
        assertAll(
                () -> assertThat(result.timeout()).isGreaterThanOrEqualTo(0),
                () -> assertThat(result.timeUnit()).isEqualTo(timeUnit)
        );
    }

    @Property
    public void timeout_is_always_greater_than_zero_with_time_unit_seconds(@ForAll Duration duration) {
        Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, SECONDS);
        assertAll(
                () -> assertThat(result.timeout()).isGreaterThanOrEqualTo(0),
                () -> assertThat(result.timeUnit()).isEqualTo(SECONDS)
        );
    }

    @Property
    public void timeout_is_always_greater_than_zero_with_time_unit_nanos(@ForAll Duration duration) {
        Timeout result = DurationToTimeoutConverter.convertDurationToTimeout(duration, NANOSECONDS);
        assertAll(
                () -> assertThat(result.timeout()).isGreaterThanOrEqualTo(0),
                () -> assertThat(result.timeUnit()).isEqualTo(NANOSECONDS)
        );
    }
}