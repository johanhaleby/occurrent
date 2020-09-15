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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

class RFC3339Test {

    @ParameterizedTest
    @MethodSource("rfc3339Data")
    void rfc3339_date_time_conversion_test(String dateTimeString, OffsetDateTime expected) {
        // When
        OffsetDateTime actual = OffsetDateTime.from(RFC_3339_DATE_TIME_FORMATTER.parse(dateTimeString));

        // Then
        assertThat(expected).isEqualTo(actual);
    }

    private static Stream<Arguments> rfc3339Data() {
        return Stream.of(
                Arguments.of("2007-05-01T15:43:26+07:00", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26.3+07:00", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452+07:00", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26-07:00", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3-07:00", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452-07:00", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452Z", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), UTC)),
                Arguments.of("2007-05-01T15:43:26.3Z", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), UTC)),
                Arguments.of("2007-05-01T15:43:26Z", OffsetDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), UTC))
        );
    }

}