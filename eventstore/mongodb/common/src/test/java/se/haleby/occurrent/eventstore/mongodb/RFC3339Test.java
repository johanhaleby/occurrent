package se.haleby.occurrent.eventstore.mongodb;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static se.haleby.occurrent.eventstore.mongodb.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

class RFC3339Test {

    @ParameterizedTest
    @MethodSource("rfc3339Data")
    void rfc3339_date_time_conversion_test(String dateTimeString, ZonedDateTime expected) {
        // When
        ZonedDateTime actual = ZonedDateTime.from(RFC_3339_DATE_TIME_FORMATTER.parse(dateTimeString));

        // Then
        assertThat(expected).isEqualTo(actual);
    }

    private static Stream<Arguments> rfc3339Data() {
        return Stream.of(
                Arguments.of("2007-05-01T15:43:26+07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26.3+07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452+07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26-07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3-07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452-07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452Z", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), UTC)),
                Arguments.of("2007-05-01T15:43:26.3Z", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), UTC)),
                Arguments.of("2007-05-01T15:43:26Z", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), UTC))
        );
    }

}