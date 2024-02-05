package org.occurrent.eventstore.jpa.domain;

import static java.time.ZoneOffset.UTC;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

public class TimeConversion {
  public static Date toDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atOffset(UTC).toInstant());
  }

  public static LocalDateTime toLocalDateTime(Date date) {
    return LocalDateTime.ofInstant(date.toInstant(), UTC);
  }

  public static OffsetDateTime offsetDateTimeFrom(LocalDateTime ldf, ZoneId zoneId) {
    return ZonedDateTime.of(ldf, zoneId).toOffsetDateTime();
  }
}
