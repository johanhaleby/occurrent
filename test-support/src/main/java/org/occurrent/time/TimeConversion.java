package org.occurrent.time;

import java.time.LocalDateTime;
import java.util.Date;

import static java.time.ZoneOffset.UTC;

public class TimeConversion {
    public static Date toDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(UTC).toInstant());
    }

    public static LocalDateTime toLocalDateTime(Date date) {
        return LocalDateTime.ofInstant(date.toInstant(), UTC);
    }
}