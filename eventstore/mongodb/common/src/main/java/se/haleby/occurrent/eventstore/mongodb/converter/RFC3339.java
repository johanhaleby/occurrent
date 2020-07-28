package se.haleby.occurrent.eventstore.mongodb.converter;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

public class RFC3339 {

    public static final DateTimeFormatter RFC_3339_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
                .append(ISO_LOCAL_DATE_TIME)
                .optionalStart()
                .appendOffset("+HH:MM", "Z")
                .optionalEnd()
                .toFormatter();
}
