package se.haleby.occurrent.eventstore.mongodb.internal;

import se.haleby.occurrent.condition.Condition;
import se.haleby.occurrent.filter.Filter.SingleConditionFilter;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import java.sql.Date;
import java.time.ZonedDateTime;

import static se.haleby.occurrent.filter.Filter.TIME;
import static se.haleby.occurrent.eventstore.mongodb.TimeRepresentation.RFC_3339_STRING;
import static se.haleby.occurrent.eventstore.mongodb.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * Some filters need to be treated specially, for example they may be dependent on the EventStore configuration.
 */
public class SpecialFilterHandling {

    @SuppressWarnings("unchecked")
    public static Condition<?> resolveSpecialCases(TimeRepresentation timeRepresentation, SingleConditionFilter scf) {
        if (TIME.equals(scf.fieldName)) {
            Condition<ZonedDateTime> zdfCondition = (Condition<ZonedDateTime>) scf.condition;
            if (timeRepresentation == RFC_3339_STRING) {
                return zdfCondition.map(RFC_3339_DATE_TIME_FORMATTER::format);
            } else {
                return zdfCondition.map(zdf -> Date.from(zdf.toInstant()));
            }
        }
        return scf.condition;
    }
}