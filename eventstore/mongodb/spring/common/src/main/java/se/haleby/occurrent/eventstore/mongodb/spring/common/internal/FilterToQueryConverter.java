package se.haleby.occurrent.eventstore.mongodb.spring.common.internal;

import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import se.haleby.occurrent.eventstore.api.Condition;
import se.haleby.occurrent.eventstore.api.Filter;
import se.haleby.occurrent.eventstore.api.Filter.All;
import se.haleby.occurrent.eventstore.api.Filter.CompositionFilter;
import se.haleby.occurrent.eventstore.api.Filter.SingleConditionFilter;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import java.sql.Date;
import java.time.ZonedDateTime;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.eventstore.api.Filter.TIME;
import static se.haleby.occurrent.eventstore.mongodb.RFC3339.RFC_3339_DATE_TIME_FORMATTER;
import static se.haleby.occurrent.eventstore.mongodb.TimeRepresentation.RFC_3339_STRING;
import static se.haleby.occurrent.eventstore.mongodb.spring.common.internal.ConditionToCriteriaConverter.convertConditionToCriteria;

public class FilterToQueryConverter {

    public static Query convertFilterToQuery(TimeRepresentation timeRepresentation, Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(timeRepresentation, "TimeRepresentation cannot be null");

        final Query query;
        if (filter instanceof All) {
            query = new Query();
        } else {
            query = Query.query(convertFilterToCriteria(timeRepresentation, filter));
        }
        return query;
    }

    private static Criteria convertFilterToCriteria(TimeRepresentation timeRepresentation, Filter filter) {
        final Criteria criteria;
        if (filter instanceof All) {
            criteria = new Criteria();
        } else if (filter instanceof SingleConditionFilter) {
            SingleConditionFilter scf = (SingleConditionFilter) filter;
            Condition<?> conditionToUse = resolveSpecialCases(timeRepresentation, scf);
            criteria = convertConditionToCriteria(scf.fieldName, conditionToUse);
        } else if (filter instanceof CompositionFilter) {
            CompositionFilter cf = (CompositionFilter) filter;
            Criteria[] composedCriteria = cf.filters.stream().map(f -> FilterToQueryConverter.convertFilterToCriteria(timeRepresentation, f)).toArray(Criteria[]::new);
            Criteria c = new Criteria();
            switch (cf.operator) {
                case AND:
                    c.andOperator(composedCriteria);
                    break;
                case OR:
                    c.orOperator(composedCriteria);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + cf.operator);
            }
            criteria = c;
        } else {
            throw new IllegalStateException("Unexpected filter: " + filter.getClass().getName());
        }
        return criteria;
    }


    @SuppressWarnings("unchecked")
    private static Condition<?> resolveSpecialCases(TimeRepresentation timeRepresentation, SingleConditionFilter scf) {
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
