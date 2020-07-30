package se.haleby.occurrent.eventstore.mongodb.nativedriver.internal;

import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import se.haleby.occurrent.eventstore.api.Condition;
import se.haleby.occurrent.eventstore.api.Filter;
import se.haleby.occurrent.eventstore.api.Filter.All;
import se.haleby.occurrent.eventstore.api.Filter.CompositionFilter;
import se.haleby.occurrent.eventstore.api.Filter.SingleConditionFilter;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.eventstore.mongodb.internal.SpecialFilterHandling.resolveSpecialCases;
import static se.haleby.occurrent.eventstore.mongodb.nativedriver.internal.ConditionConverter.convertConditionToBsonCriteria;

public class FilterToBsonFilterConverter {

    public static Bson convertFilterToBsonFilter(TimeRepresentation timeRepresentation, Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(timeRepresentation, "TimeRepresentation cannot be null");

        final Bson query;
        if (filter instanceof All) {
            query = new BsonDocument();
        } else {
            query = innerConvert(timeRepresentation, filter);
        }
        return query;
    }

    private static Bson innerConvert(TimeRepresentation timeRepresentation, Filter filter) {
        final Bson criteria;
        if (filter instanceof All) {
            criteria = new BsonDocument();
        } else if (filter instanceof SingleConditionFilter) {
            SingleConditionFilter scf = (SingleConditionFilter) filter;
            Condition<?> conditionToUse = resolveSpecialCases(timeRepresentation, scf);
            criteria = convertConditionToBsonCriteria(scf.fieldName, conditionToUse);
        } else if (filter instanceof CompositionFilter) {
            CompositionFilter cf = (CompositionFilter) filter;
            Bson[] composedBson = cf.filters.stream().map(f -> innerConvert(timeRepresentation, f)).toArray(Bson[]::new);
            switch (cf.operator) {
                case AND:
                    criteria = Filters.and(composedBson);
                    break;
                case OR:
                    criteria = Filters.or(composedBson);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + cf.operator);
            }
        } else {
            throw new IllegalStateException("Unexpected filter: " + filter.getClass().getName());
        }
        return criteria;
    }
}