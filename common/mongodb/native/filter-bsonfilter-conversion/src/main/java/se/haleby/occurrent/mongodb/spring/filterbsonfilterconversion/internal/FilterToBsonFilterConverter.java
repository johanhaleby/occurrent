package se.haleby.occurrent.mongodb.spring.filterbsonfilterconversion.internal;

import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import se.haleby.occurrent.condition.Condition;
import se.haleby.occurrent.filter.Filter;
import se.haleby.occurrent.filter.Filter.All;
import se.haleby.occurrent.filter.Filter.CompositionFilter;
import se.haleby.occurrent.filter.Filter.SingleConditionFilter;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.mongodb.specialfilterhandling.internal.SpecialFilterHandling.resolveSpecialCases;
import static se.haleby.occurrent.mongodb.spring.filterbsonfilterconversion.internal.ConditionConverter.convertConditionToBsonCriteria;

public class FilterToBsonFilterConverter {
    public static Bson convertFilterToBsonFilter(TimeRepresentation timeRepresentation, Filter filter) {
        return convertFilterToBsonFilter(null, timeRepresentation, filter);
    }

    public static Bson convertFilterToBsonFilter(String fieldNamePrefix, TimeRepresentation timeRepresentation, Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(timeRepresentation, "TimeRepresentation cannot be null");

        final Bson query;
        if (filter instanceof All) {
            query = new BsonDocument();
        } else {
            query = innerConvert(fieldNamePrefix, timeRepresentation, filter);
        }
        return query;
    }

    private static Bson innerConvert(String fieldNamePrefix, TimeRepresentation timeRepresentation, Filter filter) {
        final Bson criteria;
        if (filter instanceof All) {
            criteria = new BsonDocument();
        } else if (filter instanceof SingleConditionFilter) {
            SingleConditionFilter scf = (SingleConditionFilter) filter;
            Condition<?> conditionToUse = resolveSpecialCases(timeRepresentation, scf);
            String fieldName = fieldNameOf(fieldNamePrefix, scf.fieldName);
            criteria = convertConditionToBsonCriteria(fieldName, conditionToUse);
        } else if (filter instanceof CompositionFilter) {
            CompositionFilter cf = (CompositionFilter) filter;
            Bson[] composedBson = cf.filters.stream().map(f -> innerConvert(fieldNamePrefix, timeRepresentation, f)).toArray(Bson[]::new);
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

    private static String fieldNameOf(String fieldNamePrefix, String fieldName) {
        return fieldNamePrefix == null ? fieldName : fieldNamePrefix + "." + fieldName;
    }
}