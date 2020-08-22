package se.haleby.occurrent.mongodb.spring.filterqueryconversion.internal;

import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import se.haleby.occurrent.condition.Condition;
import se.haleby.occurrent.filter.Filter;
import se.haleby.occurrent.filter.Filter.All;
import se.haleby.occurrent.filter.Filter.CompositionFilter;
import se.haleby.occurrent.filter.Filter.SingleConditionFilter;
import se.haleby.occurrent.mongodb.specialfilterhandling.internal.SpecialFilterHandling;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.mongodb.specialfilterhandling.internal.SpecialFilterHandling.resolveSpecialCases;
import static se.haleby.occurrent.mongodb.spring.filterqueryconversion.internal.ConditionToCriteriaConverter.convertConditionToCriteria;

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

}
