package org.occurrent.eventstore.jpa.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.filter.Filter;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;

/**
 * A mapper for all-things query related:
 * - Filter
 * - Condition
 * - SortBy
 */
@Component
class QueryMapper {

    public Specification<CloudEventEntity> mapFilter(Filter aFilter) {
        if (aFilter instanceof Filter.All) {
            return Specification.unrestricted();
        } else if (aFilter instanceof Filter.SingleConditionFilter single) {
            return (root, query, builder) -> {
                Expression<?> field = mapFieldName(root, single.fieldName());
                return mapCondition(field, single.condition(), builder);
            };
        } else if (aFilter instanceof Filter.CompositionFilter composition) {
            List<Specification<CloudEventEntity>> specs = composition.filters().stream()
                    .map(this::mapFilter)
                    .toList();
            return switch (composition.operator()) {
                case OR -> Specification.anyOf(specs);
                case AND -> Specification.allOf(specs);
            };
        }
        throw new IllegalArgumentException("Unknown filter type: " + aFilter.getClass());
    }

    Expression<?> mapFieldName(Root<CloudEventEntity> root, String fieldName) {
        return switch (fieldName) {
            case "id" -> root.get("eventId");
            case "datacontenttype" -> root.get("dataContentType");
            case "dataschema" -> root.get("dataSchema");
            case "streamid" -> root.get("stream").get("name");
            case "streamversion" -> root.get("streamPosition");
            default -> root.get(fieldName);
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Predicate mapCondition(Expression field, Condition aCondition, CriteriaBuilder builder) {
        if (aCondition instanceof Condition.InOperandCondition<?> inOperand) {
            var inClause = builder.in(field);
            for (Object op : inOperand.operand()) {
                inClause.value(op);
            }
            return inClause;
        } else if (aCondition instanceof Condition.SingleOperandCondition<?> singleOperand) {


            Comparable operandValue = (Comparable) singleOperand.operand();
            // Special case: cast the operand back to an URI ...
            if (field.getJavaType() == URI.class) {
                operandValue = URI.create(singleOperand.operand().toString());
            }
            return switch (singleOperand.operandConditionName()) {
                case EQ -> builder.equal(field, operandValue);
                case NE -> builder.notEqual(field, operandValue);
                case GT -> builder.greaterThan(field, operandValue);
                case GTE -> builder.greaterThanOrEqualTo(field, operandValue);
                case LT -> builder.lessThan(field, operandValue);
                case LTE -> builder.lessThanOrEqualTo(field, operandValue);
            };
        } else if (aCondition instanceof Condition.MultiOperandCondition<?> multiOperand) {

            Predicate[] conditions = multiOperand.operations().stream()
                    .map(c -> mapCondition(field, c, builder))
                    .toArray(Predicate[]::new);
            return switch (multiOperand.operationName()) {
                case AND -> builder.and(conditions);
                case OR -> builder.or(conditions);
                case NOT -> builder.not(conditions[0]);
            };
        }
        throw new IllegalArgumentException("Unknown condition type: " + aCondition.getClass());
    }

    public Sort mapSortBy(SortBy aSortBy) {
        if (aSortBy instanceof SortBy.Unsorted) {
            return Sort.unsorted();
        } else if (aSortBy instanceof SortBy.NaturalImpl natural) {
            return Sort.by(mapDirection(natural.direction), "streamPosition");
        } else if (aSortBy instanceof SortBy.SingleFieldImpl singleFieldSort) {
            return Sort.by(mapDirection(singleFieldSort.direction), mapFieldName(singleFieldSort.fieldName));
        } else if (aSortBy instanceof SortBy.MultipleSortStepsImpl multipleSort) {
            Sort sort = Sort.unsorted();
            for (SortBy sortBy : multipleSort.steps) {
                sort = sort.and(mapSortBy(sortBy));
            }
            return sort;
        }
        throw new IllegalArgumentException("Unknown SortBy type: " + aSortBy.getClass());
    }

    String mapFieldName(String fieldName) {
        return switch (fieldName.toLowerCase()) {
            case "id" -> "eventId";
            case "datacontenttype" -> "dataContentType";
            case "dataschema" -> "dataSchema";
            case "streamid" -> "stream.name";
            case "streamversion" -> "streamPosition";
            default -> fieldName;
        };
    }

    Sort.Direction mapDirection(SortBy.SortDirection aDirection) {
        return switch (aDirection) {
            case ASCENDING -> Sort.Direction.ASC;
            case DESCENDING -> Sort.Direction.DESC;
        };
    }

}
