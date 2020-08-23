package org.occurrent.mongodb.spring.filterbsonfilterconversion.internal;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.MultiOperandCondition;
import org.occurrent.condition.Condition.SingleOperandCondition;
import org.occurrent.condition.Condition.SingleOperandConditionName;

import java.util.List;

/**
 * Converts a {@link Condition} into a {@link Bson} document.
 */
public class ConditionConverter {

    public static <T> Bson convertConditionToBsonCriteria(String fieldName, Condition<T> condition) {
        if (condition instanceof MultiOperandCondition) {
            MultiOperandCondition<T> operation = (MultiOperandCondition<T>) condition;
            Condition.MultiOperandConditionName operationName = operation.operationName;
            List<Condition<T>> operations = operation.operations;
            Bson[] filters = operations.stream().map(c -> convertConditionToBsonCriteria(fieldName, c)).toArray(Bson[]::new);
            switch (operationName) {
                case AND:
                    return Filters.and(filters);
                case OR:
                    return Filters.or(filters);
                case NOT:
                    return Filters.not(filters[0]);
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else if (condition instanceof SingleOperandCondition) {
            SingleOperandCondition<T> singleOperandCondition = (SingleOperandCondition<T>) condition;
            T expectedVersion = singleOperandCondition.operand;
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
            switch (singleOperandConditionName) {
                case EQ:
                    return Filters.eq(fieldName, expectedVersion);
                case LT:
                    return Filters.lt(fieldName, expectedVersion);
                case GT:
                    return Filters.gt(fieldName, expectedVersion);
                case LTE:
                    return Filters.lte(fieldName, expectedVersion);
                case GTE:
                    return Filters.gte(fieldName, expectedVersion);
                case NE:
                    return Filters.ne(fieldName, expectedVersion);
                default:
                    throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }
}
