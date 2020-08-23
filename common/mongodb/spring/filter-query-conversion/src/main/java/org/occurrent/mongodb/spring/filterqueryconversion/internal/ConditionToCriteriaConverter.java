package org.occurrent.mongodb.spring.filterqueryconversion.internal;

import org.springframework.data.mongodb.core.query.Criteria;
import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.MultiOperandCondition;
import org.occurrent.condition.Condition.SingleOperandCondition;

import java.util.List;

/**
 * Converts a {@link Condition} into a Spring MongoDB {@link Criteria}.
 */
public class ConditionToCriteriaConverter {

    public static <T> Criteria convertConditionToCriteria(String fieldName, Condition<T> condition) {
        if (condition instanceof Condition.MultiOperandCondition) {
            MultiOperandCondition<T> operation = (MultiOperandCondition<T>) condition;
            Condition.MultiOperandConditionName operationName = operation.operationName;
            List<Condition<T>> operations = operation.operations;
            Criteria[] criteria = operations.stream().map(c -> convertConditionToCriteria(fieldName, c)).toArray(Criteria[]::new);
            switch (operationName) {
                case AND:
                    return new Criteria().andOperator(criteria);
                case OR:
                    return new Criteria().orOperator(criteria);
                case NOT:
                    return new Criteria().norOperator(criteria);
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else if (condition instanceof SingleOperandCondition) {
            SingleOperandCondition<T> singleOperandCondition = (SingleOperandCondition<T>) condition;
            T value = singleOperandCondition.operand;
            Condition.SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
            switch (singleOperandConditionName) {
                case EQ:
                    return Criteria.where(fieldName).is(value);
                case LT:
                    return Criteria.where(fieldName).lt(value);
                case GT:
                    return Criteria.where(fieldName).gt(value);
                case LTE:
                    return Criteria.where(fieldName).lte(value);
                case GTE:
                    return Criteria.where(fieldName).gte(value);
                case NE:
                    return Criteria.where(fieldName).ne(value);
                default:
                    throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }
}
