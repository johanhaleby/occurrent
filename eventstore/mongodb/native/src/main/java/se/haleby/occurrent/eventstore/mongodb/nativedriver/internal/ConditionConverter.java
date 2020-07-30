package se.haleby.occurrent.eventstore.mongodb.nativedriver.internal;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import se.haleby.occurrent.eventstore.api.Condition;

import java.util.List;

import static com.mongodb.client.model.Filters.*;

public class ConditionConverter {

    public static <T> Bson convertConditionToBsonCriteria(String fieldName, Condition<T> condition) {
        if (condition instanceof Condition.MultiOperandCondition) {
            Condition.MultiOperandCondition<T> operation = (Condition.MultiOperandCondition<T>) condition;
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
        } else if (condition instanceof Condition.SingleOperandCondition) {
            Condition.SingleOperandCondition<T> singleOperandCondition = (Condition.SingleOperandCondition<T>) condition;
            T expectedVersion = singleOperandCondition.operand;
            Condition.SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
            switch (singleOperandConditionName) {
                case EQ:
                    return eq(fieldName, expectedVersion);
                case LT:
                    return lt(fieldName, expectedVersion);
                case GT:
                    return gt(fieldName, expectedVersion);
                case LTE:
                    return lte(fieldName, expectedVersion);
                case GTE:
                    return gte(fieldName, expectedVersion);
                case NE:
                    return ne(fieldName, expectedVersion);
                default:
                    throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }
}
