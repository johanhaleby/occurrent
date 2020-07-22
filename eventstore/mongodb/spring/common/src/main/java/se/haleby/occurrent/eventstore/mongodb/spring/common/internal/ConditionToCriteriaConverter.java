package se.haleby.occurrent.eventstore.mongodb.spring.common.internal;

import org.springframework.data.mongodb.core.query.Criteria;
import se.haleby.occurrent.eventstore.api.WriteCondition;
import se.haleby.occurrent.eventstore.api.WriteCondition.Condition;
import se.haleby.occurrent.eventstore.api.WriteCondition.Condition.MultiOperation;
import se.haleby.occurrent.eventstore.api.WriteCondition.Condition.Operation;

import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class ConditionToCriteriaConverter {

    public static Criteria convertConditionToCriteria(String fieldName, Condition<Long> condition) {
        if (condition instanceof MultiOperation) {
            MultiOperation<Long> operation = (MultiOperation<Long>) condition;
            WriteCondition.MultiOperationName operationName = operation.operationName;
            List<Condition<Long>> operations = operation.operations;
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
        } else if (condition instanceof Operation) {
            Operation<Long> operation = (Operation<Long>) condition;
            long expectedVersion = operation.operand;
            WriteCondition.OperationName operationName = operation.operationName;
            switch (operationName) {
                case EQ:
                    return where(fieldName).is(expectedVersion);
                case LT:
                    return where(fieldName).lt(expectedVersion);
                case GT:
                    return where(fieldName).gt(expectedVersion);
                case LTE:
                    return where(fieldName).lte(expectedVersion);
                case GTE:
                    return where(fieldName).gte(expectedVersion);
                case NE:
                    return where(fieldName).ne(expectedVersion);
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }
}
