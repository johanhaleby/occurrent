package se.haleby.occurrent.eventstore.api;

import se.haleby.occurrent.eventstore.api.Condition.MultiOperandCondition;
import se.haleby.occurrent.eventstore.api.Condition.MultiOperandConditionName;
import se.haleby.occurrent.eventstore.api.Condition.SingleOperandCondition;
import se.haleby.occurrent.eventstore.api.Condition.SingleOperandConditionName;

import java.util.Objects;
import java.util.stream.Stream;

/**
 * Evaluates {@link Condition} of type {@link Long} to {@code true} or {@code false}.
 */
public class LongConditionEvaluator {

    public static boolean evaluate(Condition<Long> condition, long value) {
        Objects.requireNonNull(condition, "Condition cannot be null");

        if (condition instanceof MultiOperandCondition) {
            MultiOperandCondition<Long> operation = (MultiOperandCondition<Long>) condition;
            MultiOperandConditionName operationName = operation.operationName;
            Stream<Condition<Long>> operations = operation.operations.stream();
            switch (operationName) {
                case AND:
                    return operations.allMatch(c -> evaluate(c, value));
                case OR:
                    return operations.anyMatch(c -> evaluate(c, value));
                case NOT:
                    return operations.noneMatch(c -> evaluate(c, value));
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else if (condition instanceof SingleOperandCondition) {
            SingleOperandCondition<Long> singleOperandCondition = (SingleOperandCondition<Long>) condition;
            long operand = singleOperandCondition.operand;
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
            switch (singleOperandConditionName) {
                case EQ:
                    return value == operand;
                case LT:
                    return value < operand;
                case GT:
                    return value > operand;
                case LTE:
                    return value <= operand;
                case GTE:
                    return value >= operand;
                case NE:
                    return value != operand;
                default:
                    throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }
}