/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.eventstore.api;

import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.MultiOperandCondition;
import org.occurrent.condition.Condition.MultiOperandConditionName;
import org.occurrent.condition.Condition.SingleOperandCondition;
import org.occurrent.condition.Condition.SingleOperandConditionName;

import java.util.Objects;
import java.util.stream.Stream;

/**
 * Evaluates {@link Condition} of type {@link Long} to {@code true} or {@code false}.
 */
public class LongConditionEvaluator {

    public static boolean evaluate(Condition<Long> condition, long value) {
        Objects.requireNonNull(condition, "Condition cannot be null");

        if (condition instanceof MultiOperandCondition<Long> operation) {
            MultiOperandConditionName operationName = operation.operationName();
            Stream<Condition<Long>> operations = operation.operations().stream();
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
        } else if (condition instanceof SingleOperandCondition<Long> singleOperandCondition) {
            long operand = singleOperandCondition.operand();
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.operandConditionName();
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