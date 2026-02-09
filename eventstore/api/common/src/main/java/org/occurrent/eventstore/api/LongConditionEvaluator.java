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

import org.jspecify.annotations.NullMarked;
import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.*;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Evaluates {@link Condition} of type {@link Long} to {@code true} or {@code false}.
 */
@NullMarked
public class LongConditionEvaluator {

    public static boolean evaluate(Condition<Long> condition, long value) {
        Objects.requireNonNull(condition, "Condition cannot be null");

        if (condition instanceof MultiOperandCondition<Long> operation) {
            MultiOperandConditionName operationName = operation.operationName();
            Stream<Condition<Long>> operations = operation.operations().stream();
            return switch (operationName) {
                case AND -> operations.allMatch(c -> evaluate(c, value));
                case OR -> operations.anyMatch(c -> evaluate(c, value));
                case NOT -> operations.noneMatch(c -> evaluate(c, value));
            };
        } else if (condition instanceof SingleOperandCondition<Long> singleOperandCondition) {
            long operand = singleOperandCondition.operand();
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.operandConditionName();
            return switch (singleOperandConditionName) {
                case EQ -> value == operand;
                case LT -> value < operand;
                case GT -> value > operand;
                case LTE -> value <= operand;
                case GTE -> value >= operand;
                case NE -> value != operand;
            };
        } else if (condition instanceof InOperandCondition<Long> inOperandCondition) {
            Collection<Long> longs = inOperandCondition.operand();
            return longs.contains(value);
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }
}