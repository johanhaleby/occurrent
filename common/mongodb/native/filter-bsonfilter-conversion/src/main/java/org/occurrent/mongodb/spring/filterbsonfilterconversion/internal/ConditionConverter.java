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
        return switch (condition) {
            case MultiOperandCondition<T> operation -> convertMultiOperandConditionToBsonCriteria(fieldName, operation);
            case SingleOperandCondition<T> singleOperandCondition -> convertSingleOperandConditionToBsonCriteria(fieldName, singleOperandCondition);
            case Condition.InOperandCondition<T> inOperandCondition -> Filters.in(fieldName, inOperandCondition.operand());
        };
    }

    private static <T> Bson convertMultiOperandConditionToBsonCriteria(String fieldName, MultiOperandCondition<T> operation) {
            Condition.MultiOperandConditionName operationName = operation.operationName();
            List<Condition<T>> operations = operation.operations();
            Bson[] filters = operations.stream().map(c -> convertConditionToBsonCriteria(fieldName, c)).toArray(Bson[]::new);
            return switch (operationName) {
                case AND -> Filters.and(filters);
                case OR -> Filters.or(filters);
                case NOT -> Filters.not(filters[0]);
            };
    }

    private static <T> Bson convertSingleOperandConditionToBsonCriteria(String fieldName, SingleOperandCondition<T> singleOperandCondition) {
            T operand = singleOperandCondition.operand();
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.operandConditionName();
            return switch (singleOperandConditionName) {
                case EQ -> Filters.eq(fieldName, operand);
                case LT -> Filters.lt(fieldName, operand);
                case GT -> Filters.gt(fieldName, operand);
                case LTE -> Filters.lte(fieldName, operand);
                case GTE -> Filters.gte(fieldName, operand);
                case NE -> Filters.ne(fieldName, operand);
            };
    }
}
