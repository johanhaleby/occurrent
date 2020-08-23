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
