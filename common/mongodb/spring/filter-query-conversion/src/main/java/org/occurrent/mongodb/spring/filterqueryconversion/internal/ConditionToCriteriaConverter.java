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

package org.occurrent.mongodb.spring.filterqueryconversion.internal;

import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.MultiOperandCondition;
import org.occurrent.condition.Condition.SingleOperandCondition;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.Collection;
import java.util.List;

/**
 * Converts a {@link Condition} into a Spring MongoDB {@link Criteria}.
 */
public class ConditionToCriteriaConverter {

    public static <T> Criteria convertConditionToCriteria(String fieldName, Condition<T> condition) {
        if (condition instanceof MultiOperandCondition<T> operation) {
            Condition.MultiOperandConditionName operationName = operation.operationName();
            List<Condition<T>> operations = operation.operations();
            Criteria[] criteria = operations.stream().map(c -> convertConditionToCriteria(fieldName, c)).toArray(Criteria[]::new);
            return switch (operationName) {
                case AND -> new Criteria().andOperator(criteria);
                case OR -> new Criteria().orOperator(criteria);
                case NOT -> new Criteria().norOperator(criteria);
            };
        } else if (condition instanceof SingleOperandCondition<T> singleOperandCondition) {
            T value = singleOperandCondition.operand();
            Condition.SingleOperandConditionName singleOperandConditionName = singleOperandCondition.operandConditionName();
            return switch (singleOperandConditionName) {
                case EQ -> Criteria.where(fieldName).is(value);
                case LT -> Criteria.where(fieldName).lt(value);
                case GT -> Criteria.where(fieldName).gt(value);
                case LTE -> Criteria.where(fieldName).lte(value);
                case GTE -> Criteria.where(fieldName).gte(value);
                case NE -> Criteria.where(fieldName).ne(value);
            };
        } else if (condition instanceof Condition.InOperandCondition<T> inOperandCondition) {
            Collection<T> operand = inOperandCondition.operand();
            return Criteria.where(fieldName).in(operand);
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }
}
