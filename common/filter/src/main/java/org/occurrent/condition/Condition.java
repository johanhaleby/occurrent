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

package org.occurrent.condition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.occurrent.condition.Condition.MultiOperandConditionName.*;
import static org.occurrent.condition.Condition.SingleOperandConditionName.*;

/**
 * Conditions that can be used when querying an event store or filtering a subscription when applied to a {@code Filter}.
 *
 * @param <T> The type of the value in the condition
 */
public sealed interface Condition<T> {

    String description();

    <T2> Condition<T2> map(Function<T, T2> fn);

    record SingleOperandCondition<T>(SingleOperandConditionName operandConditionName, T operand, String description) implements Condition<T> {

        public SingleOperandCondition {
            requireNonNull(operandConditionName, "Operand condition name cannot be null");
            requireNonNull(operand, "Operand cannot be null");
            requireNonNull(description, "Description cannot be null");
        }

        @Override
        public <T2> Condition<T2> map(Function<T, T2> fn) {
            requireNonNull(fn, "Mapping function cannot be null");
            return new SingleOperandCondition<>(operandConditionName, fn.apply(operand), description);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    record MultiOperandCondition<T>(MultiOperandConditionName operationName, List<Condition<T>> operations, String description) implements Condition<T> {

        public MultiOperandCondition {
            requireNonNull(operationName, "Operation name cannot be null");
            requireNonNull(operations, "Operations cannot be null");
            requireNonNull(description, "Description cannot be null");
        }

        @Override
        public <T2> Condition<T2> map(Function<T, T2> fn) {
            return new MultiOperandCondition<>(operationName, operations.stream().map(condition -> condition.map(fn)).collect(Collectors.toList()), description);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    static <T> Condition<T> eq(T t) {
        return new SingleOperandCondition<>(EQ, t, String.format("to be equal to %s", t));
    }

    static <T> Condition<T> lt(T t) {
        return new SingleOperandCondition<>(LT, t, String.format("to be less than %s", t));
    }

    static <T> Condition<T> gt(T t) {
        return new SingleOperandCondition<>(GT, t, String.format("to be greater than %s", t));
    }

    static <T> Condition<T> lte(T t) {
        return new SingleOperandCondition<>(LTE, t, String.format("to be less than or equal to %s", t));
    }

    static <T> Condition<T> gte(T t) {
        return new SingleOperandCondition<>(GTE, t, String.format("to be greater than or equal to %s", t));
    }

    static <T> Condition<T> ne(T t) {
        return new SingleOperandCondition<>(NE, t, String.format("to not be equal to %s", t));
    }

    @SafeVarargs
    static <T> Condition<T> and(Condition<T> firstCondition, Condition<T> secondCondition, Condition<T>... additionalConditions) {
        List<Condition<T>> conditions = createList(firstCondition, secondCondition, additionalConditions);
        return and(conditions);
    }

    static <T> Condition<T> and(List<Condition<T>> conditions) {
        return new MultiOperandCondition<>(AND, conditions, conditions.stream().map(Condition::toString).collect(Collectors.joining(" and ")));
    }

    /**
     * Special case of {@link #and(Condition, Condition, Condition[])} where each element will be mapped to equal conditions ({@link #eq(Object)}).
     */
    @SafeVarargs
    static <T> Condition<T> and(T first, T second, T... additional) {
        List<Condition<T>> conditions = createList(first, second, additional).stream().map(Condition::eq).collect(Collectors.toList());
        return and(conditions);
    }

    /**
     * Special case of {@link #or(Condition, Condition, Condition[])} where each element will be mapped to equal conditions ({@link #eq(Object)}).
     */
    @SafeVarargs
    static <T> Condition<T> or(T first, T second, T... additional) {
        List<Condition<T>> conditions = createList(first, second, additional).stream().map(Condition::eq).collect(Collectors.toList());
        return or(conditions);
    }

    @SafeVarargs
    static <T> Condition<T> or(Condition<T> firstCondition, Condition<T> secondCondition, Condition<T>... additionalConditions) {
        List<Condition<T>> conditions = createList(firstCondition, secondCondition, additionalConditions);
        return or(conditions);
    }

    static <T> Condition<T> or(List<Condition<T>> conditions) {
        return new MultiOperandCondition<>(OR, conditions, conditions.stream().map(Condition::toString).collect(Collectors.joining(" or ")));
    }

    static <T> Condition<T> not(Condition<T> condition) {
        return new MultiOperandCondition<>(NOT, Collections.singletonList(condition), "not " + condition);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> createList(T firstCondition, T secondCondition, T[] additionalConditions) {
        requireNonNull(firstCondition, "First condition cannot be null");
        requireNonNull(secondCondition, "Second condition cannot be null");
        T[] additionalConditionsToUse = additionalConditions == null ? (T[]) Collections.emptyList().toArray() : additionalConditions;
        List<T> conditions = new ArrayList<>(2 + additionalConditionsToUse.length);
        conditions.add(firstCondition);
        conditions.add(secondCondition);
        Collections.addAll(conditions, additionalConditionsToUse);
        return conditions;
    }

    enum SingleOperandConditionName {
        EQ, LT, GT, LTE, GTE, NE
    }

    enum MultiOperandConditionName {
        AND, OR, NOT
    }
}