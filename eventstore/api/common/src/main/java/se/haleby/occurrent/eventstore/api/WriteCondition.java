package se.haleby.occurrent.eventstore.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static se.haleby.occurrent.eventstore.api.WriteCondition.Condition.eq;
import static se.haleby.occurrent.eventstore.api.WriteCondition.MultiOperationName.*;
import static se.haleby.occurrent.eventstore.api.WriteCondition.OperationName.*;

// TODO Add Any stream version!!
public abstract class WriteCondition {

    private WriteCondition() {
    }

    public static WriteCondition streamVersionEq(long version) {
        return streamVersion(eq(version));
    }

    public static WriteCondition streamVersion(Condition<Long> condition) {
        return StreamVersionWriteCondition.streamVersion(condition);
    }

    public static class StreamVersionWriteCondition extends WriteCondition {
        public final Condition<Long> condition;

        private StreamVersionWriteCondition(Condition<Long> condition) {
            this.condition = condition;
        }

        public static StreamVersionWriteCondition streamVersion(Condition<Long> condition) {
            return new StreamVersionWriteCondition(condition);
        }

        @Override
        public String toString() {
            return condition.description;
        }
    }

    public abstract static class Condition<T> {
        private final String description;

        private Condition(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }

        public static class Operation<T> extends Condition<T> {
            public final OperationName operationName;
            public final T operand;

            private Operation(OperationName operationName, T operand, String description) {
                super(description);
                this.operationName = operationName;
                this.operand = operand;
            }
        }

        public static class MultiOperation<T> extends Condition<T> {
            public final MultiOperationName operationName;
            public final List<Condition<T>> operations;

            private MultiOperation(final MultiOperationName operationName, List<Condition<T>> operations, String description) {
                super(description);
                this.operationName = operationName;
                this.operations = Collections.unmodifiableList(operations);
            }
        }

        public static <T> Condition<T> eq(T t) {
            return new Operation<>(EQ, t, String.format("to be equal to %s", t));
        }

        public static <T> Condition<T> lt(T t) {
            return new Operation<>(LT, t, String.format("to be less than %s", t));
        }

        public static <T> Condition<T> gt(T t) {
            return new Operation<>(GT, t, String.format("to be greater than %s", t));
        }

        public static <T> Condition<T> lte(T t) {
            return new Operation<>(LTE, t, String.format("to be less than or equal to %s", t));
        }

        public static <T> Condition<T> gte(T t) {
            return new Operation<>(GTE, t, String.format("to be greater than or equal to %s", t));
        }

        public static <T> Condition<T> ne(T t) {
            return new Operation<>(NE, t, String.format("to not be equal to %s", t));
        }

        @SafeVarargs
        public static <T> Condition<T> and(Condition<T> firstCondition, Condition<T> secondCondition, Condition<T>... additionalConditions) {
            List<Condition<T>> conditions = createConditionsFrom(firstCondition, secondCondition, additionalConditions);
            return new MultiOperation<>(AND, conditions, conditions.stream().map(Condition::toString).collect(Collectors.joining(" and ")));
        }

        @SafeVarargs
        public static <T> Condition<T> or(Condition<T> firstCondition, Condition<T> secondCondition, Condition<T>... additionalConditions) {
            List<Condition<T>> conditions = createConditionsFrom(firstCondition, secondCondition, additionalConditions);
            return new MultiOperation<>(OR, conditions, conditions.stream().map(Condition::toString).collect(Collectors.joining(" or ")));
        }

        public static <T> Condition<T> not(Condition<T> condition) {
            return new MultiOperation<>(NOT, Collections.singletonList(condition), "not " + condition);
        }

        private static <T> List<Condition<T>> createConditionsFrom(Condition<T> firstCondition, Condition<T> secondCondition, Condition<T>[] additionalConditions) {
            List<Condition<T>> conditions = new ArrayList<>(2 + additionalConditions.length);
            conditions.add(firstCondition);
            conditions.add(secondCondition);
            Collections.addAll(conditions, additionalConditions);
            return conditions;
        }
    }

    public enum OperationName {
        EQ, LT, GT, LTE, GTE, NE
    }

    public enum MultiOperationName {
        AND, OR, NOT
    }
}