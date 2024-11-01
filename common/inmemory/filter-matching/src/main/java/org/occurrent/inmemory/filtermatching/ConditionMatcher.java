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

package org.occurrent.inmemory.filtermatching;

import io.cloudevents.CloudEvent;
import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.MultiOperandCondition;
import org.occurrent.condition.Condition.SingleOperandCondition;
import org.occurrent.condition.Condition.SingleOperandConditionName;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.function.Predicate.isEqual;
import static org.occurrent.condition.Condition.SingleOperandConditionName.EQ;
import static org.occurrent.condition.Condition.SingleOperandConditionName.NE;
import static org.occurrent.filter.Filter.*;

/**
 * Check if a condition matches a certain cloud event
 */
public class ConditionMatcher {

    private static final Set<String> ATTRIBUTE_NAMES = Set.of(SPEC_VERSION, ID, TYPE, TIME, SOURCE, SUBJECT, DATA_SCHEMA, DATA_CONTENT_TYPE);

    public static <T> boolean matchesCondition(CloudEvent cloudEvent, String fieldName, Condition<T> condition) {
        if (condition instanceof MultiOperandCondition<T> operation) {
            Condition.MultiOperandConditionName operationName = operation.operationName();
            List<Condition<T>> operations = operation.operations();
            Stream<Boolean> filters = operations.stream().map(c -> matchesCondition(cloudEvent, fieldName, c));
            return switch (operationName) {
                case AND -> filters.allMatch(isEqual(true));
                case OR -> filters.anyMatch(isEqual(true));
                case NOT -> filters.allMatch(isEqual(false));
            };
        } else if (condition instanceof SingleOperandCondition<T> singleOperandCondition) {
            T expected = singleOperandCondition.operand();
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.operandConditionName();
            Object actual = extractValue(cloudEvent, fieldName);
            final boolean matches;
            if (singleOperandConditionName == EQ) {
                matches = Objects.equals(actual, expected);
            } else if (singleOperandConditionName == NE) {
                matches = !Objects.equals(actual, expected);
            } else {
                Comparable<Object> expectedComparable = toComparable(expected, "Expected value must implement " + Comparable.class.getName() + " in order to be used in Filter's");
                Comparable<Object> actualComparable = toComparable(actual, "Value in CloudEvent must implement " + Comparable.class.getName() + " in order to be used in Filter's");
                int comparisonResult = actualComparable.compareTo(expectedComparable);
                matches = switch (singleOperandConditionName) {
                    case LT -> comparisonResult < 0;
                    case GT -> comparisonResult > 0;
                    case LTE -> comparisonResult <= 0;
                    case GTE -> comparisonResult >= 0;
                    default -> throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
                };
            }
            return matches;
        } else if (condition instanceof Condition.InOperandCondition<T> inOperandCondition) {
            Object actual = extractValue(cloudEvent, fieldName);
            Collection<T> operand = inOperandCondition.operand();
            return operand.stream().anyMatch(it -> Objects.equals(it, actual));
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Comparable<Object> toComparable(T operand, String failureMessage) {
        if (!(operand instanceof Comparable)) {
            throw new IllegalArgumentException(failureMessage);
        }
        return (Comparable<Object>) operand;
    }

    private static Object extractValue(CloudEvent cloudEvent, String fieldName) {
        // TODO data if content-type is json
        if (fieldName != null && fieldName.startsWith(DATA + ".")) {
            throw new IllegalArgumentException("Currently, it's not possible to query the data field from in-memory event stores/subscriptions. The good thing is that Occurrent is open-source, so feel free to contribute :) (https://github.com/johanhaleby/occurrent/issues/58).");
        }

        Object object = ATTRIBUTE_NAMES.contains(fieldName) ? cloudEvent.getAttribute(fieldName) : cloudEvent.getExtension(fieldName);
        if (object instanceof URI) {
            return object.toString();
        }
        return object;
    }
}