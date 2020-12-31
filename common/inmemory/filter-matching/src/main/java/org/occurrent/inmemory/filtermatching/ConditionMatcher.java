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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.occurrent.condition.Condition.SingleOperandConditionName.EQ;
import static org.occurrent.condition.Condition.SingleOperandConditionName.NE;
import static org.occurrent.filter.Filter.*;

/**
 * Check if a condition matches a certain cloud event
 */
public class ConditionMatcher {

    private static final Set<String> ATTRIBUTE_NAMES = new HashSet<String>() {{
        add(SPEC_VERSION);
        add(ID);
        add(TYPE);
        add(TIME);
        add(SOURCE);
        add(SUBJECT);
        add(DATA_SCHEMA);
        add(DATA_CONTENT_TYPE);
    }};

    public static <T> boolean matchesCondition(CloudEvent cloudEvent, String fieldName, Condition<T> condition) {
        if (condition instanceof MultiOperandCondition) {
            MultiOperandCondition<T> operation = (MultiOperandCondition<T>) condition;
            Condition.MultiOperandConditionName operationName = operation.operationName;
            List<Condition<T>> operations = operation.operations;
            Stream<Boolean> filters = operations.stream().map(c -> matchesCondition(cloudEvent, fieldName, c));
            switch (operationName) {
                case AND:
                    return filters.allMatch(Predicate.isEqual(true));
                case OR:
                    return filters.anyMatch(Predicate.isEqual(true));
                case NOT:
                    return filters.allMatch(Predicate.isEqual(false));
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else if (condition instanceof SingleOperandCondition) {
            SingleOperandCondition<T> singleOperandCondition = (SingleOperandCondition<T>) condition;
            T expected = singleOperandCondition.operand;
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
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
                switch (singleOperandConditionName) {
                    case LT:
                        matches = comparisonResult < 0;
                        break;
                    case GT:
                        matches = comparisonResult > 0;
                        break;
                    case LTE:
                        matches = comparisonResult <= 0;
                        break;
                    case GTE:
                        matches = comparisonResult >= 0;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
                }
            }
            return matches;
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