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
import org.jspecify.annotations.NullMarked;
import org.occurrent.condition.Condition;
import org.occurrent.condition.Condition.MultiOperandCondition;
import org.occurrent.condition.Condition.SingleOperandCondition;
import org.occurrent.condition.Condition.SingleOperandConditionName;
import org.occurrent.filtermatching.DataFieldReader;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Predicate.isEqual;
import static org.occurrent.condition.Condition.SingleOperandConditionName.EQ;
import static org.occurrent.condition.Condition.SingleOperandConditionName.NE;
import static org.occurrent.filter.Filter.*;

/**
 * Check if a condition matches a certain cloud event
 */
@NullMarked
public class ConditionMatcher {

    private static final Set<String> ATTRIBUTE_NAMES = Set.of(SPEC_VERSION, ID, TYPE, TIME, SOURCE, SUBJECT, DATA_SCHEMA, DATA_CONTENT_TYPE);

    // Marks a data path that led nowhere, so every condition answers "no match" for it without going through any
    // comparison logic. Kept distinct from null, which still means "this attribute or extension is absent" and
    // keeps behaving exactly as before (see matchesRange). Package-private rather than private, since FilterMatcher
    // passes it to matchesCondition(Object, Condition) for a batched data leaf that readAll resolved to nothing.
    static final Object ABSENT = new Object();

    public static <T> boolean matchesCondition(CloudEvent cloudEvent, String fieldName, Condition<T> condition) {
        return matchesCondition(cloudEvent, fieldName, condition, DataFieldReader.refusing());
    }

    public static <T> boolean matchesCondition(CloudEvent cloudEvent, String fieldName, Condition<T> condition, DataFieldReader dataFieldReader) {
        return matchesCondition(extractValue(cloudEvent, fieldName, dataFieldReader), condition);
    }

    /**
     * Evaluates {@code condition} against a value already known, rather than reading it through a
     * {@link DataFieldReader}. FilterMatcher uses this for a data-field leaf of a composed {@code AND} whose value
     * came out of one batched {@link DataFieldReader#readAll} call instead of its own {@link DataFieldReader#read}
     * call, so a filter with several data-field leaves does not pay for a {@code DataFieldReader} round trip, an
     * {@code Optional}, and the field-name/attribute-or-data branch in {@link #extractValue} on top of a lookup the
     * batch already did. Pass {@link #ABSENT} for a path the batch resolved to nothing, the same as
     * {@link #matchesCondition(CloudEvent, String, Condition, DataFieldReader)} would treat an empty read.
     */
    static <T> boolean matchesCondition(Object extractedValue, Condition<T> condition) {
        return switch (condition) {
            case MultiOperandCondition<T> operation -> matchesMultiOperandCondition(extractedValue, operation);
            case SingleOperandCondition<T> singleOperandCondition -> matchesSingleOperandCondition(extractedValue, singleOperandCondition);
            case Condition.InOperandCondition<T> inOperandCondition -> matchesInOperandCondition(extractedValue, inOperandCondition);
        };
    }

    private static <T> boolean matchesMultiOperandCondition(Object extractedValue, MultiOperandCondition<T> operation) {
            Condition.MultiOperandConditionName operationName = operation.operationName();
            List<Condition<T>> operations = operation.operations();
            Stream<Boolean> filters = operations.stream().map(c -> matchesCondition(extractedValue, c));
            return switch (operationName) {
                case AND -> filters.allMatch(isEqual(true));
                case OR -> filters.anyMatch(isEqual(true));
                case NOT -> filters.allMatch(isEqual(false));
            };
    }

    private static <T> boolean matchesSingleOperandCondition(Object actual, SingleOperandCondition<T> singleOperandCondition) {
            T expected = singleOperandCondition.operand();
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.operandConditionName();
            if (actual == ABSENT) {
                return false;
            }
            if (singleOperandConditionName == EQ) {
                return anyElementMatches(actual, element -> valuesEqual(element, expected));
            } else if (singleOperandConditionName == NE) {
                // "No element equals" rather than "any element differs", the same reading MongoDB gives an array.
                // Not pinned by the conformance suite, which only exercises EQ and the range operators on an array.
                return !anyElementMatches(actual, element -> valuesEqual(element, expected));
            } else {
                return anyElementMatches(actual, element -> matchesRange(element, expected, singleOperandConditionName));
            }
    }

    private static <T> boolean matchesInOperandCondition(Object actual, Condition.InOperandCondition<T> inOperandCondition) {
            if (actual == ABSENT) {
                return false;
            }
            Collection<T> operand = inOperandCondition.operand();
            return anyElementMatches(actual, element -> operand.stream().anyMatch(it -> valuesEqual(it, element)));
    }

    // EQ, NE and IN share this rather than Objects.equals, for the same reason the range operators use BigDecimal:
    // a stored Long and a filter operand built from an int literal are unequal by Objects.equals despite being the
    // same number, which is not the divergence-from-MongoDB's-$eq that a caller asking "does this field equal 42"
    // wants to hit. Two operands that are both numbers compare by value; anything else falls back to Objects.equals.
    private static boolean valuesEqual(Object actual, Object expected) {
        if (actual instanceof Number actualNumber && expected instanceof Number expectedNumber
                && isFinite(actualNumber) && isFinite(expectedNumber)) {
            return toBigDecimal(actualNumber).compareTo(toBigDecimal(expectedNumber)) == 0;
        }
        return Objects.equals(actual, expected);
    }

    // BigDecimal.valueOf(double) throws for NaN and an infinity, which Objects.equals handles fine instead (NaN
    // equals NaN, an infinity equals itself), so a non-finite Double or Float takes that path rather than crashing.
    private static boolean isFinite(Number number) {
        return switch (number) {
            case Double d -> !d.isNaN() && !d.isInfinite();
            case Float f -> !f.isNaN() && !f.isInfinite();
            default -> true;
        };
    }

    // A collection value matches a condition if any element does, the same rule MongoDB applies to an array field.
    private static boolean anyElementMatches(Object actual, Predicate<Object> matchesElement) {
        if (actual instanceof Collection<?> collection) {
            return collection.stream().anyMatch(matchesElement);
        }
        return matchesElement.test(actual);
    }

    private static <T> boolean matchesRange(Object actual, T expected, SingleOperandConditionName singleOperandConditionName) {
        Comparable<Object> expectedComparable = toComparable(expected, "Expected value must implement " + Comparable.class.getName() + " in order to be used in Filter's");
        int comparisonResult;
        if (actual instanceof Number actualNumber && expected instanceof Number expectedNumber) {
            // Compare by value rather than by Java type, so an Integer field matches gt(10) the same way a Double one does.
            comparisonResult = toBigDecimal(actualNumber).compareTo(toBigDecimal(expectedNumber));
        } else {
            Comparable<Object> actualComparable = toComparable(actual, "Value in CloudEvent must implement " + Comparable.class.getName() + " in order to be used in Filter's");
            try {
                comparisonResult = actualComparable.compareTo(expectedComparable);
            } catch (ClassCastException e) {
                // Both sides are Comparable, just not with each other, e.g. a String compared to an Integer.
                // A type mismatch means no match, not a crash.
                return false;
            }
        }
        return switch (singleOperandConditionName) {
            case LT -> comparisonResult < 0;
            case GT -> comparisonResult > 0;
            case LTE -> comparisonResult <= 0;
            case GTE -> comparisonResult >= 0;
            default -> throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
        };
    }

    private static BigDecimal toBigDecimal(Number number) {
        if (number instanceof BigDecimal bigDecimal) {
            return bigDecimal;
        } else if (number instanceof BigInteger bigInteger) {
            return new BigDecimal(bigInteger);
        } else if (number instanceof Double || number instanceof Float) {
            return BigDecimal.valueOf(number.doubleValue());
        } else {
            return BigDecimal.valueOf(number.longValue());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Comparable<Object> toComparable(T operand, String failureMessage) {
        if (!(operand instanceof Comparable)) {
            throw new IllegalArgumentException(failureMessage);
        }
        return (Comparable<Object>) operand;
    }

    private static Object extractValue(CloudEvent cloudEvent, String fieldName, DataFieldReader dataFieldReader) {
        if (fieldName.startsWith(DATA + ".")) {
            String path = fieldName.substring((DATA + ".").length());
            return dataFieldReader.read(cloudEvent, path).orElse(ABSENT);
        }

        Object object = ATTRIBUTE_NAMES.contains(fieldName) ? cloudEvent.getAttribute(fieldName) : cloudEvent.getExtension(fieldName);
        if (object instanceof URI) {
            return object.toString();
        }
        return object;
    }
}
