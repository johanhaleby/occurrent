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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.condition.Condition;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LongConditionEvaluatorTest {

    @ParameterizedTest
    @MethodSource("arguments")
    void evaluatesConditionCorrectly(long value, Condition<Long> condition, boolean expectedResult) {
        boolean actual = LongConditionEvaluator.evaluate(condition, value);

        assertThat(actual).isEqualTo(expectedResult);
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(2L, Condition.eq(2L), true),
                Arguments.of(3L, Condition.eq(2L), false),
                Arguments.of(2L, Condition.eq(3L), false),
                Arguments.of(2L, Condition.lt(3L), true),
                Arguments.of(3L, Condition.lt(2L), false),
                Arguments.of(3L, Condition.lt(3L), false),
                Arguments.of(2L, Condition.gt(3L), false),
                Arguments.of(3L, Condition.gt(2L), true),
                Arguments.of(3L, Condition.gt(3L), false),
                Arguments.of(2L, Condition.lte(3L), true),
                Arguments.of(3L, Condition.lte(2L), false),
                Arguments.of(3L, Condition.lte(3L), true),
                Arguments.of(2L, Condition.gte(3L), false),
                Arguments.of(3L, Condition.gte(2L), true),
                Arguments.of(3L, Condition.gte(3L), true),
                Arguments.of(2L, Condition.ne(3L), true),
                Arguments.of(3L, Condition.ne(2L), true),
                Arguments.of(3L, Condition.ne(3L), false),
                Arguments.of(3L, Condition.or(Condition.gt(3L), Condition.lt(5L)), true),
                Arguments.of(3L, Condition.or(Condition.gt(3L), Condition.ne(2L)), true),
                Arguments.of(3L, Condition.or(Condition.ne(3L), Condition.lt(2L)), false),
                Arguments.of(3L, Condition.and(Condition.eq(3L), Condition.lt(20L)), true),
                Arguments.of(3L, Condition.and(Condition.eq(3L), Condition.lt(2L)), false),
                Arguments.of(3L, Condition.not(Condition.and(Condition.eq(3L), Condition.lt(2L))), true),
                Arguments.of(3L, Condition.not(Condition.or(Condition.gt(3L), Condition.ne(2L))), false)
        );
    }
}