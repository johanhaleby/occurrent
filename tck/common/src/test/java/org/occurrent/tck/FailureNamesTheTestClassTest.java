/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.tck;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.opentest4j.AssertionFailedError;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * The suite at the bottom has the shape every conformance suite in this TCK has, an abstract class whose tests live in
 * a {@code @Nested} class, extended once per implementation. Two implementations extend it because one is not enough
 * to show what the extension is for.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a failure out of a conformance suite")
class FailureNamesTheTestClassTest {

    @Test
    void names_the_test_class_that_ran_it_rather_than_the_suite_the_test_is_declared_in() {
        AssertionFailedError failure = failureFrom(FirstImplementation.class, "asserts_something_untrue");

        assertThat(failure)
                .hasMessageStartingWith("Run by FirstImplementation.")
                .describedAs("the description the suite wrote is what says which contract was broken, so it has to "
                        + "survive the rename")
                .hasMessageContaining("what this test is about");
    }

    @Test
    void tells_two_implementations_of_one_suite_apart() {
        // Both run the same inherited test and report it under the same name, so without this the two failures read
        // as one test that failed and then passed.
        String first = failureFrom(FirstImplementation.class, "asserts_something_untrue").getMessage();
        String second = failureFrom(SecondImplementation.class, "asserts_something_untrue").getMessage();

        assertThat(first).contains("FirstImplementation").doesNotContain("SecondImplementation");
        assertThat(second).contains("SecondImplementation").doesNotContain("FirstImplementation");
    }

    @Test
    void keeps_the_expected_and_actual_values_of_the_original() {
        AssertionFailedError failure = failureFrom(FirstImplementation.class, "compares_two_values");

        assertThat(failure.isExpectedDefined())
                .describedAs("an assertion whose values are dropped stops showing a diff in an IDE, which is a worse "
                        + "report than the one this set out to improve")
                .isTrue();
        assertThat(failure.getExpected().getValue()).isEqualTo("what was expected");
        assertThat(failure.getActual().getValue()).isEqualTo("what arrived");
    }

    @Test
    void claims_no_expected_and_actual_values_when_the_original_compared_none() {
        AssertionFailedError failure = failureFrom(FirstImplementation.class, "fails_without_comparing_two_values");

        assertThat(failure.isExpectedDefined())
                .describedAs("inventing a comparison would make an IDE show \"expected null but was null\" for an "
                        + "assertion that never compared anything")
                .isFalse();
        assertThat(failure.isActualDefined()).isFalse();
    }

    @Test
    void still_points_at_the_line_the_assertion_failed_on() {
        AssertionFailedError failure = failureFrom(FirstImplementation.class, "asserts_something_untrue");

        assertThat(Arrays.stream(failure.getStackTrace()).map(StackTraceElement::getClassName))
                .describedAs("a rewritten failure that points into this extension sends the reader to the wrong file")
                .contains(Suite.Assertions.class.getName())
                .doesNotContain(FailureNamesTheTestClass.class.getName());
    }

    @Test
    void leaves_anything_that_is_not_an_assertion_failure_as_it_was() {
        Throwable thrown = throwableFrom(FirstImplementation.class, "throws_the_way_a_broken_implementation_does");

        assertThat(thrown)
                .describedAs("an implementation that throws already names itself in the stack trace, so rewriting it "
                        + "would only cost the reader the type")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("thrown by the implementation");
    }

    private static AssertionFailedError failureFrom(Class<?> testClass, String testMethod) {
        Throwable thrown = throwableFrom(testClass, testMethod);
        assertThat(thrown).isInstanceOf(AssertionFailedError.class);
        return (AssertionFailedError) thrown;
    }

    private static Throwable throwableFrom(Class<?> testClass, String testMethod) {
        Events tests = EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(testClass))
                .execute()
                .testEvents();

        Optional<Throwable> failure = tests.failed().stream()
                .filter(event -> event.getTestDescriptor().getSource()
                        .filter(MethodSource.class::isInstance)
                        .map(MethodSource.class::cast)
                        .filter(source -> source.getMethodName().equals(testMethod))
                        .isPresent())
                .findFirst()
                .flatMap(event -> event.getPayload(TestExecutionResult.class))
                .flatMap(TestExecutionResult::getThrowable);
        return failure.orElseThrow(() -> new AssertionError(testMethod + " did not fail in " + testClass.getName()
                + ", so there is no failure to read"));
    }

    // Neither implementation is named *Test, so Surefire does not pick them up as tests of their own. They exist only
    // for the runs above to select.

    @ExtendWith(FailureNamesTheTestClass.class)
    static abstract class Suite {

        @Nested
        class Assertions {

            @Test
            void asserts_something_untrue() {
                assertThat(false).as("what this test is about").isTrue();
            }

            @Test
            void compares_two_values() {
                throw new AssertionFailedError("compared two values", "what was expected", "what arrived");
            }

            @Test
            void fails_without_comparing_two_values() {
                throw new AssertionFailedError("nothing was compared");
            }

            @Test
            void throws_the_way_a_broken_implementation_does() {
                throw new IllegalStateException("thrown by the implementation");
            }
        }
    }

    static class FirstImplementation extends Suite {
    }

    static class SecondImplementation extends Suite {
    }
}
