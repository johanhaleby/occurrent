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

package org.occurrent.tck.internal.scaffolding;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.testkit.engine.EngineExecutionResults;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.opentest4j.TestAbortedException;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * The suite at the bottom has the shape of the late-commit suites, an abstract class whose tests are inherited by the
 * class that carries the annotation.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("an expected guarantee violation")
class ExpectedGuaranteeViolationTest {

    private static final String ISSUE = "https://github.com/johanhaleby/occurrent/issues/1230";

    @Test
    void passes_a_listed_test_that_throws_the_violation() {
        assertThat(resultOf(Listing.class, "violates_the_guarantee").getStatus())
                .isEqualTo(TestExecutionResult.Status.SUCCESSFUL);
    }

    @Test
    void fails_a_listed_test_that_completes_and_says_the_guarantee_now_holds() {
        TestExecutionResult result = resultOf(Listing.class, "holds_the_guarantee");

        assertThat(result.getStatus()).isEqualTo(TestExecutionResult.Status.FAILED);
        assertThat(throwableOf(result))
                .hasMessageContaining("the guarantee now holds")
                .hasMessageContaining("expected violation for " + ISSUE + " must be removed");
    }

    @Test
    void lets_any_other_throwable_from_a_listed_test_propagate_unchanged() {
        TestExecutionResult result = resultOf(Listing.class, "fails_for_another_reason");

        assertThat(result.getStatus()).isEqualTo(TestExecutionResult.Status.FAILED);
        assertThat(throwableOf(result))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("the hold did not enter the window");
    }

    @Test
    void leaves_a_test_that_is_not_listed_alone_when_it_throws_the_violation() {
        TestExecutionResult result = resultOf(Listing.class, "unlisted_violation");

        assertThat(result.getStatus()).isEqualTo(TestExecutionResult.Status.FAILED);
        assertThat(throwableOf(result)).isExactlyInstanceOf(GuaranteeViolated.class);
    }

    @Test
    void leaves_a_test_that_is_not_listed_alone_when_it_passes() {
        assertThat(resultOf(Listing.class, "unlisted_pass").getStatus())
                .isEqualTo(TestExecutionResult.Status.SUCCESSFUL);
    }

    @Test
    void does_not_swallow_the_violation_when_it_comes_from_setup() {
        TestExecutionResult result = resultOf(ListingWithViolatingSetup.class, "violates_the_guarantee");

        assertThat(result.getStatus())
                .describedAs("only the test method itself is expected to violate, a setup that does is a broken fixture")
                .isEqualTo(TestExecutionResult.Status.FAILED);
        assertThat(throwableOf(result)).isExactlyInstanceOf(GuaranteeViolated.class);
    }

    @Test
    void does_not_turn_an_abort_from_a_listed_test_into_a_pass() {
        TestExecutionResult result = resultOf(ListingWithSkips.class, "aborts_by_exception");

        assertThat(result.getStatus()).isEqualTo(TestExecutionResult.Status.ABORTED);
        assertThat(throwableOf(result)).isExactlyInstanceOf(TestAbortedException.class);
    }

    @Test
    void does_not_turn_a_failed_assumption_in_a_listed_test_into_a_pass() {
        TestExecutionResult result = resultOf(ListingWithSkips.class, "aborts_by_assumption");

        assertThat(result.getStatus()).isEqualTo(TestExecutionResult.Status.ABORTED);
        assertThat(throwableOf(result)).isInstanceOf(TestAbortedException.class);
    }

    @Test
    void does_not_turn_a_disabled_listed_test_into_a_pass() {
        var tests = EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(ListingWithSkips.class))
                .execute()
                .testEvents();

        assertThat(tests.skipped().stream()
                .map(event -> ((MethodSource) event.getTestDescriptor().getSource().orElseThrow()).getMethodName()))
                .containsExactly("disabled");
        assertThat(tests.succeeded().count()).isZero();
    }

    @Test
    void fails_the_class_when_a_listed_name_is_not_a_test_method_of_it() {
        EngineExecutionResults results = EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(ListingAMisspelledName.class))
                .execute();

        Throwable containerFailure = results.containerEvents().failed().stream()
                .map(event -> event.getRequiredPayload(TestExecutionResult.class).getThrowable())
                .flatMap(Optional::stream)
                .findFirst()
                .orElseThrow(() -> new AssertionError("the class did not fail"));
        assertThat(containerFailure)
                .hasMessageContaining("violates_the_guarentee")
                .hasMessageContaining("not a test method");
        assertThat(results.testEvents().started().count())
                .describedAs("a class with a listing that cannot be applied must not run its tests as if it could")
                .isZero();
    }

    @Test
    void reports_what_was_expected_and_what_was_seen() {
        GuaranteeViolated violated = new GuaranteeViolated("A is delivered", "A is missing");

        assertThat(violated).isInstanceOf(AssertionError.class).hasMessageContaining("A is delivered").hasMessageContaining("A is missing");
        assertThat(violated.expected()).isEqualTo("A is delivered");
        assertThat(violated.seen()).isEqualTo("A is missing");
    }

    private static Throwable throwableOf(TestExecutionResult result) {
        return result.getThrowable().orElseThrow(() -> new AssertionError("the test did not fail with a throwable"));
    }

    private static TestExecutionResult resultOf(Class<?> testClass, String testMethod) {
        return EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(testClass))
                .execute()
                .testEvents()
                .finished()
                .stream()
                .filter(event -> event.getTestDescriptor().getSource()
                        .filter(MethodSource.class::isInstance)
                        .map(MethodSource.class::cast)
                        .filter(source -> source.getMethodName().equals(testMethod))
                        .isPresent())
                .findFirst()
                .map(event -> event.getRequiredPayload(TestExecutionResult.class))
                .orElseThrow(() -> new AssertionError(testMethod + " did not finish in " + testClass.getName()));
    }

    // None of these is named *Test, so Surefire does not pick them up as tests of their own. They exist only for the
    // runs above to select.

    static abstract class Suite {

        @Test
        void violates_the_guarantee() {
            throw new GuaranteeViolated("A is delivered", "A is missing");
        }

        @Test
        void holds_the_guarantee() {
        }

        @Test
        void fails_for_another_reason() {
            throw new IllegalStateException("the hold did not enter the window");
        }

        @Test
        void unlisted_violation() {
            throw new GuaranteeViolated("A is delivered", "A is missing");
        }

        @Test
        void unlisted_pass() {
        }
    }

    @ExpectedGuaranteeViolation(tests = {"violates_the_guarantee", "holds_the_guarantee", "fails_for_another_reason"}, issue = ISSUE)
    static class Listing extends Suite {
    }

    @ExpectedGuaranteeViolation(tests = {"violates_the_guarantee"}, issue = ISSUE)
    static class ListingWithViolatingSetup extends Suite {

        @BeforeEach
        void setup_that_violates() {
            throw new GuaranteeViolated("a fixture", "a broken fixture");
        }
    }

    @ExpectedGuaranteeViolation(tests = {"aborts_by_exception", "aborts_by_assumption", "disabled"}, issue = ISSUE)
    static class ListingWithSkips {

        @Test
        void aborts_by_exception() {
            throw new TestAbortedException("aborted");
        }

        @Test
        void aborts_by_assumption() {
            Assumptions.assumeTrue(false);
        }

        @Disabled
        @Test
        void disabled() {
            throw new GuaranteeViolated("A is delivered", "A is missing");
        }
    }

    @ExpectedGuaranteeViolation(tests = {"violates_the_guarentee"}, issue = ISSUE)
    static class ListingAMisspelledName extends Suite {
    }
}
