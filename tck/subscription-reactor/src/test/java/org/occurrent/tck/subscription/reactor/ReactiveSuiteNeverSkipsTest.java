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

package org.occurrent.tck.subscription.reactor;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

import java.util.List;
import java.util.SortedMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * This leaf's half of the never-skip rule, mirroring the event-store reactive leaf's
 * {@code ReactiveSuiteNeverSkipsTest} and the blocking subscription leaf's {@code SuiteNeverSkipsTest}, which covers
 * the bridged suites where they are compiled.
 * <p>
 * Three mechanisms, because each answers something the others cannot. The failing run against
 * {@link NoopReactiveSubscriptionModel} proves the suite has tests and that they fail rather than skip when nothing is
 * honoured, but every test there dies on its first call into the model, so lines after that first call are never
 * reached. The green run against {@link WorkingReactiveSubscriptionModel} reaches every line and proves the suite is
 * satisfiable at all. {@link SkipMechanismScan} is what earns the no-skipping claim over the whole compiled suite:
 * it reads the class files this module compiles and fails on a reference to {@code Assumptions},
 * {@code TestAbortedException}, {@code @Disabled} or a {@code @DisabledIf} condition, wherever it hides.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the reactive subscription conformance suite")
class ReactiveSuiteNeverSkipsTest {

    @Test
    void fails_every_test_and_skips_none_of_them_against_a_model_that_honours_nothing() {
        Events tests = run(HonoursNothingReactiveConformance.class);

        long started = tests.started().count();
        assertThat(started)
                .describedAs("the suite must actually run something, or its verdict is meaningless")
                .isPositive();
        assertThat(tests.failed().count())
                .describedAs("every test must fail against a model that honours nothing")
                .isEqualTo(started);
        assertThat(tests.succeeded().count())
                .describedAs("nothing may pass against a model that honours nothing")
                .isZero();
        assertThat(tests.skipped().count())
                .describedAs("the suites must never skip, which is why they use no Assumptions")
                .isZero();
        assertThat(tests.aborted().count())
                .describedAs("an aborted test is a skip wearing a different hat")
                .isZero();
    }

    @Test
    void passes_every_test_and_skips_none_of_them_against_a_model_that_honours_the_contract() {
        Events tests = run(HonoursTheContractReactiveConformance.class);

        long started = tests.started().count();
        assertThat(started)
                .describedAs("the suite must actually run something, or its verdict is meaningless")
                .isPositive();
        assertThat(tests.succeeded().count())
                .describedAs("every test must pass against a model that honours the contract, which is what proves "
                        + "the suite is satisfiable and reaches every line rather than dying at the first call")
                .isEqualTo(started);
        assertThat(tests.failed().count()).isZero();
        assertThat(tests.skipped().count()).isZero();
        assertThat(tests.aborted().count()).isZero();
    }

    @Test
    void names_nothing_that_could_skip_a_test_in_the_suite_it_compiles() {
        assertThat(SkipMechanismScan.classesScannedAlongside(ReactiveSubscriptionModelConformance.class))
                .describedAs("the scan must reach the suite, or a clean verdict means only that it looked nowhere")
                .contains(ReactiveSubscriptionModelConformance.class.getName(),
                        BlockingSubscriptionOverReactive.class.getName());

        SortedMap<String, List<String>> offenders = SkipMechanismScan.of(ReactiveSubscriptionModelConformance.class);

        assertThat(offenders)
                .describedAs("a skipped test vanishes from the report, so a model that does not honour a contract "
                        + "ends up looking like one that does. Where models legitimately differ the fixture declares "
                        + "the difference and the suite asserts both answers, which is why nothing here may skip")
                .isEmpty();
    }

    @Test
    void would_notice_something_that_could_skip_a_test_if_one_appeared() {
        SortedMap<String, List<String>> offenders = SkipMechanismScan.of(SkipsOnPurpose.class);

        assertThat(offenders)
                .describedAs("a scan that cannot find the one class written to be found would pass a suite full of "
                        + "assumptions just as quietly as it passes a clean one")
                .containsKey(SkipsOnPurpose.class.getName());
        assertThat(offenders.get(SkipsOnPurpose.class.getName()))
                .contains("org/junit/jupiter/api/Assumptions");
    }

    private static Events run(Class<?> suite) {
        return EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(suite))
                .execute()
                .testEvents();
    }

    /**
     * Not named {@code *Test}, so Surefire does not pick it up as a test of its own. It exists only for the failing
     * run above to select, and every test in it is expected to fail.
     */
    static class HonoursNothingReactiveConformance extends ReactiveSubscriptionModelConformance {

        @Override
        protected ReactiveSubscriptionModelFixture createFixture() {
            return new ReactiveSubscriptionModelFixture() {
                @Override
                public org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel() {
                    return NoopReactiveSubscriptionModel.INSTANCE;
                }

                @Override
                public void publish(List<CloudEvent> events) {
                    throw new UnsupportedOperationException("honours nothing, on purpose");
                }
            };
        }
    }

    /**
     * Not named {@code *Test} either: it runs inside the green run above, where a failure is this leaf's own test
     * failure rather than a skipped-by-accident nothing.
     */
    static class HonoursTheContractReactiveConformance extends ReactiveSubscriptionModelConformance {

        @Override
        protected ReactiveSubscriptionModelFixture createFixture() {
            WorkingReactiveSubscriptionModel model = new WorkingReactiveSubscriptionModel();
            return new ReactiveSubscriptionModelFixture() {
                @Override
                public org.occurrent.subscription.api.reactor.SubscriptionModel subscriptionModel() {
                    return model;
                }

                @Override
                public void publish(List<CloudEvent> events) {
                    model.deliver(events);
                }

                @Override
                public void close() {
                    model.shutdown();
                }
            };
        }
    }
}
