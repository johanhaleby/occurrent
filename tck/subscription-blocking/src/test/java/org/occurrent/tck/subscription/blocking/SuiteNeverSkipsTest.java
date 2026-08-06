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

package org.occurrent.tck.subscription.blocking;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * A TCK that can be satisfied by doing nothing is worse than no TCK, so every suite in this module is run here against
 * an implementation that honours none of the contract and against one that honours all of it. The first run must fail
 * every test, the second must pass every test, and neither may skip or abort anything.
 * <p>
 * The two runs answer different questions and the second one is the reason there are two. An implementation that throws
 * from every method dies on the first call in each test, so that run says nothing about code further down a test method:
 * an {@code Assumptions} call placed after the first call would never be reached and the skipped count would stay zero.
 * Running the whole suite green does reach every line, so a skip anywhere in the suite body shows up here.
 * <p>
 * {@link InProcessDeliveryConformance} is the one suite whose failing run uses a <em>working</em> model rather than a
 * broken one. Its whole subject is that delivery has already happened when publishing returns, so what has to fail it is
 * a model that delivers asynchronously, which is exactly the regression it exists to catch.
 * <p>
 * <strong>Scanning the compiled suites for anything that can skip.</strong> This is what earns the no-skipping claim.
 * {@link SkipMechanismScan} reads the class files this module compiles and fails if any of them so much as references
 * {@code Assumptions}, {@code TestAbortedException}, {@code @Disabled} or a {@code @DisabledIf} condition. It covers
 * every line of every suite rather than the lines one fixture's declarations reach, and it covers every suite in the
 * module rather than the ones listed above, including any added later.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a conformance suite")
class SuiteNeverSkipsTest {

    @Test
    void fails_every_test_and_skips_none_of_them_against_a_storage_that_honours_nothing() {
        assertEveryTestFails(HonoursNothingCheckpointStorageConformance.class, "a storage that honours nothing");
    }

    @Test
    void passes_every_test_and_skips_none_of_them_against_a_storage_that_honours_everything() {
        assertEveryTestPasses(HonoursEverythingCheckpointStorageConformance.class, "storage");
    }

    @Test
    void fails_every_test_and_skips_none_of_them_against_a_subscription_model_that_honours_nothing() {
        assertEveryTestFails(HonoursNothingSubscriptionModelConformance.class, "a model that honours nothing");
    }

    @Test
    void passes_every_test_and_skips_none_of_them_against_a_subscription_model_that_honours_everything() {
        assertEveryTestPasses(HonoursEverythingSubscriptionModelConformance.class, "subscription model");
    }

    @Test
    void the_introspection_suite_fails_every_test_against_a_model_that_honours_nothing() {
        assertEveryTestFails(HonoursNothingIntrospectionConformance.class, "a model that honours nothing");
    }

    @Test
    void the_introspection_suite_passes_every_test_against_a_model_that_honours_everything() {
        assertEveryTestPasses(HonoursEverythingIntrospectionConformance.class, "subscription model");
    }

    @Test
    void the_checkpoint_suite_fails_every_test_against_a_model_that_honours_nothing() {
        assertEveryTestFails(HonoursNothingCheckpointAwareConformance.class, "a model that honours nothing");
    }

    @Test
    void the_checkpoint_suite_passes_every_test_against_a_model_that_honours_everything() {
        assertEveryTestPasses(HonoursEverythingCheckpointAwareConformance.class, "subscription model");
    }

    @Test
    void the_competing_consumer_suite_fails_every_test_against_a_strategy_that_honours_nothing() {
        assertEveryTestFails(HonoursNothingCompetingConsumerConformance.class, "a strategy that honours nothing");
    }

    @Test
    void the_competing_consumer_suite_passes_every_test_against_a_strategy_that_honours_everything() {
        assertEveryTestPasses(HonoursEverythingCompetingConsumerConformance.class, "competing consumer strategy");
    }

    @Test
    void the_in_process_suite_fails_every_test_against_a_model_that_delivers_asynchronously() {
        // The one case where "honours nothing" is the wrong shape. This suite's whole subject is that delivery already
        // happened when publishing returned, so the model that must fail it is a working asynchronous one rather than a
        // broken one, and that is exactly the regression the suite exists to catch.
        assertEveryTestFails(AsynchronousInProcessConformance.class, "a model that delivers asynchronously");
    }

    @Test
    void the_restart_suite_fails_every_test_against_a_model_that_honours_nothing() {
        assertEveryTestFails(HonoursNothingRestartConformance.class, "a model that honours nothing");
    }

    @Test
    void the_restart_suite_passes_every_test_against_a_model_that_resumes() {
        assertEveryTestPasses(ResumingRestartConformance.class, "subscription model");
    }

    @Test
    void the_restart_suite_passes_every_test_against_a_model_that_starts_at_the_present() {
        // The other branch of the same declaration. Both are run here rather than left to Occurrent's own models,
        // because a suite whose second branch is only ever exercised elsewhere is a suite nothing in this module can
        // prove is satisfiable.
        assertEveryTestPasses(StartingAtThePresentRestartConformance.class, "subscription model");
    }

    @Test
    void names_nothing_that_could_skip_a_test_in_any_suite_it_compiles() {
        assertThat(SkipMechanismScan.classesScannedAlongside(SubscriptionModelConformance.class))
                .describedAs("the scan must reach the suites, or a clean verdict means only that it looked nowhere")
                .contains(CheckpointStorageConformance.class.getName(), SubscriptionModelConformance.class.getName(),
                        IntrospectableSubscriptionModelConformance.class.getName(),
                        CheckpointAwareSubscriptionModelConformance.class.getName(),
                        CompetingConsumerStrategyConformance.class.getName(),
                        InProcessDeliveryConformance.class.getName(),
                        RestartConformance.class.getName());

        SortedMap<String, List<String>> offenders = SkipMechanismScan.of(SubscriptionModelConformance.class);

        assertThat(offenders)
                .describedAs("a skipped test vanishes from the report, so an implementation that does not honour a "
                        + "contract ends up looking like one that does. Where implementations legitimately differ the "
                        + "fixture declares the difference and the suite asserts both answers, which is why nothing "
                        + "here may skip")
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

    private static void assertEveryTestFails(Class<?> suite, String what) {
        assertOutcome(suite, what, false);
    }

    private static void assertEveryTestPasses(Class<?> suite, String what) {
        assertOutcome(suite, what, true);
    }

    private static void assertOutcome(Class<?> suite, String what, boolean shouldPass) {
        Events tests = run(suite);

        long started = tests.started().count();
        assertThat(started)
                .describedAs("the suite must actually run something, or its verdict is meaningless")
                .isPositive();
        long passed = tests.succeeded().count();
        long failed = tests.failed().count();
        if (shouldPass) {
            assertThat(passed)
                    .describedAs("a %s that honours the whole contract must pass the whole suite, so a test that cannot "
                            + "be satisfied by any implementation is caught here rather than by whoever adds the next "
                            + "one. What failed: %s", what, failureDetail(tests))
                    .isEqualTo(started);
            assertThat(failed).isZero();
        } else {
            assertThat(failed)
                    .describedAs("every test must fail against %s", what)
                    .isEqualTo(started);
            assertThat(passed)
                    .describedAs("nothing may pass against %s", what)
                    .isZero();
        }
        assertThat(tests.skipped().count())
                .describedAs("the suite must never skip, which is why it uses no Assumptions")
                .isZero();
        assertThat(tests.aborted().count())
                .describedAs("an aborted test is a skip wearing a different hat")
                .isZero();
    }

    /**
     * Names the tests that failed and why. Without this a regression here reports only a count, and the suite it ran is
     * not the one Surefire reports on, so there is nothing else to read.
     */
    private static String failureDetail(Events tests) {
        return tests.failed().stream()
                .map(event -> event.getTestDescriptor().getDisplayName() + ": " + event.getPayload(TestExecutionResult.class)
                        .flatMap(TestExecutionResult::getThrowable)
                        .map(Throwable::getMessage)
                        .orElse("no throwable"))
                .collect(Collectors.joining(" | "));
    }

    private static Events run(Class<?> suite) {
        return EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(suite))
                .execute()
                .testEvents();
    }

    // Neither of these is named *Test, so Surefire does not pick them up as tests of their own. They exist only for the
    // runs above to select.

    static class HonoursNothingCheckpointStorageConformance extends CheckpointStorageConformance {

        @Override
        protected CheckpointStorageFixture createFixture() {
            return new CheckpointStorageFixture() {
                @Override
                public CheckpointStorage checkpointStorage() {
                    return NoopCheckpointStorage.INSTANCE;
                }

                @Override
                public boolean preservesCheckpointType(Checkpoint checkpoint) {
                    // Never reached: every call into the storage throws before the suite consults this.
                    return true;
                }
            };
        }
    }

    static class HonoursEverythingCheckpointStorageConformance extends CheckpointStorageConformance {

        @Override
        protected CheckpointStorageFixture createFixture() {
            return new CheckpointStorageFixture() {

                private final CheckpointStorage storage = new WorkingCheckpointStorage();

                @Override
                public CheckpointStorage checkpointStorage() {
                    return storage;
                }

                /**
                 * A map hands back the checkpoint it was given, so every type survives.
                 */
                @Override
                public boolean preservesCheckpointType(Checkpoint checkpoint) {
                    return true;
                }
            };
        }
    }

    static class HonoursNothingSubscriptionModelConformance extends SubscriptionModelConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            return new NoopSubscriptionModelFixture();
        }
    }

    static class HonoursEverythingSubscriptionModelConformance extends SubscriptionModelConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            return new WorkingSubscriptionModelFixture();
        }
    }

    static class HonoursNothingIntrospectionConformance extends IntrospectableSubscriptionModelConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            return new NoopSubscriptionModelFixture();
        }
    }

    static class HonoursEverythingIntrospectionConformance extends IntrospectableSubscriptionModelConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            return new WorkingSubscriptionModelFixture();
        }
    }

    static class HonoursNothingCheckpointAwareConformance extends CheckpointAwareSubscriptionModelConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            return new NoopSubscriptionModelFixture();
        }
    }

    static class HonoursEverythingCheckpointAwareConformance extends CheckpointAwareSubscriptionModelConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            return new WorkingSubscriptionModelFixture();
        }
    }

    static class HonoursNothingCompetingConsumerConformance extends CompetingConsumerStrategyConformance {

        @Override
        protected CompetingConsumerStrategyFixture createFixture() {
            return new CompetingConsumerStrategyFixture() {
                @Override
                public CompetingConsumerStrategy competingConsumerStrategy() {
                    return NoopCompetingConsumerStrategy.INSTANCE;
                }

                @Override
                public CompetingConsumerStrategy newCompetingConsumerStrategy() {
                    return NoopCompetingConsumerStrategy.INSTANCE;
                }

                @Override
                public Duration timeToConverge() {
                    // Never waited out: every call into the strategy throws before the suite gets as far as waiting.
                    return Duration.ofSeconds(1);
                }
            };
        }
    }

    static class HonoursEverythingCompetingConsumerConformance extends CompetingConsumerStrategyConformance {

        @Override
        protected CompetingConsumerStrategyFixture createFixture() {
            return new CompetingConsumerStrategyFixture() {

                private final WorkingCompetingConsumerStrategy.Storage storage = new WorkingCompetingConsumerStrategy.Storage();
                private final CompetingConsumerStrategy strategy = new WorkingCompetingConsumerStrategy(storage);

                @Override
                public CompetingConsumerStrategy competingConsumerStrategy() {
                    return strategy;
                }

                @Override
                public CompetingConsumerStrategy newCompetingConsumerStrategy() {
                    return new WorkingCompetingConsumerStrategy(storage);
                }

                /**
                 * Two orders of magnitude above this strategy's own round, since the bound is only paid in full by a
                 * failure and a tight one would make the anti-skip run itself the flaky thing.
                 */
                @Override
                public Duration timeToConverge() {
                    return Duration.ofSeconds(5);
                }

                @Override
                public void close() {
                    strategy.shutdown();
                }
            };
        }
    }

    static class AsynchronousInProcessConformance extends InProcessDeliveryConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            // A delay, so the pool thread cannot deliver before the assertion reads the list. Without it this run
            // passes every so often by luck, which would make the anti-skip test itself the flaky thing.
            return new WorkingSubscriptionModelFixture(Duration.ofMillis(200));
        }
    }

    static class HonoursNothingRestartConformance extends RestartConformance {

        @Override
        protected RestartableSubscriptionModelFixture createFixture() {
            return new NoopRestartableSubscriptionModelFixture();
        }
    }

    static class ResumingRestartConformance extends RestartConformance {

        @Override
        protected RestartableSubscriptionModelFixture createFixture() {
            return new WorkingRestartableSubscriptionModelFixture(true);
        }
    }

    static class StartingAtThePresentRestartConformance extends RestartConformance {

        @Override
        protected RestartableSubscriptionModelFixture createFixture() {
            return new WorkingRestartableSubscriptionModelFixture(false);
        }
    }

    private static class NoopSubscriptionModelFixture implements SubscriptionModelFixture {

        @Override
        public SubscriptionModel subscriptionModel() {
            return NoopSubscriptionModel.INSTANCE;
        }

        @Override
        public void publish(List<CloudEvent> events) {
            throw new UnsupportedOperationException("NoopSubscriptionModel has nothing to publish to");
        }

        @Override
        public boolean deliversEventsPublishedWhilePaused() {
            // Never reached: every call into the model throws before the suite consults this.
            return false;
        }

        @Override
        public boolean retriesAFailingHandler() {
            return true;
        }

        @Override
        public Checkpoint aCheckpointToStartFrom() {
            // Never reached either: subscribing throws before the position is applied to anything.
            return new StringBasedCheckpoint("noop");
        }
    }

    private static class NoopRestartableSubscriptionModelFixture extends NoopSubscriptionModelFixture
            implements RestartableSubscriptionModelFixture {

        @Override
        public SubscriptionModel restart() {
            return NoopSubscriptionModel.INSTANCE;
        }

        @Override
        public boolean resumesAfterARestart() {
            // Never reached: the model throws from subscribe long before the suite asks which branch to assert.
            return true;
        }
    }

    private static class WorkingSubscriptionModelFixture implements SubscriptionModelFixture {

        private final WorkingSubscriptionModel model;

        WorkingSubscriptionModelFixture() {
            this(Duration.ZERO);
        }

        WorkingSubscriptionModelFixture(Duration deliveryDelay) {
            this.model = new WorkingSubscriptionModel(deliveryDelay);
        }

        @Override
        public SubscriptionModel subscriptionModel() {
            return model;
        }

        @Override
        public void publish(List<CloudEvent> events) {
            model.feed(events);
        }

        @Override
        public boolean deliversEventsPublishedWhilePaused() {
            return false;
        }

        @Override
        public boolean retriesAFailingHandler() {
            return true;
        }

        @Override
        public Checkpoint aCheckpointToStartFrom() {
            // This model has no history, so every position means live. Its own globalCheckpoint() says the same thing.
            return model.globalCheckpoint();
        }

        @Override
        public void close() {
            model.shutdown();
        }
    }

    private static class WorkingRestartableSubscriptionModelFixture implements RestartableSubscriptionModelFixture {

        private final WorkingRestartableSubscriptionModel.Storage storage = new WorkingRestartableSubscriptionModel.Storage();
        private final boolean resumesAfterARestart;

        private WorkingRestartableSubscriptionModel model;

        WorkingRestartableSubscriptionModelFixture(boolean resumesAfterARestart) {
            this.resumesAfterARestart = resumesAfterARestart;
            this.model = new WorkingRestartableSubscriptionModel(storage, resumesAfterARestart);
        }

        @Override
        public SubscriptionModel subscriptionModel() {
            return model;
        }

        @Override
        public SubscriptionModel restart() {
            model.shutdown();
            model = new WorkingRestartableSubscriptionModel(storage, resumesAfterARestart);
            return model;
        }

        @Override
        public boolean resumesAfterARestart() {
            return resumesAfterARestart;
        }

        @Override
        public void publish(List<CloudEvent> events) {
            // Straight to the storage-backed model, which appends whether or not anything is listening. That is what
            // lets the suite publish into the gap a restart leaves.
            model.feed(events);
        }

        @Override
        public boolean deliversEventsPublishedWhilePaused() {
            return false;
        }

        @Override
        public boolean retriesAFailingHandler() {
            return false;
        }

        @Override
        public Checkpoint aCheckpointToStartFrom() {
            return new StringBasedCheckpoint("0");
        }

        @Override
        public void close() {
            model.shutdown();
        }
    }
}
