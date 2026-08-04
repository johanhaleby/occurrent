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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * A TCK that can be satisfied by doing nothing is worse than no TCK, so {@link CheckpointStorageConformance} is run
 * here twice, against a storage that honours none of the contract and against one that honours all of it. The first run
 * must fail every test, the second must pass every test, and neither may skip or abort anything.
 * <p>
 * The two runs answer different questions and the second one is the reason there are two. A storage that throws from
 * every method dies on the first call in each test, so that run says nothing about code further down a test method: an
 * {@code Assumptions} call placed after the first {@code save} would never be reached and the skipped count would stay
 * zero. Running the whole suite green against a working storage does reach every line, so a skip anywhere in the suite
 * body shows up as a non-zero skipped count here.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a conformance suite")
class SuiteNeverSkipsTest {

    @Test
    void fails_every_test_and_skips_none_of_them_against_a_storage_that_honours_nothing() {
        Events tests = run(HonoursNothingCheckpointStorageConformance.class);

        long started = tests.started().count();
        assertThat(started)
                .describedAs("the suite must actually run something, or its verdict is meaningless")
                .isPositive();
        assertThat(tests.failed().count())
                .describedAs("every test must fail against a storage that honours nothing")
                .isEqualTo(started);
        assertThat(tests.succeeded().count())
                .describedAs("nothing may pass against a storage that honours nothing")
                .isZero();
        assertSkipsNothing(tests);
    }

    @Test
    void passes_every_test_and_skips_none_of_them_against_a_storage_that_honours_everything() {
        Events tests = run(HonoursEverythingCheckpointStorageConformance.class);

        long started = tests.started().count();
        assertThat(started)
                .describedAs("the suite must actually run something, or its verdict is meaningless")
                .isPositive();
        assertThat(tests.succeeded().count())
                .describedAs("a storage that honours the whole contract must pass the whole suite, so a test that "
                        + "cannot be satisfied by any implementation is caught here rather than by whoever adds the "
                        + "next storage")
                .isEqualTo(started);
        assertThat(tests.failed().count()).isZero();
        assertSkipsNothing(tests);
    }

    private static void assertSkipsNothing(Events tests) {
        assertThat(tests.skipped().count())
                .describedAs("the suite must never skip, which is why it uses no Assumptions")
                .isZero();
        assertThat(tests.aborted().count())
                .describedAs("an aborted test is a skip wearing a different hat")
                .isZero();
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
}
