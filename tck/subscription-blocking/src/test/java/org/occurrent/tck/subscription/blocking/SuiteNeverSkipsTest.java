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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * A TCK that can be satisfied by doing nothing is worse than no TCK, so this runs
 * {@link CheckpointStorageConformance} against a storage that honours none of the contract and asserts that the suite
 * notices. Every test must fail, none may pass, and none may be skipped or aborted.
 * <p>
 * The last part is the one worth having. The suite is written without {@code Assumptions} on purpose, so that an
 * unsupported behaviour fails loudly instead of vanishing from the report, and this is the only check that the rule is
 * being followed rather than merely intended. If somebody later reaches for an assumption, the skipped count stops
 * being zero and this test says so.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a conformance suite")
class SuiteNeverSkipsTest {

    @Test
    void fails_every_test_and_skips_none_of_them_against_a_storage_that_honours_nothing() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingCheckpointStorageConformance.class);
    }

    private static void assertSuiteFailsEveryTestAndSkipsNone(Class<?> suite) {
        Events tests = EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(suite))
                .execute()
                .testEvents();

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
        assertThat(tests.skipped().count())
                .describedAs("the suite must never skip, which is why it uses no Assumptions")
                .isZero();
        assertThat(tests.aborted().count())
                .describedAs("an aborted test is a skip wearing a different hat")
                .isZero();
    }

    // Not named *Test, so Surefire does not pick it up as a test of its own. It exists only for the run above to
    // select, and it is expected to fail every assertion it makes.
    static class HonoursNothingCheckpointStorageConformance extends CheckpointStorageConformance {

        @Override
        protected CheckpointStorageFixture createFixture() {
            return new CheckpointStorageFixture() {
                @Override
                public org.occurrent.subscription.api.blocking.CheckpointStorage checkpointStorage() {
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
}
