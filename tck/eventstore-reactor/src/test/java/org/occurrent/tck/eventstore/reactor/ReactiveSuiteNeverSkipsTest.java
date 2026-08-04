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

package org.occurrent.tck.eventstore.reactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * The reactive leaf's half of the never-skip rule, which the blocking leaf's {@code SuiteNeverSkipsTest} enforces for
 * every suite over there. Without this, {@link ReactiveEventStoreConformance} would be the one suite in the TCK where
 * somebody could reach for an {@code Assumption} and nothing would notice.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the reactive conformance suite")
class ReactiveSuiteNeverSkipsTest {

    @Test
    void fails_every_test_and_skips_none_of_them_against_a_store_that_honours_nothing() {
        Events tests = EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(HonoursNothingReactiveConformance.class))
                .execute()
                .testEvents();

        long started = tests.started().count();
        assertThat(started)
                .describedAs("the suite must actually run something, or its verdict is meaningless")
                .isPositive();
        assertThat(tests.failed().count())
                .describedAs("every test must fail against a store that honours nothing")
                .isEqualTo(started);
        assertThat(tests.succeeded().count())
                .describedAs("nothing may pass against a store that honours nothing")
                .isZero();
        assertThat(tests.skipped().count())
                .describedAs("the suites must never skip, which is why they use no Assumptions")
                .isZero();
        assertThat(tests.aborted().count())
                .describedAs("an aborted test is a skip wearing a different hat")
                .isZero();
    }

    /**
     * Not named {@code *Test}, so Surefire does not pick it up as a test of its own. It exists only for the run above to
     * select, and it is expected to fail every assertion it makes.
     */
    static class HonoursNothingReactiveConformance extends ReactiveEventStoreConformance {

        @Override
        protected ReactiveEventStoreFixture createFixture() {
            return new ReactiveEventStoreFixture() {
                @Override
                public EventStore eventStore() {
                    return NoopReactiveStore.INSTANCE;
                }

                @Override
                public EventStoreQueries queries() {
                    return NoopReactiveStore.INSTANCE;
                }

                @Override
                public EventStoreOperations operations() {
                    return NoopReactiveStore.INSTANCE;
                }

                @Override
                public PositionOrderedReader positionOrderedReader() {
                    return NoopReactiveStore.INSTANCE;
                }
            };
        }
    }
}
