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

package org.occurrent.tck.eventstore.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * A TCK that can be satisfied by doing nothing is worse than no TCK, so this runs {@link StreamEventStoreConformance},
 * {@link StreamConcurrencyConformance}, {@link StreamPositionConformance} and {@link StreamPositionDisabledConformance}
 * against a store that honours none of the contract and asserts that the suite notices. Every test must fail, none
 * may pass, and none may be skipped or aborted.
 * <p>
 * The last part is the one worth having. The suites are written without {@code Assumptions} on purpose, so that an
 * unsupported behaviour fails loudly instead of vanishing from the report, and this is the only check that the rule is
 * actually being followed rather than merely intended. If somebody later reaches for an assumption, the skipped count
 * stops being zero and this test says so. {@link StreamPositionDisabledConformance} additionally guards a second way
 * to skip: {@link EventStoreFixture#storeWithoutPosition()} answering {@link java.util.Optional#empty()}, which is
 * a legitimate answer for a fixture to give but must still fail the suite rather than pass it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a conformance suite")
class SuiteNeverSkipsTest {

    @Test
    void fails_every_test_and_skips_none_of_them_against_a_store_that_honours_nothing() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingConformance.class);
    }

    @Test
    void the_concurrency_suite_fails_every_test_and_skips_none_of_them_against_a_store_that_honours_nothing() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingConcurrencyConformance.class);
    }

    @Test
    void the_position_suite_fails_every_test_and_skips_none_of_them_against_a_store_that_honours_nothing() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingPositionConformance.class);
    }

    @Test
    void the_position_disabled_suite_fails_every_test_and_skips_none_of_them_against_a_fixture_that_declines_it() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingPositionDisabledConformance.class);
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
     * Not named {@code *Test}, so Surefire does not pick it up as a test of its own. It exists only for the run above
     * to select, and it is expected to fail every assertion it makes.
     */
    static class HonoursNothingConformance extends StreamEventStoreConformance {

        @Override
        protected EventStoreFixture createFixture() {
            return new EventStoreFixture() {
                @Override
                public Set<EventStoreCapability> capabilities() {
                    return Set.of(EventStoreCapability.STREAM);
                }

                @Override
                public EventStore eventStore() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public EventStoreQueries queries() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public EventStoreOperations operations() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public ReadEventStreamWithFilter filteredReader() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public PositionOrderedReader positionOrderedReader() {
                    return NoopStore.INSTANCE;
                }
            };
        }
    }

    /**
     * As {@link HonoursNothingConformance}, but for {@link StreamConcurrencyConformance}. Every write against
     * {@link NoopStore} throws {@link UnsupportedOperationException}, so both concurrency tests must fail rather than
     * pass or skip.
     */
    static class HonoursNothingConcurrencyConformance extends StreamConcurrencyConformance {

        @Override
        protected EventStoreFixture createFixture() {
            return new EventStoreFixture() {
                @Override
                public Set<EventStoreCapability> capabilities() {
                    return Set.of(EventStoreCapability.STREAM);
                }

                @Override
                public EventStore eventStore() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public EventStoreQueries queries() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public EventStoreOperations operations() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public ReadEventStreamWithFilter filteredReader() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public PositionOrderedReader positionOrderedReader() {
                    return NoopStore.INSTANCE;
                }
            };
        }
    }

    /**
     * As {@link HonoursNothingConformance}, but for {@link StreamPositionConformance}. Every write and every
     * position read against {@link NoopStore} throws {@link UnsupportedOperationException}, so every position test
     * must fail rather than pass or skip.
     */
    static class HonoursNothingPositionConformance extends StreamPositionConformance {

        @Override
        protected EventStoreFixture createFixture() {
            return new EventStoreFixture() {
                @Override
                public Set<EventStoreCapability> capabilities() {
                    return Set.of(EventStoreCapability.STREAM);
                }

                @Override
                public EventStore eventStore() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public EventStoreQueries queries() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public EventStoreOperations operations() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public ReadEventStreamWithFilter filteredReader() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public PositionOrderedReader positionOrderedReader() {
                    return NoopStore.INSTANCE;
                }
            };
        }
    }

    /**
     * As {@link HonoursNothingConformance}, but for {@link StreamPositionDisabledConformance}. This fixture leaves
     * {@link EventStoreFixture#storeWithoutPosition()} at its default, i.e. it declines to supply a store built with
     * position turned off, which is exactly the empty answer the suite must never let pass silently or skip. Every
     * test must still fail, just from that declined answer rather than from calling {@link NoopStore}.
     */
    static class HonoursNothingPositionDisabledConformance extends StreamPositionDisabledConformance {

        @Override
        protected EventStoreFixture createFixture() {
            return new EventStoreFixture() {
                @Override
                public Set<EventStoreCapability> capabilities() {
                    return Set.of(EventStoreCapability.STREAM);
                }

                @Override
                public EventStore eventStore() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public EventStoreQueries queries() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public EventStoreOperations operations() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public ReadEventStreamWithFilter filteredReader() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public PositionOrderedReader positionOrderedReader() {
                    return NoopStore.INSTANCE;
                }

                // storeWithoutPosition() deliberately left at its default: Optional.empty()
            };
        }
    }
}
