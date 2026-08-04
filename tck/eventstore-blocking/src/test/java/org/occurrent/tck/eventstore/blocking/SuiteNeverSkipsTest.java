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
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * A TCK that can be satisfied by doing nothing is worse than no TCK, and the suites here are written without
 * {@code Assumptions} on purpose so that an unsupported behaviour fails loudly instead of vanishing from the report.
 * Two different checks keep that true, because neither is enough alone.
 * <p>
 * <strong>Running a suite against a store that honours nothing.</strong> {@link StreamEventStoreConformance},
 * {@link StreamConcurrencyConformance}, {@link StreamPositionConformance}, {@link StreamPositionDisabledConformance},
 * {@link DcbEventStoreConformance}, {@link DcbStreamInteropConformance}, {@link DcbConcurrencyConformance} and
 * {@link CapabilityGuardConformance} are each run against {@link NoopStore} and must notice: every test fails, none
 * passes, none is skipped or aborted. That establishes each suite has tests and that they really do assert something.
 * It does not establish the no-skipping rule, because every test dies on its first call into the store, so an
 * assumption placed anywhere after that is never reached and the skipped count stays zero either way.
 * {@link StreamPositionDisabledConformance} does earn something the others do not here: a second way to skip is
 * {@link EventStoreFixture#storeWithoutPosition()} answering {@link java.util.Optional#empty()}, which is a legitimate
 * answer for a fixture to give but must still fail the suite rather than pass it, and
 * {@link CapabilityGuardConformance} covers the same shape for its two restricted-store accessors.
 * <p>
 * <strong>Scanning the compiled suites for anything that can skip.</strong> This is what earns the no-skipping claim.
 * {@link SkipMechanismScan} reads the class files this module compiles and fails if any of them so much as references
 * {@code Assumptions}, {@code TestAbortedException}, {@code @Disabled} or a {@code @DisabledIf} condition. It covers
 * every line of every suite rather than the lines one fixture's declarations reach, and it covers every suite in the
 * module rather than the ones listed above: {@link EventStoreQueriesConformance},
 * {@link EventStoreOperationsConformance} and {@link EventStoreTimePrecisionConformance} have no run of their own here
 * and rest on the scan alone, as does any suite added later.
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

    @Test
    void the_dcb_suite_fails_every_test_and_skips_none_of_them_against_a_store_that_honours_nothing() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingDcbConformance.class);
    }

    @Test
    void the_dcb_stream_interop_suite_fails_every_test_and_skips_none_of_them_against_a_store_that_honours_nothing() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingDcbStreamInteropConformance.class);
    }

    @Test
    void the_dcb_concurrency_suite_fails_every_test_and_skips_none_of_them_against_a_store_that_honours_nothing() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingDcbConcurrencyConformance.class);
    }

    @Test
    void the_capability_guard_suite_fails_every_test_and_skips_none_of_them_against_a_fixture_that_declines_it() {
        assertSuiteFailsEveryTestAndSkipsNone(HonoursNothingCapabilityGuardConformance.class);
    }

    @Test
    void names_nothing_that_could_skip_a_test_in_any_suite_it_compiles() {
        assertThat(SkipMechanismScan.classesScannedAlongside(EventStoreConformance.class))
                .describedAs("the scan must reach the suites, or a clean verdict means only that it looked nowhere")
                .contains(EventStoreConformance.class.getName(), StreamEventStoreConformance.class.getName(),
                        EventStoreQueriesConformance.class.getName(), EventStoreOperationsConformance.class.getName(),
                        EventStoreTimePrecisionConformance.class.getName());

        SortedMap<String, List<String>> offenders = SkipMechanismScan.of(EventStoreConformance.class);

        assertThat(offenders)
                .describedAs("a skipped test vanishes from the report, so a store that does not honour a contract ends "
                        + "up looking like one that does. Where stores legitimately differ the fixture declares the "
                        + "difference and the suite asserts both answers, which is why nothing here may skip")
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

    /**
     * As {@link HonoursNothingConformance}, but for {@link DcbEventStoreConformance}. Every DCB call against
     * {@link NoopStore} throws {@link UnsupportedOperationException}, so every DCB test must fail rather than pass or
     * skip.
     */
    static class HonoursNothingDcbConformance extends DcbEventStoreConformance {

        @Override
        protected EventStoreFixture createFixture() {
            return new EventStoreFixture() {
                @Override
                public Set<EventStoreCapability> capabilities() {
                    return Set.of(EventStoreCapability.DCB);
                }

                @Override
                public DcbEventStore dcbEventStore() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public DcbAppendConditionModel appendConditionModel() {
                    return DcbAppendConditionModel.EXACT_CRITERIA;
                }
            };
        }
    }

    /**
     * As {@link HonoursNothingConformance}, but for {@link DcbStreamInteropConformance}, which needs both
     * {@link EventStoreCapability#STREAM} and {@link EventStoreCapability#DCB}. Every call against {@link NoopStore}
     * throws {@link UnsupportedOperationException}, so every test must fail rather than pass or skip.
     */
    static class HonoursNothingDcbStreamInteropConformance extends DcbStreamInteropConformance {

        @Override
        protected EventStoreFixture createFixture() {
            return new EventStoreFixture() {
                @Override
                public Set<EventStoreCapability> capabilities() {
                    return Set.of(EventStoreCapability.STREAM, EventStoreCapability.DCB);
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

                @Override
                public DcbEventStore dcbEventStore() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public DcbAppendConditionModel appendConditionModel() {
                    return DcbAppendConditionModel.EXACT_CRITERIA;
                }
            };
        }
    }

    /**
     * As {@link HonoursNothingConformance}, but for {@link CapabilityGuardConformance}. This fixture leaves
     * {@link EventStoreFixture#storeWithoutDcb()} and {@link EventStoreFixture#storeWithoutStream()} at their defaults,
     * i.e. it declines to build a store restricted to one capability, which is the second way this suite could quietly
     * pass. Every test must fail from that declined answer rather than skip, the same rule
     * {@link HonoursNothingPositionDisabledConformance} covers for the position-disabled suite.
     */
    static class HonoursNothingCapabilityGuardConformance extends CapabilityGuardConformance {

        @Override
        protected EventStoreFixture createFixture() {
            return new EventStoreFixture() {
                @Override
                public Set<EventStoreCapability> capabilities() {
                    return Set.of(EventStoreCapability.STREAM, EventStoreCapability.DCB);
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

                @Override
                public DcbEventStore dcbEventStore() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public DcbAppendConditionModel appendConditionModel() {
                    return DcbAppendConditionModel.EXACT_CRITERIA;
                }

                // storeWithoutDcb() and storeWithoutStream() deliberately left at their defaults: Optional.empty()
            };
        }
    }

    /**
     * As {@link HonoursNothingConformance}, but for {@link DcbConcurrencyConformance}. Every append against
     * {@link NoopStore} throws {@link UnsupportedOperationException}, so a race between them has no winner and every
     * test must fail rather than pass or skip.
     */
    static class HonoursNothingDcbConcurrencyConformance extends DcbConcurrencyConformance {

        @Override
        protected EventStoreFixture createFixture() {
            return new EventStoreFixture() {
                @Override
                public Set<EventStoreCapability> capabilities() {
                    return Set.of(EventStoreCapability.DCB);
                }

                @Override
                public DcbEventStore dcbEventStore() {
                    return NoopStore.INSTANCE;
                }

                @Override
                public DcbAppendConditionModel appendConditionModel() {
                    return DcbAppendConditionModel.EXACT_CRITERIA;
                }
            };
        }
    }
}
