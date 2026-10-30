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

package org.occurrent.eventstore.inmemory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.tck.eventstore.blocking.DcbLateCommitConformance;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.tck.eventstore.blocking.StreamLateCommitConformance;
import org.occurrent.tck.latecommit.AppendHold;
import org.occurrent.tck.latecommit.HeldAppend;
import org.occurrent.tck.latecommit.LateCommits;

import org.jspecify.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * The late-commit suites trust a fixture's hold, so they check it. Each fixture here gets the hold or the declaration
 * wrong in one way, and every test of the suite must fail with an {@link IllegalStateException} that says so. It must
 * not be a {@code GuaranteeViolated}, because that would let an expected-violation listing hide a broken fixture.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the late-commit suites")
class LateCommitChecksTest {

    @Test
    void reject_a_hold_that_sits_before_the_store_assigns_the_position() {
        assertEveryTestFailsWith(TooEarlyDcbSuite.class, "the hold did not enter the window");
    }

    @Test
    void reject_a_hook_that_never_engages_for_a_dcb_append() {
        assertEveryTestFailsWith(NeverEngagesDcbSuite.class, "completed without being held");
    }

    @Test
    void reject_a_hook_that_never_engages_for_a_stream_write() {
        assertEveryTestFailsWith(NeverEngagesStreamSuite.class, "completed without being held");
    }

    @Test
    void reject_a_store_declared_to_commit_in_position_order_that_does_not_make_the_append_visible() {
        assertEveryTestFailsWith(DeclaresInOrderButDropsDcbSuite.class, "not visible once it had returned");
    }

    @Test
    void reject_a_stream_store_declared_to_commit_in_position_order_that_does_not_make_the_write_visible() {
        assertEveryTestFailsWith(DeclaresInOrderButDropsStreamSuite.class, "not visible once it had returned");
    }

    private static void assertEveryTestFailsWith(Class<?> suite, String message) {
        Events tests = EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(suite))
                .execute()
                .testEvents();

        assertThat(tests.started().count()).isPositive();
        assertThat(tests.failed().count()).isEqualTo(tests.started().count());
        assertThat(tests.failed().stream())
                .allSatisfy(event -> assertThat(event.getRequiredPayload(TestExecutionResult.class).getThrowable().orElseThrow())
                        .isExactlyInstanceOf(IllegalStateException.class)
                        .hasMessageContaining(message));
    }

    // None of these is named *Test, so Surefire does not pick them up as tests of their own.

    static class TooEarlyDcbSuite extends DcbLateCommitConformance {
        @Override
        protected EventStoreFixture createFixture() {
            return new Fixture(Fixture.OWN_DCB, Fixture.OWN_STREAM, store -> {
                AppendHold hold = new AppendHold();
                DcbEventStore holdsBeforeAssigningAPosition = delegatingTo(DcbEventStore.class, store, "append", () -> {
                    if (hold.engage()) {
                        hold.awaitRelease();
                    }
                }, store);
                return HeldAppend.of(hold, holdsBeforeAssigningAPosition, store, () -> {
                });
            });
        }
    }

    static class NeverEngagesDcbSuite extends DcbLateCommitConformance {
        @Override
        protected EventStoreFixture createFixture() {
            return new Fixture(Fixture.OWN_DCB, Fixture.OWN_STREAM,
                    store -> HeldAppend.of(new AppendHold(), store, () -> {
                    }));
        }
    }

    static class NeverEngagesStreamSuite extends StreamLateCommitConformance {
        @Override
        protected EventStoreFixture createFixture() {
            return new Fixture(Fixture.OWN_DCB, Fixture.OWN_STREAM,
                    store -> HeldAppend.of(new AppendHold(), store, () -> {
                    }));
        }
    }

    static class DeclaresInOrderButDropsDcbSuite extends DcbLateCommitConformance {
        @Override
        protected EventStoreFixture createFixture() {
            return new Fixture(store -> delegatingTo(DcbEventStore.class, store, "append", () -> {
            }, new InMemoryEventStore()), Fixture.OWN_STREAM, null);
        }
    }

    static class DeclaresInOrderButDropsStreamSuite extends StreamLateCommitConformance {
        @Override
        protected EventStoreFixture createFixture() {
            return new Fixture(Fixture.OWN_DCB, store -> delegatingTo(EventStore.class, store, "write", () -> {
            }, new InMemoryEventStore()), null);
        }
    }

    /**
     * The in-memory fixture with its late-commit answer replaced, and optionally its DCB or stream store wrapped. The
     * wrappers get the fixture's own store, so reads that the fixture does not wrap see the same data.
     */
    private static final class Fixture extends InMemoryEventStoreConformanceFixture {
        static final Function<InMemoryEventStore, DcbEventStore> OWN_DCB = store -> store;
        static final Function<InMemoryEventStore, EventStore> OWN_STREAM = store -> store;

        private final Function<InMemoryEventStore, DcbEventStore> dcb;
        private final Function<InMemoryEventStore, EventStore> stream;
        private final @Nullable Function<InMemoryEventStore, HeldAppend> holds;

        Fixture(Function<InMemoryEventStore, DcbEventStore> dcb, Function<InMemoryEventStore, EventStore> stream,
                @Nullable Function<InMemoryEventStore, HeldAppend> holds) {
            this.dcb = dcb;
            this.stream = stream;
            this.holds = holds;
        }

        private InMemoryEventStore own() {
            return (InMemoryEventStore) super.eventStore();
        }

        @Override
        public DcbEventStore dcbEventStore() {
            return dcb.apply(own());
        }

        @Override
        public EventStore eventStore() {
            return stream.apply(own());
        }

        @Override
        public LateCommits lateCommits() {
            return holds == null ? LateCommits.inPositionOrder() : LateCommits.holding(() -> holds.apply(own()));
        }
    }

    /**
     * Forwards every call to {@code store}, except that {@code method} first runs {@code before} and then goes to
     * {@code target}.
     */
    @SuppressWarnings("unchecked")
    private static <T> T delegatingTo(Class<T> type, Object store, String method, Runnable before, Object target) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, called, args) -> {
            try {
                if (called.getName().equals(method)) {
                    before.run();
                    return called.invoke(target, args);
                }
                return called.invoke(store, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }
}
