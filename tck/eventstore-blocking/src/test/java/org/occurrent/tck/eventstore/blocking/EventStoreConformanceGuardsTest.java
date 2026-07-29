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
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * The rules that stop a conformance suite from quietly testing nothing have no other coverage, so they get their own
 * tests. Everything here drives {@link EventStoreConformance}'s lifecycle hook directly, which the suites in this
 * package can reach because they share its package.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the conformance guards")
class EventStoreConformanceGuardsTest {

    @Test
    void reject_a_fixture_that_declares_less_than_the_suite_needs_instead_of_skipping() {
        EventStoreConformance suite = suiteRequiring(Set.of(STREAM, DCB), fixtureDeclaring(Set.of(STREAM)));

        Throwable thrown = catchThrowable(suite::createFixtureAndCheckItsDeclaration);

        assertThat(thrown)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exercises [STREAM, DCB]")
                .hasMessageContaining("declares only [STREAM]")
                .hasMessageContaining("[DCB] cannot be tested")
                .hasMessageContaining("stop extending this suite");
    }

    @Test
    void reject_a_fixture_that_declares_no_capabilities_at_all() {
        EventStoreConformance suite = suiteRequiring(Set.of(STREAM), fixtureDeclaring(Set.of()));

        Throwable thrown = catchThrowable(suite::createFixtureAndCheckItsDeclaration);

        assertThat(thrown)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("declares no capabilities");
    }

    @Test
    void reject_a_fixture_that_declares_a_capability_it_has_not_wired_up() {
        EventStoreFixture declaresDcbWithoutWiringIt = new EventStoreFixture() {
            @Override
            public Set<EventStoreCapability> capabilities() {
                return Set.of(STREAM, DCB);
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
            // dcbEventStore() deliberately left at its default
        };

        EventStoreConformance suite = suiteRequiring(Set.of(STREAM, DCB), declaresDcbWithoutWiringIt);

        Throwable thrown = catchThrowable(suite::createFixtureAndCheckItsDeclaration);

        assertThat(thrown)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("declares DCB but does not override dcbEventStore()")
                .hasMessageContaining("stop declaring DCB in capabilities()");
    }

    @Test
    void accept_a_fixture_that_declares_more_than_the_suite_needs() {
        EventStoreConformance suite = suiteRequiring(Set.of(STREAM), new StubFixture(Set.of(STREAM, DCB)) {
            @Override
            public DcbEventStore dcbEventStore() {
                return NoopStore.INSTANCE;
            }
        });

        suite.createFixtureAndCheckItsDeclaration();

        assertThat(suite.fixture().capabilities()).containsExactlyInAnyOrder(STREAM, DCB);
    }

    @Test
    void report_that_there_is_no_fixture_rather_than_a_null_pointer_when_asked_outside_a_test() {
        EventStoreConformance suite = suiteRequiring(Set.of(STREAM), fixtureDeclaring(Set.of(STREAM)));

        Throwable thrown = catchThrowable(suite::fixture);

        assertThat(thrown)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No fixture");
    }

    @Test
    void close_the_fixture_after_a_test_even_when_the_test_failed() {
        CountingFixture fixture = new CountingFixture();
        EventStoreConformance suite = suiteRequiring(Set.of(STREAM), fixture);

        suite.createFixtureAndCheckItsDeclaration();
        suite.closeFixture();

        assertThat(fixture.closed).isEqualTo(1);
    }

    private static EventStoreConformance suiteRequiring(Set<EventStoreCapability> required, EventStoreFixture fixture) {
        return new EventStoreConformance() {
            @Override
            protected EventStoreFixture createFixture() {
                return fixture;
            }

            @Override
            protected Set<EventStoreCapability> requiredCapabilities() {
                return required;
            }
        };
    }

    private static StubFixture fixtureDeclaring(Set<EventStoreCapability> capabilities) {
        return new StubFixture(capabilities);
    }

    private static class StubFixture implements EventStoreFixture {
        private final Set<EventStoreCapability> capabilities;

        StubFixture(Set<EventStoreCapability> capabilities) {
            this.capabilities = capabilities;
        }

        @Override
        public Set<EventStoreCapability> capabilities() {
            return capabilities;
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
    }

    private static final class CountingFixture extends StubFixture {
        private int closed;

        CountingFixture() {
            super(Set.of(STREAM));
        }

        @Override
        public void close() {
            closed++;
        }
    }
}
