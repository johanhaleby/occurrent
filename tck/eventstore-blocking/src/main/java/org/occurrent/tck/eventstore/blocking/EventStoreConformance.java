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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import java.util.EnumSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The base every blocking event-store conformance suite extends. It owns the fixture lifecycle and the rule that a
 * suite must never quietly test nothing.
 * <p>
 * An implementation does not extend this directly. It extends one of the concrete suites, once per capability it
 * declares, and supplies a fixture:
 * <pre>{@code
 * class PostgresqlEventStoreTest extends StreamEventStoreConformance {
 *     @Override
 *     protected EventStoreFixture createFixture() {
 *         return new PostgresqlEventStoreFixture();
 *     }
 * }
 * }</pre>
 * <p>
 * Not extending a suite is how an implementation declines a capability. That is deliberately a visible, greppable
 * absence rather than a runtime skip, because a suite that skips silently is worse than no suite at all. For the same
 * reason nothing in this TCK calls {@code Assumptions}: where behaviour legitimately differs, the fixture declares
 * which way it goes and the suite asserts the documented outcome for that answer, so both branches are checked by
 * somebody.
 */
@NullMarked
public abstract class EventStoreConformance {

    private @Nullable EventStoreFixture fixture;

    /**
     * Creates a fixture whose store contains no events. Called before every test method.
     */
    protected abstract EventStoreFixture createFixture();

    /**
     * The capabilities this suite exercises. A fixture that does not declare all of them fails the suite immediately,
     * with a message naming what is missing.
     */
    protected abstract Set<EventStoreCapability> requiredCapabilities();

    @BeforeEach
    final void createFixtureAndCheckItsDeclaration() {
        EventStoreFixture created = requireNonNull(createFixture(), "createFixture() returned null");

        // Copied into an EnumSet so every message below lists capabilities in enum declaration order. A Set.of(..)
        // fixture would otherwise word the same failure differently from one run to the next.
        Set<EventStoreCapability> declared = asEnumSet(requireNonNull(created.capabilities(),
                created.getClass().getName() + " returned null from capabilities()"));
        if (declared.isEmpty()) {
            throw new IllegalStateException(created.getClass().getName()
                    + " declares no capabilities. A store that supports nothing cannot be conformance tested, so this "
                    + "is a fixture bug rather than a store without capabilities.");
        }

        Set<EventStoreCapability> required = asEnumSet(requireNonNull(requiredCapabilities(),
                getClass().getName() + " returned null from requiredCapabilities()"));
        Set<EventStoreCapability> missing = asEnumSet(required);
        missing.removeAll(declared);
        if (!missing.isEmpty()) {
            throw new IllegalStateException(getClass().getName() + " exercises " + required
                    + " but " + created.getClass().getName() + " declares only " + declared + ", so " + missing
                    + " cannot be tested. Either declare " + missing + " on the fixture, or stop extending this suite "
                    + "for this store.");
        }

        // Touch each accessor the suite needs now, so a fixture that declares a capability without wiring it up says
        // so before the first assertion rather than halfway through an unrelated test.
        this.fixture = created;
        if (required.contains(EventStoreCapability.STREAM)) {
            created.eventStore();
            created.queries();
            created.operations();
            created.filteredReader();
            // Every store owes an answer here, even one that does not write positions, so a fixture that forgot to
            // wire it should say so now rather than in whichever later suite happens to ask first.
            created.positionOrderedReader();
        }
        if (required.contains(EventStoreCapability.DCB)) {
            created.dcbEventStore();
            created.appendConditionModel();
        }
    }

    @AfterEach
    final void closeFixture() {
        EventStoreFixture current = this.fixture;
        this.fixture = null;
        if (current != null) {
            current.close();
        }
    }

    /**
     * The fixture for the running test.
     */
    protected final EventStoreFixture fixture() {
        EventStoreFixture current = this.fixture;
        if (current == null) {
            throw new IllegalStateException("No fixture. The conformance suites create one per test method, so this "
                    + "is only reachable from a constructor or a @BeforeAll, neither of which a suite should use.");
        }
        return current;
    }

    protected final EventStore eventStore() {
        return fixture().eventStore();
    }

    protected final EventStoreQueries queries() {
        return fixture().queries();
    }

    protected final EventStoreOperations operations() {
        return fixture().operations();
    }

    protected final ReadEventStreamWithFilter filteredReader() {
        return fixture().filteredReader();
    }

    protected final DcbEventStore dcbEventStore() {
        return fixture().dcbEventStore();
    }

    protected final DcbAppendConditionModel appendConditionModel() {
        return fixture().appendConditionModel();
    }

    protected final PositionOrderedReader positionOrderedReader() {
        return fixture().positionOrderedReader();
    }

    private static EnumSet<EventStoreCapability> asEnumSet(Set<EventStoreCapability> capabilities) {
        EnumSet<EventStoreCapability> copy = EnumSet.noneOf(EventStoreCapability.class);
        copy.addAll(capabilities);
        return copy;
    }
}
