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
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.*;
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;

/**
 * What an event store implementation hands the conformance suites.
 * <p>
 * A fixture is created fresh for every test method, and the store it hands back <strong>must contain no events</strong>.
 * How that is achieved is up to the implementation, whether that means dropping a collection, truncating a table, or
 * simply constructing a new in-memory instance. A suite never cleans up on an implementation's behalf, because what
 * needs cleaning is exactly the part a contract cannot describe.
 * <p>
 * The accessors are separate rather than one object the suites downcast, so a suite asks for precisely the capability
 * interface it exercises and an implementation says out loud which of them it can answer. Most implementations return
 * the same object from every accessor, which makes a fixture roughly one line per method.
 * <p>
 * Every other accessor here depends on {@link #capabilities()}. Occurrent's capabilities are a construction-time
 * argument to a store's config and nothing on {@link EventStore} reports them back, so the suites cannot discover
 * what a store supports and have to be told. Declaring a capability is a promise the suites will hold the
 * implementation to, including the negative. A store that does not declare {@link EventStoreCapability#DCB} is
 * expected to reject DCB calls rather than quietly accept them.
 */
@NullMarked
public interface EventStoreFixture {

    /**
     * The capabilities this store was constructed with. Never empty.
     * <p>
     * A suite that needs a capability this set does not contain fails immediately rather than skipping, so an
     * implementation cannot accidentally pass a suite by declaring less than it supports.
     */
    Set<EventStoreCapability> capabilities();

    /**
     * Whether this store accepts a natural sort step composed with a field sort, as in
     * {@code SortBy.time(DESCENDING).thenNatural(ASCENDING)}. Defaults to {@code false}, which is what three of the
     * four stores shipping with Occurrent do.
     * <p>
     * This is a variation the contract already documents on {@link org.occurrent.eventstore.api.SortBy#natural}, not
     * something the TCK discovered. The in-memory store treats natural order as an insertion-order tiebreaker for the
     * preceding fields, while the MongoDB stores reject the compound sort with an {@link IllegalArgumentException}
     * because MongoDB cannot express a natural sort combined with other keys.
     * <p>
     * The query suite does not skip either way. It asserts the tiebreaker ordering when this is {@code true} and the
     * rejection when it is {@code false}, so a store that quietly drops the natural step instead of doing one of those
     * two things fails.
     */
    default boolean composesNaturalSortWithFieldSorts() {
        return false;
    }

    /**
     * Whether {@link org.occurrent.eventstore.api.SortBy#natural} is this store's insertion order. Defaults to
     * {@code false}.
     * <p>
     * This describes what the datastore actually promises, not an aspiration. {@link org.occurrent.eventstore.api.SortBy#natural}
     * documents natural order as "typically the insertion order, but it could also be undefined for certain
     * datastores", so a fixture answering {@code false} is not failing anything. It is saying its store falls on the
     * "could be undefined" side, which is true of every MongoDB store shipping with Occurrent, since {@code $natural}
     * on a non-capped collection is not a documented insertion-order guarantee.
     * <p>
     * Declaring {@code true} is a stronger promise than the default and is asserted accordingly. The query suite
     * checks insertion order itself, in both ascending and descending direction, rather than only checking that every
     * event comes back once.
     */
    default boolean naturalOrderIsInsertionOrder() {
        return false;
    }

    /**
     * Whether this store can filter on a field inside a CloudEvent's {@code data} payload with {@link
     * org.occurrent.filter.Filter#data}. Defaults to {@code true}, which is what every MongoDB store shipping with
     * Occurrent does. It parses the payload into BSON on write specifically so a later {@code Filter.data(..)} can
     * reach inside it.
     * <p>
     * A fixture answering {@code false} is documenting a real limitation, not a bug. The in-memory store keeps a
     * payload as opaque bytes and has nothing to reach into, so it must reject {@code Filter.data(..)} with an
     * {@link UnsupportedOperationException}, the same way a store refuses any capability it was not built with, rather
     * than silently ignoring it or scanning every payload without an index.
     */
    default boolean supportsDataFilter() {
        return true;
    }

    /**
     * The finest precision this store keeps for a CloudEvent's {@code time} attribute. Defaults to
     * {@link ChronoUnit#NANOS}, which is what the in-memory store and MongoDB's {@code RFC_3339_STRING} both manage.
     * <p>
     * <strong>A store must refuse a time it cannot represent, not round it off.</strong> That is the contract, not an
     * implementation detail. An event is the record of something that happened, and a silently truncated timestamp
     * cannot be detected afterwards by anything. MongoDB's {@code DATE} representation stores a millisecond epoch value
     * and throws rather than lose the rest, which is the behaviour to copy.
     * <p>
     * This is a {@link ChronoUnit} rather than a flag because precision is not binary. A relational store on a
     * {@code timestamp(6)} column keeps microseconds, which no boolean can express.
     */
    default ChronoUnit timePrecision() {
        return ChronoUnit.NANOS;
    }

    /**
     * Whether this store gives back a CloudEvent's {@code time} carrying the same UTC offset it was written with.
     * Defaults to {@code true}.
     * <p>
     * A store answering {@code false} must refuse a time that is not already in UTC, for the same reason as
     * {@link #timePrecision()}. Quietly rewriting {@code +02:00} to {@code Z} preserves the instant and loses the
     * offset, and nothing downstream can tell that it happened. MongoDB's {@code DATE} representation cannot hold an
     * offset, so it rejects one.
     */
    default boolean preservesTimeOffset() {
        return true;
    }

    /**
     * The stream-capability store under test. Required when {@link EventStoreCapability#STREAM} is declared.
     */
    default EventStore eventStore() {
        throw notOverridden("eventStore", EventStoreCapability.STREAM);
    }

    /**
     * The query capability. Required when {@link EventStoreCapability#STREAM} is declared, because
     * {@link EventStoreQueries} reads across streams rather than within one.
     */
    default EventStoreQueries queries() {
        throw notOverridden("queries", EventStoreCapability.STREAM);
    }

    /**
     * The operations capability, covering deleting streams and single events, deleting by filter, and updating an
     * event.
     */
    default EventStoreOperations operations() {
        throw notOverridden("operations", EventStoreCapability.STREAM);
    }

    /**
     * Reading a single stream through a {@link org.occurrent.eventstore.api.StreamReadFilter}.
     */
    default ReadEventStreamWithFilter filteredReader() {
        throw notOverridden("filteredReader", EventStoreCapability.STREAM);
    }

    /**
     * The Dynamic Consistency Boundary store. Required when {@link EventStoreCapability#DCB} is declared.
     */
    default DcbEventStore dcbEventStore() {
        throw notOverridden("dcbEventStore", EventStoreCapability.DCB);
    }

    /**
     * How this store decides whether a token-qualified DCB append condition was violated. Required when
     * {@link EventStoreCapability#DCB} is declared, with no default, because there is no answer that is right often
     * enough to inherit and getting it wrong makes the suite assert the opposite of what the store does.
     * <p>
     * This is a declaration rather than a question put to the store because there is nothing to ask. The model is a
     * property of how the write path is built, and no method on {@link DcbEventStore} reports it. {@code timePrecision()}
     * is answered the same way, by declaration, which is the opposite of {@code PositionOrderedReader.writesPosition()},
     * which the suites do ask the store directly.
     */
    default DcbAppendConditionModel appendConditionModel() {
        throw notOverridden("appendConditionModel", EventStoreCapability.DCB);
    }

    /**
     * Position-ordered reads. Required by every store. A store that does not write positions still has to answer for
     * that, and the suite asks the store itself through {@link PositionOrderedReader#writesPosition()} rather than
     * being told in advance by the fixture.
     */
    default PositionOrderedReader positionOrderedReader() {
        throw notOverridden("positionOrderedReader", EventStoreCapability.STREAM);
    }

    /**
     * A store built with its global position turned off, for the suite that asserts the position-disabled contract.
     * Defaults to empty, meaning this implementation cannot build a store with position off.
     * <p>
     * Supplying one opts the implementation into the position-disabled conformance assertions. The returned store
     * must be STREAM-only. {@link EventStoreCapability#DCB} always writes a position, and the stores reject building
     * one with DCB and position disabled together.
     */
    default Optional<StoreWithoutPosition> storeWithoutPosition() {
        return Optional.empty();
    }

    /**
     * A store built with {@link EventStoreCapability#STREAM} alone. Defaults to empty, meaning this implementation
     * cannot build a store that leaves DCB out.
     * <p>
     * Supplying one opts the implementation into {@link CapabilityGuardConformance}, which asserts that every DCB call
     * on it refuses while its stream capability still works.
     */
    default Optional<StoreWithoutDcb> storeWithoutDcb() {
        return Optional.empty();
    }

    /**
     * A store built with {@link EventStoreCapability#DCB} alone. Defaults to empty, meaning this implementation cannot
     * build a store that leaves STREAM out.
     * <p>
     * Supplying one opts the implementation into {@link CapabilityGuardConformance}, which asserts that every stream
     * call on it refuses while its DCB capability still works.
     */
    default Optional<StoreWithoutStream> storeWithoutStream() {
        return Optional.empty();
    }

    /**
     * Releases whatever the fixture holds. Called after every test method, including a failing one.
     */
    default void close() {
    }

    private UnsupportedOperationException notOverridden(String accessor, EventStoreCapability capability) {
        return new UnsupportedOperationException(getClass().getName() + " declares " + capability
                + " but does not override " + accessor + "(). Either override it, or stop declaring " + capability
                + " in capabilities().");
    }
}
