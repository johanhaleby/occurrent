/*
 * Copyright 2020 Johan Haleby
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

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.SortBy.MultipleSortStepsImpl;
import org.occurrent.eventstore.api.SortBy.NaturalImpl;
import org.occurrent.eventstore.api.SortBy.SingleFieldImpl;
import org.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import org.occurrent.eventstore.api.blocking.*;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.internal.StreamReadFilterToFilterMapper;
import org.occurrent.eventstore.api.internal.StreamReadFilterValidator;
import org.occurrent.eventstore.api.internal.UpdateEventFunctionValidator;
import org.occurrent.filter.Filter;
import org.occurrent.functionalsupport.internal.FunctionalSupport.Pair;
import org.occurrent.filtermatching.DataFieldReader;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.cloudevents.core.v1.CloudEventV1.*;
import static java.util.Comparator.comparing;
import static java.util.Comparator.nullsFirst;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.zip;
import static org.occurrent.inmemory.filtermatching.FilterMatcher.matchesFilter;

/**
 * This is an {@link EventStore} that stores events in-memory. This is mainly useful for testing
 * and/or demo purposes. It also supports the {@link EventStoreOperations} contract.
 */
@NullMarked
public class InMemoryEventStore implements EventStore, EventStoreOperations, EventStoreQueries, ReadEventStreamWithFilter, DcbEventStore, PositionOrderedReader {

    // We cannot use ConcurrentMap since it doesn't maintain insertion order
    private final Map<String, CopyOnWriteArrayList<CloudEvent>> state = Collections.synchronizedMap(new LinkedHashMap<>());
    private final AtomicLong nextPosition = new AtomicLong(1);

    // Global insertion order assigned to each event at write time, keyed by "id + source" (same key as
    // validateNoDuplicateEventExists). Backs SortBy.natural, so natural order means global insertion order
    // (matching MongoDB's $natural), not the per-stream grouping from iterating "state".
    private final AtomicLong insertionSequence = new AtomicLong();
    private final Map<String, Long> insertionOrderByEventKey = new ConcurrentHashMap<>();

    private final Consumer<List<CloudEvent>> listener;
    private final DcbStreamIdGenerator dcbStreamIdGenerator;
    // Whether stream-written events get a global position from the same counter DCB uses, so stream and DCB events
    // share one sequence. Turn it off with withoutStreamPosition() for a STREAM-only store that wants no position.
    private final boolean streamPositionEnabled;

    // Reads a field out of an event's data payload so Filter.data(..) can be answered. Refuses by default, since
    // reading a payload means parsing it and this store has no parser. Supply one with withDataFieldReader(..).
    private final DataFieldReader dataFieldReader;

    /**
     * Create an instance of {@link InMemoryEventStore}
     */
    public InMemoryEventStore() {
        // @formatter:off
        this(__ -> {});
        // @formatter:on
    }

    /**
     * Create an instance of {@link InMemoryEventStore} that has a <code>listener</code> that will be invoked
     * after events have been written to the event store. This is typically not something you should implement
     * yourself, it's mainly here to allow the in-memory repository to work with "subscriptions". See the
     * in-memory subscription model implementation.
     *
     * @param listener A listener that will be invoked after events have been written to the datastore (synchronously!)
     */
    public InMemoryEventStore(Consumer<List<CloudEvent>> listener) {
        this(listener, new PartitionedDcbStreamIdGenerator());
    }

    /**
     * Create an instance of {@link InMemoryEventStore} with a <code>listener</code> and a custom
     * {@link DcbStreamIdGenerator} that decides which Occurrent storage stream DCB-written events are placed in.
     *
     * @param listener             A listener that will be invoked after events have been written to the datastore (synchronously!)
     * @param dcbStreamIdGenerator Derives the storage stream id for DCB appends from the events' DCB tags
     */
    public InMemoryEventStore(Consumer<List<CloudEvent>> listener, DcbStreamIdGenerator dcbStreamIdGenerator) {
        this(listener, dcbStreamIdGenerator, true);
    }

    private InMemoryEventStore(Consumer<List<CloudEvent>> listener, DcbStreamIdGenerator dcbStreamIdGenerator, boolean streamPositionEnabled) {
        this(listener, dcbStreamIdGenerator, streamPositionEnabled, DataFieldReader.refusing());
    }

    private InMemoryEventStore(Consumer<List<CloudEvent>> listener, DcbStreamIdGenerator dcbStreamIdGenerator, boolean streamPositionEnabled, DataFieldReader dataFieldReader) {
        this.listener = requireNonNull(listener, "listener cannot be null");
        this.dcbStreamIdGenerator = requireNonNull(dcbStreamIdGenerator, DcbStreamIdGenerator.class.getSimpleName() + " cannot be null");
        this.streamPositionEnabled = streamPositionEnabled;
        this.dataFieldReader = requireNonNull(dataFieldReader, DataFieldReader.class.getSimpleName() + " cannot be null");
    }

    /**
     * Returns a copy of this store where stream-written events carry no global position. Only meaningful for a
     * STREAM-only store, since DCB events always carry a position.
     */
    public InMemoryEventStore withoutStreamPosition() {
        return new InMemoryEventStore(listener, dcbStreamIdGenerator, false, dataFieldReader);
    }

    /**
     * Returns a copy of this store where stream-written events get the same global position DCB uses, so both share
     * one sequence.
     */
    public InMemoryEventStore withStreamPosition() {
        return new InMemoryEventStore(listener, dcbStreamIdGenerator, true, dataFieldReader);
    }

    /**
     * Returns a copy of this store that can answer {@link org.occurrent.filter.Filter#data(String, org.occurrent.condition.Condition)}
     * by reading the supplied field reader. Without one, a data filter is refused rather than silently matching nothing.
     * <p>
     * Occurrent ships a Jackson-backed reader in {@code occurrent-common-inmemory-filter-matching-jackson}. As with
     * the other withers, this returns a new empty store, so call it before writing anything.
     */
    public InMemoryEventStore withDataFieldReader(DataFieldReader dataFieldReader) {
        return new InMemoryEventStore(listener, dcbStreamIdGenerator, streamPositionEnabled, dataFieldReader);
    }

    /**
     * Returns whether stream-written events carry a global position, so position-requiring APIs are safe to call. DCB
     * always writes a position regardless of this flag.
     */
    @Override
    public boolean writesPosition() {
        return streamPositionEnabled;
    }

    private void requirePosition() {
        if (!writesPosition()) {
            throw new UnsupportedOperationException("This event store does not write a position. Enable DCB, or do not call " + "withoutStreamPosition() on a STREAM-only store, to use position-requiring APIs.");
        }
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        return read(streamId, null, skip, limit);
    }

    @Override
    public WriteResult write(String streamId, WriteCondition writeCondition, List<CloudEvent> events) {
        requireTrue(writeCondition != null, WriteCondition.class.getSimpleName() + " cannot be null");
        rejectDcbTaggedEvents(events);
        Stream<CloudEvent> cloudEventStream = events.stream().peek(e -> requireTrue(e.getSpecVersion() == SpecVersion.V1, "Spec version needs to be " + SpecVersion.V1));
        // Minted once for the whole call, and only when there is something to stamp it on, so a call that persists
        // no events reports no append id (ADR 132, decision 4).
        final Optional<AppendId> appendId = events.isEmpty() ? Optional.empty() : Optional.of(AppendId.mint());

        final AtomicReference<@Nullable List<CloudEvent>> newCloudEvents = new AtomicReference<>();
        final AtomicLong currentStreamVersionContainer = new AtomicLong();
        synchronized (state) {
            state.compute(streamId, (__, currentEvents) -> {
                long currentStreamVersion = calculateStreamVersion(currentEvents);
                currentStreamVersionContainer.set(currentStreamVersion);

                if (currentEvents == null && isConditionFulfilledBy(writeCondition, 0)) {
                    List<CloudEvent> cloudEvents = applyStreamWriteExtensions(cloudEventStream, streamId, 0, appendId);
                    newCloudEvents.set(cloudEvents);
                    validateNoDuplicateEventExists(cloudEvents);
                    assignInsertionOrder(cloudEvents);
                    return new CopyOnWriteArrayList<>(cloudEvents);
                } else if (currentEvents != null && isConditionFulfilledBy(writeCondition, currentStreamVersion)) {
                    List<CloudEvent> eventList = new ArrayList<>(currentEvents);
                    List<CloudEvent> newEvents = applyStreamWriteExtensions(cloudEventStream, streamId, currentStreamVersion, appendId);
                    eventList.addAll(newEvents);
                    validateNoDuplicateEventExists(eventList);
                    newCloudEvents.set(newEvents);
                    assignInsertionOrder(newEvents);
                    return new CopyOnWriteArrayList<>(eventList);
                } else {
                    throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition);
                }
            });
        }

        final WriteResult writeResult;
        List<CloudEvent> addedEvents = newCloudEvents.get();
        final long oldStreamVersion = currentStreamVersionContainer.get();
        if (addedEvents != null && !addedEvents.isEmpty()) {
            listener.accept(addedEvents);
            CloudEvent cloudEvent = addedEvents.getLast();
            long newStreamVersion = OccurrentExtensionGetter.getStreamVersion(cloudEvent);
            writeResult = new WriteResult(streamId, oldStreamVersion, newStreamVersion, appendId);
        } else {
            writeResult = new WriteResult(streamId, oldStreamVersion, oldStreamVersion, Optional.empty());
        }

        return writeResult;
    }

    private static void validateNoDuplicateEventExists(List<CloudEvent> events) {
        Map<String, List<CloudEvent>> eventsById = events.stream().collect(groupingBy(c -> c.getId() + c.getSource().toString()));
        eventsById.forEach((key, cloudEvents) -> {
            if (cloudEvents.size() > 1) {
                CloudEvent cloudEvent = cloudEvents.getFirst();
                throw new DuplicateCloudEventException(cloudEvent.getId(), cloudEvent.getSource());
            }
        });
    }

    // Call from inside the "state.compute" critical section so positions come from the same nextPosition
    // counter, under the same lock, that DCB uses, keeping stream and DCB writes on one shared sequence.
    private List<CloudEvent> applyStreamWriteExtensions(Stream<CloudEvent> events, String streamId, long streamVersion, Optional<AppendId> appendId) {
        List<CloudEvent> withStreamMetadata = zip(LongStream.iterate(streamVersion + 1, i -> i + 1).boxed(), events, Pair::new)
                .map(pair -> modifyCloudEvent(e -> e.withExtension(new OccurrentCloudEventExtension(streamId, pair.t1))).apply(pair.t2))
                .map(event -> stampAppendId(event, appendId))
                .collect(Collectors.toList());
        if (!streamPositionEnabled || withStreamMetadata.isEmpty()) {
            return withStreamMetadata;
        }
        List<CloudEvent> withPosition = new ArrayList<>(withStreamMetadata.size());
        for (CloudEvent event : withStreamMetadata) {
            withPosition.add(OccurrentCloudEventExtension.withPosition(event, nextPosition.getAndIncrement()));
        }
        return withPosition;
    }

    private static CloudEvent stampAppendId(CloudEvent event, Optional<AppendId> appendId) {
        return appendId.isEmpty() ? event : OccurrentCloudEventExtension.withAppendId(event, appendId.get().toString());
    }

    // Must run inside the "state.compute" critical section so sequence numbers reflect the serialized
    // insertion order across all streams.
    private void assignInsertionOrder(List<CloudEvent> events) {
        for (CloudEvent event : events) {
            insertionOrderByEventKey.put(insertionKey(event), insertionSequence.getAndIncrement());
        }
    }

    private static String insertionKey(CloudEvent cloudEvent) {
        return insertionKey(cloudEvent.getId(), cloudEvent.getSource());
    }

    private static String insertionKey(String cloudEventId, URI cloudEventSource) {
        return cloudEventId + cloudEventSource;
    }

    private static List<CloudEvent> applyPositionAndOccurrentMetadata(Stream<CloudEvent> events, String streamId, long streamVersion, long startPosition, AppendId appendId) {
        AtomicLong streamVersionCounter = new AtomicLong(streamVersion + 1);
        AtomicLong positionCounter = new AtomicLong(startPosition);
        return events
                .map(event -> OccurrentCloudEventExtension.withPosition(event, positionCounter.getAndIncrement()))
                .map(event -> modifyCloudEvent(e -> e.withExtension(new OccurrentCloudEventExtension(streamId, streamVersionCounter.getAndIncrement()))).apply(event))
                .map(event -> OccurrentCloudEventExtension.withAppendId(event, appendId.toString()))
                .collect(Collectors.toList());
    }

    @Override
    public WriteResult write(String streamId, List<CloudEvent> events) {
        return write(streamId, WriteCondition.anyStreamVersion(), events);
    }

    @Override
    public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");

        synchronized (state) {
            long highWatermark = nextPosition.get() - 1;
            long afterPosition = options.afterPosition().orElse(0);
            long upperBound = Math.min(highWatermark, options.upToPosition().orElse(highWatermark));
            List<CloudEvent> matchingEvents = allEvents()
                    .filter(event -> position(event) > afterPosition)
                    .filter(event -> position(event) <= upperBound)
                    .filter(event -> DcbCloudEvents.isDcbEvent(event) && DcbCloudEvents.matches(event, criteria))
                    .sorted(Comparator.comparingLong(InMemoryEventStore::position))
                    .toList();
            // highWatermark (the store head) is the consistency boundary and is deliberately independent of the
            // direction, skip, and limit selection, so a partial read still protects an append from later matches.
            return new DcbEventStream(applySelection(matchingEvents, options), highWatermark);
        }
    }

    private static List<CloudEvent> applySelection(List<CloudEvent> ascendingMatches, DcbReadOptions options) {
        int available = Math.max(0, ascendingMatches.size() - options.skip());
        int selected = Math.min(options.limit().orElse(available), available);
        if (selected == 0) {
            return List.of();
        }
        int fromIndex = options.direction() == DcbReadOptions.Direction.FORWARD
                ? options.skip()
                : ascendingMatches.size() - options.skip() - selected;
        return ascendingMatches.subList(fromIndex, fromIndex + selected);
    }

    @Override
    public boolean exists(DcbCriteria criteria) {
        return exists(criteria, DcbReadOptions.fromBeginning());
    }

    @Override
    public boolean exists(DcbCriteria criteria, DcbReadOptions options) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        synchronized (state) {
            return matchingDcbEvents(criteria, options.positionRange()).findAny().isPresent();
        }
    }

    @Override
    public long count(DcbCriteria criteria) {
        return count(criteria, DcbReadOptions.fromBeginning());
    }

    @Override
    public long count(DcbCriteria criteria, DcbReadOptions options) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        synchronized (state) {
            return matchingDcbEvents(criteria, options.positionRange()).count();
        }
    }

    private Stream<CloudEvent> matchingDcbEvents(DcbCriteria criteria, PositionRange positionRange) {
        long highWatermark = nextPosition.get() - 1;
        long afterPosition = positionRange.afterPosition().orElse(0);
        long upperBound = Math.min(highWatermark, positionRange.upToPosition().orElse(highWatermark));
        return allEvents()
                .filter(event -> position(event) > afterPosition && position(event) <= upperBound)
                .filter(event -> DcbCloudEvents.isDcbEvent(event) && DcbCloudEvents.matches(event, criteria));
    }

    @Override
    public DcbAppendResult append(List<CloudEvent> events) {
        return appendDcb(events, null);
    }

    @Override
    public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
        requireNonNull(condition, "Append condition cannot be null");
        return appendDcb(events, condition);
    }

    private DcbAppendResult appendDcb(List<CloudEvent> events, @Nullable DcbAppendCondition condition) {
        List<CloudEvent> eventsToAppend = validateDcbEvents(events);
        // Place by the condition's boundary tags when it constrains tags, so the same boundary always lands
        // in the same partition regardless of per-event tags. Otherwise fall back to the events' tags, so
        // tagless boundaries do not all collapse onto one hot partition.
        Set<Tag> conditionTags = condition == null ? Set.of() : DcbCloudEvents.tagsOf(condition.criteria());
        Set<Tag> placementTags = conditionTags.isEmpty() ? tagsOf(eventsToAppend) : conditionTags;
        String streamId = requireNonNull(dcbStreamIdGenerator.generateStreamId(placementTags), "DcbStreamIdGenerator returned a null stream id");

        // A DCB append always persists at least one event (validateDcbEvents refuses an empty list above), so
        // this is minted unconditionally, unlike the stream write path.
        AppendId appendId = AppendId.mint();
        List<CloudEvent> addedEvents;
        DcbAppendResult result;
        synchronized (state) {
            if (condition != null) {
                // Positions are assigned and committed atomically under the lock, so the read head is a sound
                // concurrency boundary. The token value is simply the position observed at read time.
                long afterPosition = condition.consistencyToken().map(DcbConsistencyToken::value).orElse(0L);
                boolean fulfilled = allEvents()
                        .filter(event -> position(event) > afterPosition)
                        .noneMatch(event -> DcbCloudEvents.isDcbEvent(event) && DcbCloudEvents.matches(event, condition.criteria()));
                long currentPosition = nextPosition.get() - 1;
                if (!fulfilled) {
                    throw new DcbAppendConditionNotFulfilledException(condition, currentPosition);
                }
            }

            CopyOnWriteArrayList<CloudEvent> currentEvents = state.get(streamId);
            long currentStreamVersion = calculateStreamVersion(currentEvents);
            addedEvents = applyPositionAndOccurrentMetadata(eventsToAppend.stream(), streamId, currentStreamVersion, nextPosition.get(), appendId);

            List<CloudEvent> eventList = currentEvents == null ? new ArrayList<>() : new ArrayList<>(currentEvents);
            eventList.addAll(addedEvents);
            List<CloudEvent> allEvents = allEvents().collect(Collectors.toCollection(ArrayList::new));
            allEvents.addAll(addedEvents);
            validateNoDuplicateEventExists(allEvents);
            assignInsertionOrder(addedEvents);
            state.put(streamId, new CopyOnWriteArrayList<>(eventList));
            nextPosition.addAndGet(addedEvents.size());
            long firstPosition = position(addedEvents.getFirst());
            long lastPosition = position(addedEvents.getLast());
            result = new DcbAppendResult(firstPosition, lastPosition, addedEvents.size(), Optional.of(appendId));
        }

        listener.accept(addedEvents);
        return result;
    }

    @Override
    public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(range, "Range cannot be null");
        requirePosition();

        synchronized (state) {
            long highWatermark = nextPosition.get() - 1;
            long afterPosition = range.afterPosition().orElse(0);
            long upToPosition = Math.min(highWatermark, range.upToPosition().orElse(highWatermark));
            return allEvents()
                    .filter(event -> position(event) > afterPosition)
                    .filter(event -> position(event) <= upToPosition)
                    .filter(event -> matchesFilter(event, filter, dataFieldReader))
                    .sorted(Comparator.comparingLong(InMemoryEventStore::position))
                    .toList()
                    .stream();
        }
    }

    @Override
    public long currentPosition() {
        requirePosition();
        synchronized (state) {
            return nextPosition.get() - 1;
        }
    }

    private Stream<CloudEvent> allEvents() {
        return state.values().stream().flatMap(List::stream);
    }

    private static Set<Tag> tagsOf(List<CloudEvent> events) {
        return events.stream().flatMap(event -> DcbCloudEvents.getTags(event).stream()).collect(Collectors.toCollection(TreeSet::new));
    }

    private static List<CloudEvent> validateDcbEvents(List<CloudEvent> events) {
        requireNonNull(events, "Events cannot be null");
        List<CloudEvent> copy = List.copyOf(events);
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("Events cannot be empty");
        }
        return copy.stream()
                .peek(event -> requireTrue(event.getSpecVersion() == SpecVersion.V1, "Spec version needs to be " + SpecVersion.V1))
                .map(event -> DcbCloudEvents.withTags(event, DcbCloudEvents.getTags(event)))
                .toList();
    }

    private static long position(CloudEvent event) {
        return OccurrentCloudEventExtension.getPosition(event);
    }

    @Override
    public boolean exists(String streamId) {
        CopyOnWriteArrayList<CloudEvent> events = state.get(streamId);
        return events != null && !events.isEmpty();
    }

    private static boolean isConditionFulfilledBy(WriteCondition writeCondition, long version) {
        if (writeCondition.isAnyStreamVersion()) {
            return true;
        }

        if (!(writeCondition instanceof StreamVersionWriteCondition c)) {
            return false;
        }

        return LongConditionEvaluator.evaluate(c.condition(), version);
    }

    @Override
    public void deleteEventStream(String streamId) {
        requireNonNull(streamId, "StreamId cannot be null");
        CopyOnWriteArrayList<CloudEvent> removed = state.remove(streamId);
        if (removed != null) {
            removed.forEach(event -> insertionOrderByEventKey.remove(insertionKey(event)));
        }
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        Predicate<CloudEvent> cloudEventMatchesInput = uniqueCloudEvent(cloudEventId, cloudEventSource);
        String streamId = findStreamIdByCloudEvent(cloudEventMatchesInput).orElse(null);

        if (streamId == null) {
            return;
        }

        state.computeIfPresent(streamId, (__, events) -> {
            CopyOnWriteArrayList<CloudEvent> newEvents = events.stream().filter(cloudEventMatchesInput.negate()).collect(Collectors.toCollection(CopyOnWriteArrayList::new));
            if (newEvents.isEmpty()) {
                return null;
            }
            return newEvents;
        });
        insertionOrderByEventKey.remove(insertionKey(cloudEventId, cloudEventSource));
    }

    public void deleteAll() {
        synchronized (state) {
            state.clear();
            insertionOrderByEventKey.clear();
            insertionSequence.set(0);
            nextPosition.set(1);
        }
    }

    @Override
    public void delete(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        // Held for the whole delete, not just the key snapshot: "state" is a synchronized map, so iterating any of its
        // views needs the monitor, and holding it also keeps the delete atomic against a concurrent write the way the
        // single replaceAll call this replaced was.
        synchronized (state) {
            new ArrayList<>(state.keySet()).forEach(streamId -> state.computeIfPresent(streamId, (__, cloudEvents) -> {
                Map<Boolean, List<CloudEvent>> partitioned = cloudEvents.stream()
                        .collect(Collectors.partitioningBy(cloudEvent -> matchesFilter(cloudEvent, filter, dataFieldReader)));
                partitioned.get(true).forEach(removed -> insertionOrderByEventKey.remove(insertionKey(removed)));
                CopyOnWriteArrayList<CloudEvent> remaining = new CopyOnWriteArrayList<>(partitioned.get(false));
                return remaining.isEmpty() ? null : remaining;
            }));
        }
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        requireNonNull(updateFunction, "Update function cannot be null");

        Predicate<CloudEvent> cloudEventPredicate = uniqueCloudEvent(cloudEventId, cloudEventSource);
        AtomicReference<CloudEvent> result = new AtomicReference<>();
        findStreamIdByCloudEvent(cloudEventPredicate)
                .ifPresent(streamId -> state.computeIfPresent(streamId, (__, events) -> {
                    Optional<CloudEvent> currentCloudEvent = events.stream().filter(cloudEventPredicate).findFirst();
                    if (currentCloudEvent.isEmpty()) {
                        return events;
                    }

                    CloudEvent updatedCloudEvent = updateFunction.apply(currentCloudEvent.get());
                    //noinspection ConstantValue
                    if (updatedCloudEvent == null) {
                        throw UpdateEventFunctionValidator.updateFunctionReturnedNull();
                    }
                    updatedCloudEvent = OccurrentCloudEventExtension.preserveStreamIdentity(currentCloudEvent.get(), updatedCloudEvent);
                    updatedCloudEvent = OccurrentCloudEventExtension.preserveAppendId(currentCloudEvent.get(), updatedCloudEvent);
                    updatedCloudEvent = OccurrentCloudEventExtension.preservePosition(currentCloudEvent.get(), updatedCloudEvent);
                    updatedCloudEvent = DcbCloudEvents.preserveTags(currentCloudEvent.get(), updatedCloudEvent);
                    if (Objects.equals(updatedCloudEvent, currentCloudEvent.get())) {
                        result.set(currentCloudEvent.get());
                        return events;
                    }

                    result.set(updatedCloudEvent);
                    CloudEvent finalUpdatedCloudEvent = updatedCloudEvent;
                    return events.stream()
                            .map(cloudEvent -> cloudEventPredicate.test(cloudEvent) ? finalUpdatedCloudEvent : cloudEvent)
                            .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
                }));
        return Optional.ofNullable(result.get());
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        Objects.requireNonNull(filter, Filter.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(sortBy, SortBy.class.getSimpleName() + " cannot be null");

        // Snapshot the per-stream lists under the lock, then filter and sort outside it. The returned stream is
        // consumed lazily, so iterating state.values() outside the lock could race with a concurrent write()
        // and throw ConcurrentModificationException. Each value is a CopyOnWriteArrayList that write() replaces
        // atomically, so iterating the snapshotted reference stays safe.
        final List<CopyOnWriteArrayList<CloudEvent>> snapshot;
        synchronized (state) {
            snapshot = new ArrayList<>(state.values());
        }
        Stream<CloudEvent> stream = snapshot.stream().flatMap(List::stream).filter(cloudEvent -> matchesFilter(cloudEvent, filter, dataFieldReader));

        if (sortBy instanceof SortBy.Unsorted) {
            // Use natural ascending by default
            sortBy = SortBy.natural(ASCENDING);
        }

        Comparator<CloudEvent> comparator = toComparator(sortBy);
        final Stream<CloudEvent> streamToUse = comparator == null ? stream : stream.sorted(comparator);
        return streamToUse.skip(skip).limit(limit);
    }

    @Override
    public long count(Filter filter) {
        synchronized (state) {
            return state.values().stream().mapToLong(cloudEvents -> cloudEvents.stream().filter(cloudEvent -> matchesFilter(cloudEvent, filter, dataFieldReader)).count()).reduce(0, Long::sum);
        }
    }

    @Override
    public boolean exists(Filter filter) {
        return count(filter) > 0;
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, @Nullable StreamReadFilter filter, int skip, int limit) {
        if (skip < 0) {
            throw new IllegalArgumentException("skip cannot be negative");
        }
        List<CloudEvent> events = state.get(streamId);
        if (events == null) {
            return new EventStreamImpl(streamId, 0, Collections.emptyList());
        }

        var streamVersion = calculateStreamVersion(events);

        // "skip" is matched against each event's own STREAM_VERSION rather than its list index, so it stays
        // correct after deleteEvent or delete(Filter) removes an earlier event and the survivors shift down in
        // the list. Applied before the filter runs, not to the filtered result.
        List<CloudEvent> eventsAfterSkip = skip == 0 ? events : events.stream().filter(e -> OccurrentExtensionGetter.getStreamVersion(e) > skip).toList();

        List<CloudEvent> eventsAfterFilter;
        if (filter == null) {
            eventsAfterFilter = eventsAfterSkip;
        } else {
            StreamReadFilterValidator.validate(filter);
            Filter readFilter = StreamReadFilterToFilterMapper.map(filter);
            eventsAfterFilter = eventsAfterSkip.stream().filter(cloudEvent -> matchesFilter(cloudEvent, readFilter, dataFieldReader)).toList();
        }

        List<CloudEvent> result = limit == Integer.MAX_VALUE
                ? eventsAfterFilter
                : eventsAfterFilter.subList(0, (int) Math.min((long) limit, eventsAfterFilter.size()));

        return new EventStreamImpl(streamId, streamVersion, result);
    }

    private static class EventStreamImpl implements EventStream<CloudEvent> {
        private final String streamId;
        private final long version;
        private final List<CloudEvent> events;

        public EventStreamImpl(String streamId, long version, List<CloudEvent> events) {
            this.streamId = streamId;
            this.version = version;
            this.events = Collections.unmodifiableList(events);
        }

        @Override
        public String id() {
            return streamId;
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public Stream<CloudEvent> events() {
            return events.stream();
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (!(o instanceof EventStreamImpl that)) return false;
            return version == that.version &&
                    Objects.equals(streamId, that.streamId) &&
                    Objects.equals(events, that.events);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, version, events);
        }

        @Override
        public String toString() {
            return "EventStreamImpl{" +
                    "streamId='" + streamId + '\'' +
                    ", version=" + version +
                    ", events=" + events +
                    '}';
        }
    }

    /**
     * Rejects DCB-tagged events on the stream write path. A dcbtags-carrying event written through write(...)
     * would bypass the DCB append path and stay invisible to DCB reads, so this keeps dcbtags a reliable
     * DCB discriminator.
     */
    private static void rejectDcbTaggedEvents(List<CloudEvent> events) {
        if (events.stream().anyMatch(DcbCloudEvents::isDcbEvent)) {
            throw new IllegalArgumentException("A DCB-tagged event cannot be written through the stream write(...) API, use the DCB append(...) API instead.");
        }
    }

    private static void requireTrue(boolean bool, String message) {
        if (!bool) {
            throw new IllegalArgumentException(message);
        }
    }

    private static Function<CloudEvent, CloudEvent> modifyCloudEvent(Function<CloudEventBuilder, CloudEventBuilder> fn) {
        return (cloudEvent) -> fn.apply(CloudEventBuilder.v1(cloudEvent)).build();
    }

    private static Predicate<CloudEvent> uniqueCloudEvent(String cloudEventId, URI cloudEventSource) {
        requireNonNull(cloudEventId, "CloudEvent id cannot be null");
        requireNonNull(cloudEventSource, "CloudEvent source cannot be null");
        return e -> e.getId().equals(cloudEventId) && e.getSource().equals(cloudEventSource);
    }

    private Optional<String> findStreamIdByCloudEvent(Predicate<CloudEvent> predicate) {
        return state.entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(predicate))
                .map(Entry::getKey)
                .findFirst();
    }

    @SuppressWarnings("ConstantConditions")
    private static long calculateStreamVersion(@Nullable List<CloudEvent> events) {
        if (events == null || events.isEmpty()) {
            return 0;
        }
        return (long) events.getLast().getExtension(STREAM_VERSION);
    }

    @Nullable
    private Comparator<CloudEvent> toComparator(SortBy sortBy) {
        final Comparator<CloudEvent> comparator;
        if (sortBy instanceof NaturalImpl) {
            // "Natural" order is global insertion order (see insertionOrderByEventKey), matching MongoDB's
            // $natural. Monotonic with insertion regardless of the events' "time", both standalone and as a
            // tie-breaker step.
            Comparator<CloudEvent> byInsertionOrder = comparing((CloudEvent cloudEvent) -> insertionOrderByEventKey.getOrDefault(insertionKey(cloudEvent), Long.MAX_VALUE));
            comparator = ((NaturalImpl) sortBy).direction == DESCENDING ? byInsertionOrder.reversed() : byInsertionOrder;
        } else if (sortBy instanceof SingleFieldImpl) {
            comparator = singleFieldComparator((SingleFieldImpl) sortBy);
        } else if (sortBy instanceof MultipleSortStepsImpl) {
            comparator = ((MultipleSortStepsImpl) sortBy).steps.stream()
                    .map(this::toComparator)
                    .filter(Objects::nonNull)
                    .reduce(Comparator::thenComparing)
                    .orElse(null);
        } else {
            throw new IllegalStateException("Internal error: Unrecognized \"sort by\" " + sortBy);
        }
        return comparator;
    }

    private static Comparator<CloudEvent> singleFieldComparator(SingleFieldImpl singleField) {
        String fieldName = singleField.fieldName;
        final Comparator<CloudEvent> comparator = switch (fieldName) {
            case TIME -> comparing(CloudEvent::getTime, nullsFirst(OffsetDateTime::compareTo));
            case STREAM_VERSION -> comparing(OccurrentExtensionGetter::getStreamVersion);
            case STREAM_ID -> comparing(OccurrentExtensionGetter::getStreamId);
            case ID -> comparing(CloudEvent::getId);
            case SOURCE -> comparing(CloudEvent::getSource);
            case SUBJECT -> comparing(CloudEvent::getSubject, nullsFirst(String::compareTo));
            case TYPE -> comparing(CloudEvent::getType);
            case SPECVERSION -> comparing(CloudEvent::getSpecVersion);
            case DATACONTENTTYPE -> comparing(CloudEvent::getDataContentType, nullsFirst(String::compareTo));
            case DATASCHEMA -> comparing(CloudEvent::getDataSchema, nullsFirst(URI::compareTo));
            default -> throw new IllegalStateException("Unexpected value: " + fieldName);
        };

        if (singleField.direction == ASCENDING) {
            return comparator;
        } else {
            return comparator.reversed();
        }
    }
}
