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
import org.occurrent.filter.Filter;
import org.occurrent.functionalsupport.internal.FunctionalSupport.Pair;

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
import static java.util.function.Predicate.not;
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
public class InMemoryEventStore implements EventStore, EventStoreOperations, EventStoreQueries, ReadEventStreamWithFilter, DcbEventStore {

    // We cannot use ConcurrentMap since it doesn't maintain insertion order
    private final Map<String, CopyOnWriteArrayList<CloudEvent>> state = Collections.synchronizedMap(new LinkedHashMap<>());
    private final AtomicLong nextDcbPosition = new AtomicLong(1);

    // Global, monotonically increasing insertion order assigned to each event at write time, keyed by the
    // event's "id + source" (the same uniqueness key used by validateNoDuplicateEventExists). This is what
    // SortBy.natural relies on so that "natural order" means *global* insertion order (matching MongoDB's
    // $natural), rather than the per-stream grouping that results from iterating "state". Sequences are
    // assigned inside the "state.compute" critical section in write(...), so they reflect the actual
    // serialized insertion order across all streams.
    private final AtomicLong insertionSequence = new AtomicLong();
    private final Map<String, Long> insertionOrderByEventKey = new ConcurrentHashMap<>();

    private final Consumer<Stream<CloudEvent>> listener;

    /**
     * Create an instance of {@link InMemoryEventStore}
     */
    public InMemoryEventStore() {
        // @formatter:off
        this(__ -> {
        });
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
    public InMemoryEventStore(@Nullable Consumer<Stream<CloudEvent>> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener cannot be null");
        }
        this.listener = listener;
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        return read(streamId, null, skip, limit);
    }

    @Override
    public WriteResult write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        requireTrue(writeCondition != null, WriteCondition.class.getSimpleName() + " cannot be null");
        Stream<CloudEvent> cloudEventStream = events.peek(e -> requireTrue(e.getSpecVersion() == SpecVersion.V1, "Spec version needs to be " + SpecVersion.V1));

        final AtomicReference<@Nullable List<CloudEvent>> newCloudEvents = new AtomicReference<>();
        final AtomicLong currentStreamVersionContainer = new AtomicLong();
        state.compute(streamId, (__, currentEvents) -> {
            long currentStreamVersion = calculateStreamVersion(currentEvents);
            currentStreamVersionContainer.set(currentStreamVersion);

            if (currentEvents == null && isConditionFulfilledBy(writeCondition, 0)) {
                List<CloudEvent> cloudEvents = applyOccurrentCloudEventExtension(cloudEventStream, streamId, 0);
                newCloudEvents.set(cloudEvents);
                validateNoDuplicateEventExists(cloudEvents);
                assignInsertionOrder(cloudEvents);
                return new CopyOnWriteArrayList<>(cloudEvents);
            } else if (currentEvents != null && isConditionFulfilledBy(writeCondition, currentStreamVersion)) {
                List<CloudEvent> eventList = new ArrayList<>(currentEvents);
                List<CloudEvent> newEvents = applyOccurrentCloudEventExtension(cloudEventStream, streamId, currentStreamVersion);
                eventList.addAll(newEvents);
                validateNoDuplicateEventExists(eventList);
                newCloudEvents.set(newEvents);
                assignInsertionOrder(newEvents);
                return new CopyOnWriteArrayList<>(eventList);
            } else {
                throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition, currentStreamVersion));
            }
        });

        final WriteResult writeResult;
        List<CloudEvent> addedEvents = newCloudEvents.get();
        final long oldStreamVersion = currentStreamVersionContainer.get();
        if (addedEvents != null && !addedEvents.isEmpty()) {
            listener.accept(addedEvents.stream());
            CloudEvent cloudEvent = addedEvents.get(addedEvents.size() - 1);
            long newStreamVersion = OccurrentExtensionGetter.getStreamVersion(cloudEvent);
            writeResult = new WriteResult(streamId, oldStreamVersion, newStreamVersion);
        } else {
            writeResult = new WriteResult(streamId, oldStreamVersion, oldStreamVersion);
        }

        return writeResult;
    }

    private static void validateNoDuplicateEventExists(List<CloudEvent> events) {
        Map<String, List<CloudEvent>> eventsById = events.stream().collect(groupingBy(c -> c.getId() + c.getSource().toString()));
        eventsById.forEach((key, cloudEvents) -> {
            if (cloudEvents.size() > 1) {
                CloudEvent cloudEvent = cloudEvents.get(0);
                throw new DuplicateCloudEventException(cloudEvent.getId(), cloudEvent.getSource());
            }
        });
    }

    private static List<CloudEvent> applyOccurrentCloudEventExtension(Stream<CloudEvent> events, String streamId, long streamVersion) {
        return zip(LongStream.iterate(streamVersion + 1, i -> i + 1).boxed(), events, Pair::new)
                .map(pair -> modifyCloudEvent(e -> e.withExtension(new OccurrentCloudEventExtension(streamId, pair.t1))).apply(pair.t2))
                .collect(Collectors.toList());
    }

    // Must be called from inside the "state.compute" critical section so that sequence numbers reflect the
    // serialized insertion order across all streams.
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

    private static List<CloudEvent> applyDcbAndOccurrentMetadata(Stream<CloudEvent> events, String streamId, long streamVersion, long dcbPosition) {
        AtomicLong streamVersionCounter = new AtomicLong(streamVersion + 1);
        AtomicLong dcbPositionCounter = new AtomicLong(dcbPosition);
        return events
                .map(event -> DcbCloudEvents.withPosition(event, dcbPositionCounter.getAndIncrement()))
                .map(event -> modifyCloudEvent(e -> e.withExtension(new OccurrentCloudEventExtension(streamId, streamVersionCounter.getAndIncrement()))).apply(event))
                .collect(Collectors.toList());
    }

    @Override
    public WriteResult write(String streamId, Stream<CloudEvent> events) {
        return write(streamId, WriteCondition.anyStreamVersion(), events);
    }

    @Override
    public DcbEventStream read(DcbQuery query, DcbReadOptions options) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");

        synchronized (state) {
            long highWatermark = nextDcbPosition.get() - 1;
            long afterSequencePosition = options.afterSequencePosition().orElse(0);
            List<CloudEvent> matchingEvents = allEvents()
                    .filter(event -> dcbPosition(event) > afterSequencePosition)
                    .filter(event -> dcbPosition(event) <= highWatermark)
                    .filter(event -> matches(event, query))
                    .toList();
            return new DcbEventStream(matchingEvents, highWatermark);
        }
    }

    @Override
    public DcbAppendResult append(String streamId, List<CloudEvent> events) {
        return appendDcb(streamId, events, null);
    }

    @Override
    public DcbAppendResult append(String streamId, List<CloudEvent> events, DcbAppendCondition condition) {
        requireNonNull(condition, "Append condition cannot be null");
        return appendDcb(streamId, events, condition);
    }

    private DcbAppendResult appendDcb(String streamId, List<CloudEvent> events, @Nullable DcbAppendCondition condition) {
        requireNonNull(streamId, "Stream id cannot be null");
        List<CloudEvent> eventsToAppend = validateDcbEvents(events);

        List<CloudEvent> addedEvents;
        DcbAppendResult result;
        synchronized (state) {
            if (condition != null) {
                long afterSequencePosition = condition.afterSequencePosition().orElse(0);
                boolean fulfilled = allEvents()
                        .filter(event -> dcbPosition(event) > afterSequencePosition)
                        .noneMatch(event -> matches(event, condition.failIfEventsMatch()));
                long currentPosition = nextDcbPosition.get() - 1;
                if (!fulfilled) {
                    throw new DcbAppendConditionNotFulfilledException(condition, currentPosition, "Append condition was not fulfilled.");
                }
            }

            CopyOnWriteArrayList<CloudEvent> currentEvents = state.get(streamId);
            long currentStreamVersion = calculateStreamVersion(currentEvents);
            addedEvents = applyDcbAndOccurrentMetadata(eventsToAppend.stream(), streamId, currentStreamVersion, nextDcbPosition.get());

            List<CloudEvent> eventList = currentEvents == null ? new ArrayList<>() : new ArrayList<>(currentEvents);
            eventList.addAll(addedEvents);
            List<CloudEvent> allEvents = allEvents().collect(Collectors.toCollection(ArrayList::new));
            allEvents.addAll(addedEvents);
            validateNoDuplicateEventExists(allEvents);
            state.put(streamId, new CopyOnWriteArrayList<>(eventList));
            nextDcbPosition.addAndGet(addedEvents.size());
            long firstPosition = dcbPosition(addedEvents.get(0));
            long lastPosition = dcbPosition(addedEvents.get(addedEvents.size() - 1));
            result = new DcbAppendResult(firstPosition, lastPosition, addedEvents.size());
        }

        listener.accept(addedEvents.stream());
        return result;
    }

    private Stream<CloudEvent> allEvents() {
        return state.values().stream().flatMap(List::stream);
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

    private static boolean matches(CloudEvent event, DcbQuery query) {
        if (query.matchAll()) {
            return true;
        }
        return query.items().stream().anyMatch(item -> matches(event, item));
    }

    private static boolean matches(CloudEvent event, DcbQueryItem item) {
        boolean typeMatches = item.types().isEmpty() || item.types().contains(event.getType());
        boolean tagsMatch = DcbCloudEvents.getTags(event).containsAll(item.tags());
        return typeMatches && tagsMatch;
    }

    private static long dcbPosition(CloudEvent event) {
        Object position = event.getExtension(DcbCloudEvents.POSITION);
        if (position instanceof Number number) {
            return number.longValue();
        }
        if (position instanceof String string) {
            return Long.parseLong(string);
        }
        return 0;
    }

    @Override
    public boolean exists(String streamId) {
        return state.containsKey(streamId);
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
        state.clear();
        insertionOrderByEventKey.clear();
        insertionSequence.set(0);
    }

    @Override
    public void delete(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        state.replaceAll((streamId, cloudEvents) -> {
            cloudEvents.stream().filter(cloudEvent -> matchesFilter(cloudEvent, filter)).forEach(removed -> insertionOrderByEventKey.remove(insertionKey(removed)));
            return cloudEvents.stream().filter(not(cloudEvent -> matchesFilter(cloudEvent, filter))).collect(Collectors.toCollection(CopyOnWriteArrayList::new));
        });
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        requireNonNull(updateFunction, "Update function cannot be null");

        Predicate<CloudEvent> cloudEventPredicate = uniqueCloudEvent(cloudEventId, cloudEventSource);
        return findStreamIdByCloudEvent(cloudEventPredicate)
                .map(streamId -> state.computeIfPresent(streamId, (__, events) ->
                        events.stream().map(cloudEvent -> {
                            if (cloudEventPredicate.test(cloudEvent)) {
                                CloudEvent updatedCloudEvent = updateFunction.apply(cloudEvent);
                                //noinspection ConstantValue
                                if (updatedCloudEvent == null) {
                                    throw new IllegalArgumentException("It's not allowed to return a null CloudEvent from the update function.");
                                }
                                return updatedCloudEvent;
                            } else {
                                return cloudEvent;
                            }
                        }).collect(Collectors.toCollection(CopyOnWriteArrayList::new))))
                .flatMap(events -> events.stream().filter(cloudEventPredicate).findFirst());
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        Objects.requireNonNull(filter, Filter.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(sortBy, SortBy.class.getSimpleName() + " cannot be null");

        // Snapshot the per-stream event lists under the lock, then filter and sort outside it. The stream returned
        // to the caller is consumed lazily, so iterating state.values() outside the lock could race with a concurrent
        // write() that structurally modifies the backing map and throw ConcurrentModificationException. Each value is
        // a CopyOnWriteArrayList that write() replaces atomically, so iterating a snapshotted reference stays safe.
        final List<CopyOnWriteArrayList<CloudEvent>> snapshot;
        synchronized (state) {
            snapshot = new ArrayList<>(state.values());
        }
        Stream<CloudEvent> stream = snapshot.stream().flatMap(List::stream).filter(cloudEvent -> matchesFilter(cloudEvent, filter));

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
            return state.values().stream().mapToLong(cloudEvents -> cloudEvents.stream().filter(cloudEvent -> matchesFilter(cloudEvent, filter)).count()).reduce(0, Long::sum);
        }
    }

    @Override
    public boolean exists(Filter filter) {
        return count(filter) > 0;
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, @Nullable StreamReadFilter filter, int skip, int limit) {
        List<CloudEvent> events = state.get(streamId);
        record StreamVersionAndEvents(long version, List<CloudEvent> events) {
        }

        final StreamVersionAndEvents streamVersionAndEvents;
        if (events == null) {
            return new EventStreamImpl(streamId, 0, Collections.emptyList());
        }

        var streamVersion = calculateStreamVersion(events);
        List<CloudEvent> eventsAfterFilter;
        if (filter == null) {
            eventsAfterFilter = events;
        } else {
            StreamReadFilterValidator.validate(filter);
            Filter readFilter = StreamReadFilterToFilterMapper.map(filter);
            eventsAfterFilter = events.stream().filter(cloudEvent -> matchesFilter(cloudEvent, readFilter)).toList();
        }

        if (skip == 0 && limit == Integer.MAX_VALUE) {
            streamVersionAndEvents = new StreamVersionAndEvents(streamVersion, eventsAfterFilter);
        } else {
            int fromIndex = Math.min(skip, eventsAfterFilter.size());
            long requestedToIndex = (long) fromIndex + (long) limit;
            int toIndex = (int) Math.min(requestedToIndex, eventsAfterFilter.size());
            streamVersionAndEvents = new StreamVersionAndEvents(streamVersion, eventsAfterFilter.subList(fromIndex, toIndex));
        }

        return new EventStreamImpl(streamId, streamVersionAndEvents.version, streamVersionAndEvents.events);
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
        public boolean equals(Object o) {
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
        return (long) events.get(events.size() - 1).getExtension(STREAM_VERSION);
    }

    @Nullable
    private Comparator<CloudEvent> toComparator(SortBy sortBy) {
        final Comparator<CloudEvent> comparator;
        if (sortBy instanceof NaturalImpl) {
            // "Natural" order is global insertion order (see insertionOrderByEventKey), so it matches MongoDB's
            // $natural and is monotonic with insertion regardless of the events' "time", both standalone and as
            // a tie-breaker step. This is what the CatchupSubscriptionModel relies on to reconcile events written
            // during the catch-up phase.
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
