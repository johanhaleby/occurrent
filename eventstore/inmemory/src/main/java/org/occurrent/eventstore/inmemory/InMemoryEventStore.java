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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.SortBy.MultipleSortStepsImpl;
import org.occurrent.eventstore.api.SortBy.NaturalImpl;
import org.occurrent.eventstore.api.SortBy.SingleFieldImpl;
import org.occurrent.eventstore.api.SortBy.SortDirection;
import org.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.functionalsupport.internal.FunctionalSupport.Pair;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.cloudevents.core.v1.CloudEventV1.*;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.not;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.zip;
import static org.occurrent.inmemory.filtermatching.FilterMatcher.matchesFilter;

/**
 * This is an {@link EventStore} that stores events in-memory. This is mainly useful for testing
 * and/or demo purposes. It also supports the {@link EventStoreOperations} contract.
 */
public class InMemoryEventStore implements EventStore, EventStoreOperations, EventStoreQueries {

    // We cannot use ConcurrentMap since it doesn't maintain insertion order
    private final Map<String, List<CloudEvent>> state = Collections.synchronizedMap(new LinkedHashMap<>());

    private final Consumer<Stream<CloudEvent>> listener;

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
    public InMemoryEventStore(Consumer<Stream<CloudEvent>> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener cannot be null");
        }
        this.listener = listener;
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        List<CloudEvent> events = state.get(streamId);
        if (events == null) {
            return new EventStreamImpl(streamId, 0, Collections.emptyList());
        } else if (skip == 0 && limit == Integer.MAX_VALUE) {
            return new EventStreamImpl(streamId, calculateStreamVersion(events), events);
        }
        return new EventStreamImpl(streamId, calculateStreamVersion(events), events.subList(skip, limit));
    }

    @Override
    public WriteResult write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        requireTrue(writeCondition != null, WriteCondition.class.getSimpleName() + " cannot be null");
        Stream<CloudEvent> cloudEventStream = events.peek(e -> requireTrue(e.getSpecVersion() == SpecVersion.V1, "Spec version needs to be " + SpecVersion.V1));

        final AtomicReference<List<CloudEvent>> newCloudEvents = new AtomicReference<>();
        final AtomicLong currentStreamVersionContainer = new AtomicLong();
        state.compute(streamId, (__, currentEvents) -> {
            long currentStreamVersion = calculateStreamVersion(currentEvents);
            currentStreamVersionContainer.set(currentStreamVersion);

            if (currentEvents == null && isConditionFulfilledBy(writeCondition, 0)) {
                List<CloudEvent> cloudEvents = applyOccurrentCloudEventExtension(cloudEventStream, streamId, 0);
                newCloudEvents.set(cloudEvents);
                return cloudEvents;
            } else if (currentEvents != null && isConditionFulfilledBy(writeCondition, currentStreamVersion)) {
                List<CloudEvent> eventList = new ArrayList<>(currentEvents);
                List<CloudEvent> newEvents = applyOccurrentCloudEventExtension(cloudEventStream, streamId, currentStreamVersion);
                newCloudEvents.set(newEvents);
                eventList.addAll(newEvents);
                return eventList;
            } else {
                throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), currentStreamVersion));
            }
        });

        final WriteResult writeResult;
        List<CloudEvent> addedEvents = newCloudEvents.get();
        if (addedEvents != null && !addedEvents.isEmpty()) {
            listener.accept(addedEvents.stream());
            CloudEvent cloudEvent = addedEvents.get(addedEvents.size() - 1);
            long streamVersion = OccurrentExtensionGetter.getStreamVersion(cloudEvent);
            writeResult = new WriteResult(streamId, streamVersion);
        } else {
            writeResult = new WriteResult(streamId, currentStreamVersionContainer.get());
        }

        return writeResult;
    }

    private static List<CloudEvent> applyOccurrentCloudEventExtension(Stream<CloudEvent> events, String streamId, long streamVersion) {
        return zip(LongStream.iterate(streamVersion + 1, i -> i + 1).boxed(), events, Pair::new)
                .map(pair -> modifyCloudEvent(e -> e.withExtension(new OccurrentCloudEventExtension(streamId, pair.t1))).apply(pair.t2))
                .collect(Collectors.toList());
    }

    @Override
    public WriteResult write(String streamId, Stream<CloudEvent> events) {
        return write(streamId, WriteCondition.anyStreamVersion(), events);
    }

    @Override
    public boolean exists(String streamId) {
        return state.containsKey(streamId);
    }

    private static boolean isConditionFulfilledBy(WriteCondition writeCondition, long version) {
        if (writeCondition.isAnyStreamVersion()) {
            return true;
        }

        if (!(writeCondition instanceof StreamVersionWriteCondition)) {
            return false;
        }

        StreamVersionWriteCondition c = (StreamVersionWriteCondition) writeCondition;
        return LongConditionEvaluator.evaluate(c.condition, version);
    }

    @Override
    public void deleteEventStream(String streamId) {
        requireNonNull(streamId, "StreamId cannot be null");
        state.remove(streamId);
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        Predicate<CloudEvent> cloudEventMatchesInput = uniqueCloudEvent(cloudEventId, cloudEventSource);
        String streamId = findStreamIdByCloudEvent(cloudEventMatchesInput).orElse(null);

        if (streamId == null) {
            return;
        }

        state.computeIfPresent(streamId, (__, events) -> {
            List<CloudEvent> newEvents = events.stream().filter(cloudEventMatchesInput.negate()).collect(Collectors.toList());
            if (newEvents.isEmpty()) {
                return null;
            }
            return newEvents;
        });
    }

    @Override
    public void delete(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        state.replaceAll((streamId, cloudEvents) -> cloudEvents.stream().filter(not(cloudEvent -> matchesFilter(cloudEvent, filter))).collect(Collectors.toList()));
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
                                if (updatedCloudEvent == null) {
                                    throw new IllegalArgumentException("It's not allowed to return a null CloudEvent from the update function.");
                                }
                                return updatedCloudEvent;
                            } else {
                                return cloudEvent;
                            }
                        }).collect(Collectors.toList())))
                .flatMap(events -> events.stream().filter(cloudEventPredicate).findFirst());
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        Objects.requireNonNull(filter, Filter.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(sortBy, SortBy.class.getSimpleName() + " cannot be null");

        Stream<CloudEvent> stream;
        synchronized (state) {
            stream = state.values().stream().flatMap(List::stream).filter(cloudEvent -> matchesFilter(cloudEvent, filter));
        }

        final Stream<CloudEvent> streamToSort;
        final Map<CloudEvent, Integer> cloudEventPositionCache;
        if (sortBy instanceof NaturalImpl) {
            SortDirection order = ((NaturalImpl) sortBy).direction;
            if (order == ASCENDING) {
                return stream.skip(skip).limit(limit);
            } else {
                Iterator<CloudEvent> cloudEventIterator = stream.collect(Collectors.toCollection(LinkedList::new)).descendingIterator();
                return StreamSupport.stream(spliteratorUnknownSize(cloudEventIterator, Spliterator.ORDERED), false).skip(skip).limit(limit);
            }
        } else if (isMultipleSortStepsContainingNaturalOrder(sortBy)) {
            cloudEventPositionCache = stream.collect(LinkedHashMap::new, (cache, event) -> cache.put(event, cache.size()), LinkedHashMap::putAll);
            streamToSort = cloudEventPositionCache.keySet().stream();
        } else {
            streamToSort = stream;
            cloudEventPositionCache = Collections.emptyMap();
        }

        Comparator<CloudEvent> comparator = toComparator(cloudEventPositionCache, sortBy);
        final Stream<CloudEvent> streamToUse;
        if (comparator == null) {
            streamToUse = streamToSort;
        } else {
            streamToUse = streamToSort.sorted(comparator);
        }

        return streamToUse.skip(skip).limit(limit);
    }

    private static boolean isMultipleSortStepsContainingNaturalOrder(SortBy sortBy) {
        if (sortBy instanceof MultipleSortStepsImpl) {
            return ((MultipleSortStepsImpl) sortBy).steps.stream().anyMatch(NaturalImpl.class::isInstance);
        }
        return false;
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
            if (!(o instanceof EventStreamImpl)) return false;
            EventStreamImpl that = (EventStreamImpl) o;
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
    private static long calculateStreamVersion(List<CloudEvent> events) {
        if (events == null || events.isEmpty()) {
            return 0;
        }
        return (long) events.get(events.size() - 1).getExtension(STREAM_VERSION);
    }

    private static Comparator<CloudEvent> toComparator(Map<CloudEvent, Integer> cloudEventPositionCache, SortBy sortBy) {
        final Comparator<CloudEvent> comparator;
        if (sortBy instanceof NaturalImpl) {
            Comparator<CloudEvent> temp = Comparator.comparingInt(cloudEventPositionCache::get);
            if (((NaturalImpl) sortBy).direction == DESCENDING) {
                comparator = temp.reversed();
            } else {
                comparator = temp;
            }
        } else if (sortBy instanceof SingleFieldImpl) {
            comparator = toComparator((SingleFieldImpl) sortBy);
        } else if (sortBy instanceof MultipleSortStepsImpl) {
            comparator = ((MultipleSortStepsImpl) sortBy).steps.stream()
                    .map(step -> toComparator(cloudEventPositionCache, step))
                    .filter(Objects::nonNull)
                    .reduce(Comparator::thenComparing)
                    .orElse(null);
        } else {
            throw new IllegalStateException("Internal error: Unrecognized \"sort by\" " + sortBy);
        }
        return comparator;
    }

    private static Comparator<CloudEvent> toComparator(SingleFieldImpl singleField) {
        String fieldName = singleField.fieldName;
        final Comparator<CloudEvent> comparator;
        switch (fieldName) {
            case TIME:
                comparator = comparing(CloudEvent::getTime);
                break;
            case STREAM_VERSION:
                comparator = comparing(OccurrentExtensionGetter::getStreamVersion);
                break;
            case STREAM_ID:
                comparator = comparing(OccurrentExtensionGetter::getStreamId);
                break;
            case ID:
                comparator = comparing(CloudEvent::getId);
                break;
            case SOURCE:
                comparator = comparing(CloudEvent::getSource);
                break;
            case SUBJECT:
                comparator = comparing(CloudEvent::getSubject);
                break;
            case TYPE:
                comparator = comparing(CloudEvent::getType);
                break;
            case SPECVERSION:
                comparator = comparing(CloudEvent::getSpecVersion);
                break;
            case DATACONTENTTYPE:
                comparator = comparing(CloudEvent::getDataContentType);
                break;
            case DATASCHEMA:
                comparator = comparing(CloudEvent::getDataSchema);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + fieldName);
        }

        if (singleField.direction == ASCENDING) {
            return comparator;
        } else {
            return comparator.reversed();
        }
    }
}