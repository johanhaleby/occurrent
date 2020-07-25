package se.haleby.occurrent.eventstore.inmemory;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension;
import se.haleby.occurrent.eventstore.api.Condition;
import se.haleby.occurrent.eventstore.api.Condition.MultiOperandCondition;
import se.haleby.occurrent.eventstore.api.Condition.MultiOperandConditionName;
import se.haleby.occurrent.eventstore.api.Condition.SingleOperandCondition;
import se.haleby.occurrent.eventstore.api.Condition.SingleOperandConditionName;
import se.haleby.occurrent.eventstore.api.WriteCondition;
import se.haleby.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import se.haleby.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStoreOperations;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.isEqual;

public class InMemoryEventStore implements EventStore, EventStoreOperations {

    private final ConcurrentMap<String, VersionAndEvents> state = new ConcurrentHashMap<>();

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        VersionAndEvents versionAndEvents = state.get(streamId);
        if (versionAndEvents == null) {
            return new EventStreamImpl(streamId, new VersionAndEvents(0, Collections.emptyList()));
        } else if (skip == 0 && limit == Integer.MAX_VALUE) {
            return new EventStreamImpl(streamId, versionAndEvents);
        }
        return new EventStreamImpl(streamId, versionAndEvents.flatMap(v -> new VersionAndEvents(v.version, v.events.subList(skip, limit))));
    }

    @Override
    public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        requireTrue(writeCondition != null, WriteCondition.class.getSimpleName() + " cannot be null");
        Stream<CloudEvent> cloudEventStream = events
                .peek(e -> requireTrue(e.getSpecVersion() == SpecVersion.V1, "Spec version needs to be " + SpecVersion.V1))
                .map(modifyCloudEvent(e -> e.withExtension(new OccurrentCloudEventExtension(streamId))));

        state.compute(streamId, (__, currentVersionAndEvents) -> {
            if (currentVersionAndEvents == null && isConditionFulfilledBy(writeCondition, 0)) {
                return new VersionAndEvents(1, cloudEventStream.collect(Collectors.toList()));
            } else if (currentVersionAndEvents != null && isConditionFulfilledBy(writeCondition, currentVersionAndEvents.version)) {
                List<CloudEvent> newEvents = new ArrayList<>(currentVersionAndEvents.events);
                newEvents.addAll(cloudEventStream.collect(Collectors.toList()));
                return new VersionAndEvents(currentVersionAndEvents.version + 1, newEvents);
            } else {
                long eventStreamVersion = currentVersionAndEvents == null ? 0 : currentVersionAndEvents.version;
                throw new WriteConditionNotFulfilledException(streamId, eventStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), eventStreamVersion));
            }
        });
    }


    @Override
    public void write(String streamId, Stream<CloudEvent> events) {
        write(streamId, WriteCondition.anyStreamVersion(), events);
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
        return isConditionFulfilledBy(c.condition, version);
    }


    private static boolean isConditionFulfilledBy(Condition<Long> condition, long actualVersion) {
        if (condition instanceof Condition.MultiOperandCondition) {
            MultiOperandCondition<Long> operation = (MultiOperandCondition<Long>) condition;
            MultiOperandConditionName operationName = operation.operationName;
            List<Condition<Long>> operations = operation.operations;
            Stream<Boolean> stream = operations.stream().map(c -> isConditionFulfilledBy(c, actualVersion));
            switch (operationName) {
                case AND:
                    return stream.allMatch(isEqual(true));
                case OR:
                    return stream.anyMatch(isEqual(true));
                case NOT:
                    return stream.allMatch(isEqual(false));
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else if (condition instanceof Condition.SingleOperandCondition) {
            SingleOperandCondition<Long> singleOperandCondition = (SingleOperandCondition<Long>) condition;
            long expectedVersion = singleOperandCondition.operand;
            SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
            switch (singleOperandConditionName) {
                case EQ:
                    return actualVersion == expectedVersion;
                case LT:
                    return actualVersion < expectedVersion;
                case GT:
                    return actualVersion > expectedVersion;
                case LTE:
                    return actualVersion <= expectedVersion;
                case GTE:
                    return actualVersion >= expectedVersion;
                case NE:
                    return actualVersion != expectedVersion;
                default:
                    throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }

    @Override
    public void deleteEventStream(String streamId) {
        state.remove(streamId);
    }

    @Override
    public void deleteAllEventsInEventStream(String streamId) {
        state.computeIfPresent(streamId, (__, versionAndEvents) -> new VersionAndEvents(versionAndEvents.version, Collections.emptyList()));
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        Predicate<CloudEvent> cloudEventMatchesInput = e -> e.getId().equals(cloudEventId) && e.getSource().equals(cloudEventSource);
        String streamId = state.entrySet().stream()
                .filter(entry -> entry.getValue().events.stream().anyMatch(cloudEventMatchesInput))
                .map(Entry::getKey)
                .findFirst()
                .orElse(null);

        if (streamId == null) {
            return;
        }

        state.computeIfPresent(streamId, (__, versionAndEvents) -> {
            List<CloudEvent> cloudEvents = versionAndEvents.events.stream().filter(cloudEventMatchesInput.negate()).collect(Collectors.toList());
            return new VersionAndEvents(versionAndEvents.version, cloudEvents);
        });
    }

    private static class VersionAndEvents {
        long version;
        List<CloudEvent> events;


        VersionAndEvents(long version, List<CloudEvent> events) {
            this.version = version;
            this.events = events;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof VersionAndEvents)) return false;
            VersionAndEvents that = (VersionAndEvents) o;
            return version == that.version &&
                    Objects.equals(events, that.events);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, events);
        }

        VersionAndEvents flatMap(Function<VersionAndEvents, VersionAndEvents> fn) {
            return fn.apply(this);
        }
    }

    private static class EventStreamImpl implements EventStream<CloudEvent> {
        private final String streamId;
        private final VersionAndEvents versionAndEvents;

        public EventStreamImpl(String streamId, VersionAndEvents versionAndEvents) {
            this.streamId = streamId;
            this.versionAndEvents = versionAndEvents;
        }

        @Override
        public String id() {
            return streamId;
        }

        @Override
        public long version() {
            return versionAndEvents.version;
        }

        @Override
        public Stream<CloudEvent> events() {
            return versionAndEvents.events.stream();
        }

        @Override
        public String toString() {
            return "EventStreamImpl{" +
                    "streamId='" + streamId + '\'' +
                    ", versionAndEvents=" + versionAndEvents +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EventStreamImpl)) return false;
            EventStreamImpl that = (EventStreamImpl) o;
            return Objects.equals(streamId, that.streamId) &&
                    Objects.equals(versionAndEvents, that.versionAndEvents);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, versionAndEvents);
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
}