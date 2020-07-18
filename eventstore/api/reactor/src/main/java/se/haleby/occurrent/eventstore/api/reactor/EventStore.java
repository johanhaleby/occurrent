package se.haleby.occurrent.eventstore.api.reactor;

public interface EventStore extends ReadEventStream, ConditionallyWriteToEventStream, UnconditionallyWriteToEventStream, EventStreamExists {
}
