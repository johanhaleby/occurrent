package se.haleby.occurrent.eventstore.api.blocking;

public interface EventStore extends ReadEventStream, ConditionallyWriteToEventStream, UnconditionallyWriteToEventStream, EventStreamExists {
}
