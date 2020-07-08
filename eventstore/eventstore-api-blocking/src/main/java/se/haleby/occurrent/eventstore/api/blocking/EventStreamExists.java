package se.haleby.occurrent.eventstore.api.blocking;

public interface EventStreamExists {
    boolean exists(String streamId);
}
