package se.haleby.occurrent.eventstore.mongodb.spring.reactor;

import se.haleby.occurrent.eventstore.mongodb.converter.TimeRepresentation;

import java.util.Objects;

public class EventStoreConfig {
    public final String eventStoreCollectionName;
    public final StreamConsistencyGuarantee streamConsistencyGuarantee;
    public final TimeRepresentation timeRepresentation;

    public EventStoreConfig(String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee, TimeRepresentation timeRepresentation) {
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.streamConsistencyGuarantee = streamConsistencyGuarantee;
        this.timeRepresentation = timeRepresentation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(eventStoreCollectionName, that.eventStoreCollectionName) &&
                Objects.equals(streamConsistencyGuarantee, that.streamConsistencyGuarantee) &&
                timeRepresentation == that.timeRepresentation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventStoreCollectionName, streamConsistencyGuarantee, timeRepresentation);
    }

    @Override
    public String toString() {
        return "EventStoreConfig{" +
                "eventStoreCollectionName='" + eventStoreCollectionName + '\'' +
                ", streamConsistencyGuarantee=" + streamConsistencyGuarantee +
                ", timeRepresentation=" + timeRepresentation +
                '}';
    }
}