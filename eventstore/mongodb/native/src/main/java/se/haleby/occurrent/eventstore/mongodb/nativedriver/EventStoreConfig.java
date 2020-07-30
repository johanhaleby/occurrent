package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import java.util.Objects;

public class EventStoreConfig {
    public final StreamConsistencyGuarantee streamConsistencyGuarantee;
    public final TimeRepresentation timeRepresentation;

    public EventStoreConfig(StreamConsistencyGuarantee streamConsistencyGuarantee, TimeRepresentation timeRepresentation) {
        this.streamConsistencyGuarantee = streamConsistencyGuarantee;
        this.timeRepresentation = timeRepresentation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(streamConsistencyGuarantee, that.streamConsistencyGuarantee) &&
                timeRepresentation == that.timeRepresentation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamConsistencyGuarantee, timeRepresentation);
    }

    @Override
    public String toString() {
        return "EventStoreConfig{" +
                "streamConsistencyGuarantee=" + streamConsistencyGuarantee +
                ", timeRepresentation=" + timeRepresentation +
                '}';
    }

}