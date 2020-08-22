package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.TransactionOptions;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.util.Objects;

public class EventStoreConfig {
    public final TransactionOptions transactionOptions;
    public final TimeRepresentation timeRepresentation;

    public EventStoreConfig(TimeRepresentation timeRepresentation) {
        this(timeRepresentation, null);
    }

    public EventStoreConfig(TimeRepresentation timeRepresentation, TransactionOptions transactionOptions) {
        Objects.requireNonNull(timeRepresentation, "Time representation cannot be null");
        if (transactionOptions == null) {
            this.transactionOptions = TransactionOptions.builder().build();
        } else {
            this.transactionOptions = transactionOptions;
        }
        this.timeRepresentation = timeRepresentation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(transactionOptions, that.transactionOptions) &&
                timeRepresentation == that.timeRepresentation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionOptions, timeRepresentation);
    }

    @Override
    public String toString() {
        return "EventStoreConfig{" +
                "transactionOptions=" + transactionOptions +
                ", timeRepresentation=" + timeRepresentation +
                '}';
    }

}