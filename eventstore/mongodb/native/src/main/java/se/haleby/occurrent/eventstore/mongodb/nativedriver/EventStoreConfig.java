package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.TransactionOptions;
import io.cloudevents.CloudEvent;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.util.Objects;

/**
 * Configuration for the synchronous java driver MongoDB EventStore
 */
public class EventStoreConfig {
    public final TransactionOptions transactionOptions;
    public final TimeRepresentation timeRepresentation;

    /**
     * Create an {@link EventStoreConfig} indicating to the event store that it should represent time according to the supplied
     * {@code timeRepresentation}. It'll use default {@link TransactionOptions}.
     *
     * @param timeRepresentation How the time field in the {@link CloudEvent} should be represented.
     * @see #EventStoreConfig(TimeRepresentation, TransactionOptions)
     * @see TimeRepresentation
     */
    public EventStoreConfig(TimeRepresentation timeRepresentation) {
        this(timeRepresentation, null);
    }

    /**
     * Create an {@link EventStoreConfig} indicating to the event store that it should represent time according to the supplied
     * {@code timeRepresentation}. Also configure the default {@link TransactionOptions} that the event store will use
     * when starting transactions.
     *
     * @param timeRepresentation How the time field in the {@link CloudEvent} should be represented.
     * @param transactionOptions The default {@link TransactionOptions} that the event store will use when starting transactions.
     * @see #EventStoreConfig(TimeRepresentation, TransactionOptions)
     * @see TimeRepresentation
     */
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