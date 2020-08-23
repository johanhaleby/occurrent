package se.haleby.occurrent.eventstore.api;

/**
 * The write condition was not fulfilled so events have not been written to the event store.
 * In a typical scenario, if an application read and writes stream A from two different places at the same time,
 * this is effectively the same as an optimistic locking exception and a retry is appropriate. For more advanced
 * write conditions this may not be the case though.
 */
public class WriteConditionNotFulfilledException extends RuntimeException {
    public final String eventStreamId;
    public final long eventStreamVersion;
    public final WriteCondition writeCondition;

    public WriteConditionNotFulfilledException(String eventStreamId, long eventStreamVersion, WriteCondition writeCondition, String message) {
        super(message);
        this.writeCondition = writeCondition;
        this.eventStreamVersion = eventStreamVersion;
        this.eventStreamId = eventStreamId;
    }
}
