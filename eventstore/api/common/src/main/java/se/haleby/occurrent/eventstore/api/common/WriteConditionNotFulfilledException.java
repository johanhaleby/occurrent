package se.haleby.occurrent.eventstore.api.common;

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
