package se.haleby.occurrent.changestreamer.mongodb;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;

import java.util.Objects;

/**
 * A {@link ChangeStreamPosition} implementation for MongoDB that provides the operation time
 * that consumers may decide which to use when continuing the stream.
 */
public class MongoDBOperationTimeBasedChangeStreamPosition implements ChangeStreamPosition {
    public final BsonTimestamp operationTime;

    public MongoDBOperationTimeBasedChangeStreamPosition(BsonTimestamp operationTime) {
        this.operationTime = operationTime;
    }

    public BsonValue getOperationTime() {
        return operationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBOperationTimeBasedChangeStreamPosition)) return false;
        MongoDBOperationTimeBasedChangeStreamPosition that = (MongoDBOperationTimeBasedChangeStreamPosition) o;
        return Objects.equals(operationTime, that.operationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationTime);
    }

    @Override
    public String toString() {
        return "MongoDBOperationTimeBasedStreamPosition{" +
                "operationTime=" + operationTime +
                '}';
    }

    @Override
    public String asString() {
        return new Document("operationTime", operationTime).toJson();
    }
}