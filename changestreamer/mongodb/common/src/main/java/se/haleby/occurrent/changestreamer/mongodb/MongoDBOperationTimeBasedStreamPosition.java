package se.haleby.occurrent.changestreamer.mongodb;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import se.haleby.occurrent.changestreamer.StreamPosition;

import java.util.Objects;

/**
 * A {@link StreamPosition} implementation for MongoDB that provides the operation time
 * that consumers may decide which to use when continuing the stream.
 */
public class MongoDBOperationTimeBasedStreamPosition implements StreamPosition {
    public final BsonTimestamp operationTime;

    public MongoDBOperationTimeBasedStreamPosition(BsonTimestamp operationTime) {
        this.operationTime = operationTime;
    }

    public BsonValue getOperationTime() {
        return operationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBOperationTimeBasedStreamPosition)) return false;
        MongoDBOperationTimeBasedStreamPosition that = (MongoDBOperationTimeBasedStreamPosition) o;
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