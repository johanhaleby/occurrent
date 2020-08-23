package org.occurrent.subscription.mongodb;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.occurrent.subscription.SubscriptionPosition;

import java.util.Objects;

/**
 * A {@link SubscriptionPosition} implementation for MongoDB that provides the operation time
 * that consumers may decide which to use when continuing the stream.
 */
public class MongoDBOperationTimeBasedSubscriptionPosition implements SubscriptionPosition {
    public final BsonTimestamp operationTime;

    public MongoDBOperationTimeBasedSubscriptionPosition(BsonTimestamp operationTime) {
        this.operationTime = operationTime;
    }

    public BsonValue getOperationTime() {
        return operationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBOperationTimeBasedSubscriptionPosition)) return false;
        MongoDBOperationTimeBasedSubscriptionPosition that = (MongoDBOperationTimeBasedSubscriptionPosition) o;
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