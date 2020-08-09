package se.haleby.occurrent.changestreamer.mongodb;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.Objects;

public class MongoDBStreamPosition {
    public final BsonTimestamp operationTime;
    public final BsonDocument resumeToken;

    public MongoDBStreamPosition(BsonTimestamp operationTime, BsonDocument resumeToken) {
        this.operationTime = operationTime;
        this.resumeToken = resumeToken;
    }

    public BsonTimestamp getOperationTime() {
        return operationTime;
    }

    public BsonDocument getResumeToken() {
        return resumeToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBStreamPosition)) return false;
        MongoDBStreamPosition that = (MongoDBStreamPosition) o;
        return Objects.equals(operationTime, that.operationTime) &&
                Objects.equals(resumeToken, that.resumeToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationTime, resumeToken);
    }

    @Override
    public String toString() {
        return "MongoDBStreamPosition{" +
                "operationTime=" + operationTime +
                ", resumeToken=" + resumeToken +
                '}';
    }
}