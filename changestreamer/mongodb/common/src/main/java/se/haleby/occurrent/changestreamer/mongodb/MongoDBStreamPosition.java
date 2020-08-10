package se.haleby.occurrent.changestreamer.mongodb;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import se.haleby.occurrent.changestreamer.StreamPosition;

import java.util.Objects;

public class MongoDBStreamPosition implements StreamPosition {
    public final BsonTimestamp clusterTime;
    public final BsonValue resumeToken;

    public MongoDBStreamPosition(BsonTimestamp clusterTime, BsonValue resumeToken) {
        this.clusterTime = clusterTime;
        this.resumeToken = resumeToken;
    }

    public BsonTimestamp getClusterTime() {
        return clusterTime;
    }

    public BsonValue getResumeToken() {
        return resumeToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBStreamPosition)) return false;
        MongoDBStreamPosition that = (MongoDBStreamPosition) o;
        return Objects.equals(clusterTime, that.clusterTime) &&
                Objects.equals(resumeToken, that.resumeToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterTime, resumeToken);
    }

    @Override
    public String toString() {
        return "MongoDBStreamPosition{" +
                "clusterTime=" + clusterTime +
                ", resumeToken=" + resumeToken +
                '}';
    }
}