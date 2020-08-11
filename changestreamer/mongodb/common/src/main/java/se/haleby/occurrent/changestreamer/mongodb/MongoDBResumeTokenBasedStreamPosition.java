package se.haleby.occurrent.changestreamer.mongodb;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import se.haleby.occurrent.changestreamer.StreamPosition;

import java.util.Objects;

/**
 * A {@link StreamPosition} implementation for MongoDB that provides a resumeToken
 * that consumers may decide which to use when continuing the stream.
 */
public class MongoDBResumeTokenBasedStreamPosition implements StreamPosition {
    public final BsonDocument resumeToken;

    public MongoDBResumeTokenBasedStreamPosition(BsonDocument resumeToken) {
        this.resumeToken = resumeToken;
    }

    public BsonValue getResumeToken() {
        return resumeToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBResumeTokenBasedStreamPosition)) return false;
        MongoDBResumeTokenBasedStreamPosition that = (MongoDBResumeTokenBasedStreamPosition) o;
        return Objects.equals(resumeToken, that.resumeToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resumeToken);
    }

    @Override
    public String toString() {
        return "MongoDBStreamPosition{" +
                "resumeToken=" + resumeToken +
                '}';
    }

    @Override
    public String asString() {
        return new Document("resumeToken", resumeToken).toJson();
    }
}