package se.haleby.occurrent.subscription.mongodb;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import se.haleby.occurrent.subscription.SubscriptionPosition;

import java.util.Objects;

/**
 * A {@link SubscriptionPosition} implementation for MongoDB that provides a resumeToken
 * that consumers may decide which to use when continuing the stream.
 */
public class MongoDBResumeTokenBasedSubscriptionPosition implements SubscriptionPosition {
    public final BsonDocument resumeToken;

    public MongoDBResumeTokenBasedSubscriptionPosition(BsonDocument resumeToken) {
        this.resumeToken = resumeToken;
    }

    public BsonValue getResumeToken() {
        return resumeToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBResumeTokenBasedSubscriptionPosition)) return false;
        MongoDBResumeTokenBasedSubscriptionPosition that = (MongoDBResumeTokenBasedSubscriptionPosition) o;
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