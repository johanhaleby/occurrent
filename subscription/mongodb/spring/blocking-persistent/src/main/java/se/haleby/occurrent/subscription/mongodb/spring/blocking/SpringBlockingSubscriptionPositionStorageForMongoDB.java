package se.haleby.occurrent.subscription.mongodb.spring.blocking;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import se.haleby.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.*;

/**
 * A Spring implementation of {@link BlockingSubscriptionPositionStorage} that stores {@link SubscriptionPosition} in MongoDB.
 */
public class SpringBlockingSubscriptionPositionStorageForMongoDB implements BlockingSubscriptionPositionStorage {

    private final MongoOperations mongoOperations;
    private final String subscriptionPositionCollection;

    /**
     * Create a {@link BlockingSubscriptionPositionStorage} that uses the Spring's {@link MongoOperations} to persist subscription positions in MongoDB.
     *
     * @param mongoOperations                The {@link MongoOperations} that'll be used to store the subscription position
     * @param subscriptionPositionCollection The collection into which subscription positions will be stored
     */
    public SpringBlockingSubscriptionPositionStorageForMongoDB(MongoOperations mongoOperations, String subscriptionPositionCollection) {
        requireNonNull(mongoOperations, "Mongo operations cannot be null");
        requireNonNull(subscriptionPositionCollection, "subscriptionPositionCollection cannot be null");

        this.mongoOperations = mongoOperations;
        this.subscriptionPositionCollection = subscriptionPositionCollection;
    }

    @Override
    public SubscriptionPosition read(String subscriptionId) {
        Document document = mongoOperations.findOne(query(where(ID).is(subscriptionId)), Document.class, subscriptionPositionCollection);
        if (document == null) {
            return null;
        }
        return calculateSubscriptionPositionFromMongoStreamPositionDocument(document);
    }

    @Override
    public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
        if (subscriptionPosition instanceof MongoDBResumeTokenBasedSubscriptionPosition) {
            persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedSubscriptionPosition) subscriptionPosition).resumeToken);
        } else if (subscriptionPosition instanceof MongoDBOperationTimeBasedSubscriptionPosition) {
            persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedSubscriptionPosition) subscriptionPosition).operationTime);
        } else {
            String subscriptionPositionString = subscriptionPosition.asString();
            Document document = generateGenericStreamPositionDocument(subscriptionId, subscriptionPositionString);
            persistDocumentStreamPosition(subscriptionId, document);
        }
        return subscriptionPosition;
    }

    @Override
    public void delete(String subscriptionId) {
        mongoOperations.remove(query(where(ID).is(subscriptionId)), subscriptionPositionCollection);
    }

    private void persistResumeTokenStreamPosition(String subscriptionId, BsonValue resumeToken) {
        persistDocumentStreamPosition(subscriptionId, generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken));
    }

    private void persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp operationTime) {
        persistDocumentStreamPosition(subscriptionId, generateOperationTimeStreamPositionDocument(subscriptionId, operationTime));
    }

    private void persistDocumentStreamPosition(String subscriptionId, Document document) {
        mongoOperations.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                subscriptionPositionCollection);
    }
}