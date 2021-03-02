package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.occurrent.subscription.api.blocking.CompetingConsumersStrategy;
import org.springframework.data.mongodb.core.MongoOperations;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Function;

public class MongoLeaseCompetingConsumersStrategy implements CompetingConsumersStrategy {

    public static final String COMPETING_CONSUMER_LOCKS_COLLECTION = "competing-consumer-locks";

    private final MongoOperations mongoOperations;
    private final Clock clock;
    private final Duration leaseTime;
    private final String collectionName;

    public MongoLeaseCompetingConsumersStrategy(MongoOperations mongoOperations, Clock clock, Duration leaseTime, String collectionName) {
        this.mongoOperations = mongoOperations;
        this.clock = clock;
        this.leaseTime = leaseTime;
        this.collectionName = collectionName;
    }


    public static CompetingConsumersStrategy withDefaults(MongoOperations mongoOperations) {
        return new MongoLeaseCompetingConsumersStrategy(mongoOperations, Clock.systemUTC(), Duration.ofMinutes(1), COMPETING_CONSUMER_LOCKS_COLLECTION);
    }

    @Override
    public void registerCompetingConsumer(String subscriptionId, String subscriberId) {
        withCompetingConsumerLocksCollectionDo(collection -> MongoListenerLockService.acquireOrRefreshFor(collection, clock, leaseTime, subscriptionId, subscriberId));
    }

    @Override
    public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        // TODO Remove document!
        withCompetingConsumerLocksCollectionDo(collection -> null);
    }

    private <T> void withCompetingConsumerLocksCollectionDo(Function<MongoCollection<BsonDocument>, T> fn) {
        mongoOperations.execute(db -> {
            MongoCollection<BsonDocument> collection = db.getCollection(collectionName, BsonDocument.class);
            return fn.apply(collection);
        });
    }
}