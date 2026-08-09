/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.blocking.competingconsumers;

import com.mongodb.client.MongoClient;
import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.tck.subscription.blocking.StartAtVariant;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.occurrent.tck.subscription.blocking.SubscriptionModelFixture.orGlobalPositionZero;

/**
 * Wraps {@link CompetingConsumerSubscriptionModel} around exactly the stack its own class-level javadoc recommends:
 * a {@link DurableSubscriptionModel} persisting a checkpoint after every event, over a {@link SpringMongoSubscriptionModel}
 * reading a real change stream, under a real {@link SpringMongoLeaseCompetingConsumerStrategy} lease. One
 * {@code CompetingConsumerSubscriptionModel} instance is uncontested here: nothing else registers for the same
 * subscription id, so every {@code registerCompetingConsumer} call this fixture triggers is granted synchronously
 * (see {@link SpringMongoLeaseCompetingConsumerStrategy#registerCompetingConsumer}, which is {@code synchronized} and
 * returns the outcome directly, no callback and no background thread involved when nobody else holds the lease). That
 * is what makes the delivery suite safe to run here without a wait for the lock: a single-consumer subscribe returns
 * only once the delegate has actually subscribed, exactly like {@code SpringMongoSubscriptionModelFixture}.
 * <p>
 * Same collection-per-fixture isolation as {@code SpringMongoSubscriptionModelFixture}, for the same reason: a shared
 * collection would let one test's change stream see another test's writes, and dropping it to clean up would kill a
 * live change stream.
 */
class CompetingConsumerSubscriptionModelFixture implements SubscriptionModelFixture {

    private final String streamId = UUID.randomUUID().toString();
    private final AtomicLong version = new AtomicLong(0);

    private final EventStore eventStore;
    private final SpringMongoSubscriptionModel innerSpringModel;
    private final CompetingConsumerSubscriptionModel subscriptionModel;
    private final SpringMongoLeaseCompetingConsumerStrategy strategy;
    private final MongoTemplate mongoTemplate;
    private final String eventCollectionName;

    CompetingConsumerSubscriptionModelFixture(MongoClient mongoClient, MongoTemplate mongoTemplate, String databaseName) {
        this.mongoTemplate = mongoTemplate;
        this.eventCollectionName = "events-" + UUID.randomUUID();
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(timeRepresentation)
                .build();
        this.eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        this.innerSpringModel = new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, timeRepresentation);
        // A checkpoint collection of its own too, so a leftover checkpoint from one test can never seed another.
        SpringMongoCheckpointStorage checkpointStorage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints-" + UUID.randomUUID());
        DurableSubscriptionModel durableModel = new DurableSubscriptionModel(innerSpringModel, checkpointStorage);
        // A lock collection of its own too. Two fixtures sharing one would let a lease left behind by a shut-down
        // model answer registerCompetingConsumer for a subscription id the new fixture never associated with it.
        this.strategy = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoTemplate).collectionName("competing-consumer-locks-" + UUID.randomUUID()).build();
        this.subscriptionModel = new CompetingConsumerSubscriptionModel(durableModel, strategy);
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        long expectedVersion = version.get();
        eventStore.write(streamId, expectedVersion, events);
        version.addAndGet(events.size());
    }

    /**
     * True, for a different reason than {@code SpringMongoSubscriptionModelFixture} answers false, and not a
     * re-litigation of #522. That fixture drives {@link SpringMongoSubscriptionModel} directly with a literal
     * {@code StartAt.subscriptionModelDefault()}, so {@link SpringMongoSubscriptionModel#resumeSubscription} reopens
     * the change stream from that same literal object, which carries no position and so resolves to "now" again,
     * every time. This wiring is what {@link CompetingConsumerSubscriptionModel}'s own class-level javadoc recommends
     * running it over, a {@link DurableSubscriptionModel} sitting between the two. Pausing (whichever way
     * {@link CompetingConsumerSubscriptionModel#pauseSubscription} gets there, it unregisters the lease for a user
     * pause and only releases it for a system one, see its private {@code pauseSubscription(String, boolean)}) always
     * calls the delegate's own {@code pauseSubscription}, and resuming (whichever way it gets there, straight through
     * when the lease was only released, or by winning the lease back when it was unregistered) always ends in the
     * delegate's own {@code resumeSubscription}, which is {@code DurableSubscriptionModel}'s. That re-reads the
     * checkpoint saved after the last event this subscription actually delivered and hands it to
     * {@link SpringMongoSubscriptionModel} as an explicit position (ADR 117, #668), rather than relying on
     * {@code SpringMongoSubscriptionModel}'s own tracked position, which another node may have long since passed by
     * the time this one regains the lease. Either way the change stream reopens from the checkpoint and replays
     * forward from there. {@code CompetingConsumerSubscriptionModelTest.can_pause_and_resume_same_subscription}
     * already demonstrates the outcome, ids {@code 1, 2, 3} arrive in order, with {@code 2} written while paused.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return true;
    }

    /**
     * The retry wrapping happens in {@link SpringMongoSubscriptionModel}'s change-stream listener
     * ({@code executeWithRetry(action, ..., retryStrategy)}), around whatever action reaches it, which here is
     * {@code DurableSubscriptionModel}'s checkpoint-saving wrapper around the caller's own handler. Neither
     * {@code DurableSubscriptionModel} nor {@code CompetingConsumerSubscriptionModel} adds a second layer that would
     * change this, so a throwing handler is retried exactly like the bare model.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    /**
     * All four. {@link CompetingConsumerSubscriptionModel#subscribe(String, String, org.occurrent.subscription.SubscriptionFilter, org.occurrent.subscription.StartAt, java.util.function.Consumer)}
     * only opts a subscription out of competing consumption when
     * {@code startAt.get(new SubscriptionModelContext(CompetingConsumerSubscriptionModel.class))} resolves to
     * {@code null}, and none of the four variants here does that: {@link StartAtVariant#DYNAMIC} resolves to
     * {@code StartAt.subscriptionModelDefault()}, not {@code null}, so every variant takes the competing-consumer
     * path and reaches the delegate with the position unchanged. Nothing below narrows the set either:
     * {@code DurableSubscriptionModel} rewrites a default or dynamic position but passes {@code now} and
     * {@code checkpoint} straight through, and {@code SpringMongoSubscriptionModelFixture} leaves this at its own
     * default of all four for the same reason, since {@link SpringMongoSubscriptionModel} is checkpoint aware and
     * places no restriction on the position it is handed.
     */
    @Override
    public Set<StartAtVariant> acceptedStartAtVariants() {
        return EnumSet.allOf(StartAtVariant.class);
    }

    /**
     * Read from the innermost model, the one actually watching the change stream, the same as
     * {@code SpringMongoSubscriptionModelFixture} does. A model that ignores the position it is asked to start from
     * has no such source, but this one is checkpoint aware all the way down.
     */
    @Override
    public Checkpoint aCheckpointToStartFrom() {
        return orGlobalPositionZero(innerSpringModel.globalCheckpoint());
    }

    @Override
    public void close() {
        // Shuts down the delegate chain (DurableSubscriptionModel, then SpringMongoSubscriptionModel) and the
        // competing consumer strategy, since CompetingConsumerSubscriptionModel.shutdown() does both.
        subscriptionModel.shutdown();
        // Empties every collection this fixture wrote to (events, checkpoints, and the competing-consumer lock),
        // rather than hand-listing them: a hand written list stops covering a collection the day a feature is
        // switched on, which is exactly why OccurrentMongoFlush exists. Empties rather than drops, since dropping
        // kills a live change stream, and shutdown() above has only just asked this model's own stream to close.
        OccurrentMongoFlush.everyCollectionIn(mongoTemplate.getDb()).run();
    }
}
