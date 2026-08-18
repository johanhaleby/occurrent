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

package org.occurrent.subscription.blocking.durable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.tck.subscription.blocking.RestartConformance;
import org.occurrent.tck.subscription.blocking.RestartableSubscriptionModelFixture;
import org.occurrent.tck.subscription.blocking.StartAtVariant;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What all three suites wired against {@link DurableSubscriptionModel} share: a {@link NativeMongoSubscriptionModel}
 * wrapped in the model under test, a {@link CheckpointStorage} it saves a checkpoint to after every successful
 * action, and a {@link MongoEventStore} in front of both that {@link #publish(List)} writes through.
 * <p>
 * The checkpoint storage is {@link InMemoryCheckpointStorage} rather than a real MongoDB one, and not by preference:
 * both MongoDB checkpoint-storage modules (native and Spring) test-depend on this module already, to exercise
 * {@code DurableSubscriptionModel} from the other side, and Maven's reactor cycle check ignores scope, so depending
 * on either of them back from here makes the whole build unbuildable. {@code InMemoryCheckpointStorage} has no such
 * dependency and is otherwise exactly what this fixture needs: a single instance whose state {@link #restart()}
 * hands unchanged to the model it rebuilds, which is the durable state {@link RestartConformance} is about. What it
 * does not exercise is a storage implementation's own durability across a real process restart, which is not what
 * this suite is for and is already covered by {@code CheckpointStorageConformance} against every real storage.
 * <p>
 * The event store, the event collection and the checkpoint storage all outlive {@link #restart()}: only the
 * subscription pair (the native model and the durable wrapper around it) is torn down and rebuilt, the same as a
 * process restart would leave the database untouched and only replace the running application.
 */
class DurableSubscriptionModelFixture implements RestartableSubscriptionModelFixture {

    private final MongoDatabase database;
    private final MongoCollection<Document> eventCollection;
    private final MongoEventStore eventStore;
    // One instance for the whole fixture, never rebuilt: it IS the durable state a restart is supposed to survive,
    // the same reason the event collection above is never recreated either.
    private final CheckpointStorage checkpointStorage = new InMemoryCheckpointStorage();
    private final TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
    private final String streamId = UUID.randomUUID().toString();
    private final AtomicLong streamVersion = new AtomicLong(0);

    private ExecutorService subscriptionExecutor;
    private NativeMongoSubscriptionModel nativeModel;
    private DurableSubscriptionModel durableModel;

    DurableSubscriptionModelFixture(MongoClient mongoClient, MongoDatabase database) {
        this.database = database;
        this.eventCollection = database.getCollection("events-" + UUID.randomUUID());
        this.eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(timeRepresentation));
        buildModel();
    }

    /**
     * Builds a fresh native model plus a fresh durable wrapper around it, over the event collection this fixture
     * already has and the one {@link #checkpointStorage} instance it will ever have. Used both by the constructor
     * and by {@link #restart()}, which is exactly the point: a restart is this same construction run a second time
     * over what the first run left behind, nothing more.
     */
    private void buildModel() {
        this.subscriptionExecutor = Executors.newCachedThreadPool();
        // Same backoff NativeMongoSubscriptionModelFixture uses for retriesAFailingHandler(): short enough that a
        // handler retried a couple of times still lands inside SubscriptionModelConformance.DELIVERY_TIMEOUT
        // (10 seconds), long enough that a retry storm does not spam MongoDB.
        RetryStrategy retryStrategy = RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500), 2.0f);
        this.nativeModel = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, retryStrategy);
        this.durableModel = new DurableSubscriptionModel(nativeModel, checkpointStorage);
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return durableModel;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        // Advanced only once the write has been accepted, so a rejected write does not leave the counter ahead of the
        // stream and turn the next publish into a version conflict that names the wrong problem. Writes through the
        // event store, not through either subscription model, since a change stream only ever observes a write made
        // this way and this same store keeps working across restart() untouched.
        long expectedVersion = streamVersion.get();
        eventStore.write(streamId, expectedVersion, events);
        streamVersion.addAndGet(events.size());
    }

    @Override
    public SubscriptionModel restart() {
        // shutdown() only tears down the native model's cursor and executor (DurableSubscriptionModel.shutdown()
        // delegates straight to it); checkpointStorage is a field, not something buildModel() creates, so it survives
        // untouched, and so does the event collection.
        durableModel.shutdown();
        buildModel();
        return durableModel;
    }

    @Override
    public boolean resumesAfterARestart() {
        // The entire reason this model exists: subscribe(..) saves a checkpoint after every successful action (the
        // default DurableSubscriptionModelConfig persists every event), and checkpointStorage is the one instance
        // this whole fixture ever has, so a subscription re-created on the fresh model reads that checkpoint back
        // and resumes from it rather than from whatever the fresh native model's own default would be.
        return true;
    }

    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        // Pausing is pure delegation. Resuming re-reads the checkpoint this fixture's own action wrapper just
        // saved and hands it to the wrapped model as an explicit position (ADR 117), which for a single-consumer
        // fixture like this one is the same resume token the wrapped model's own change stream last saw anyway.
        // Same answer as NativeMongoSubscriptionModelFixture for the same reason.
        return true;
    }

    @Override
    public boolean retriesAFailingHandler() {
        // subscribe(..) wraps the caller's action in a lambda that also saves the checkpoint, then hands that
        // composite action to the wrapped model exactly as any other action. The wrapped model is the one that
        // retries (it wraps every dispatch in its own RetryStrategy), so a throwing composite action is retried
        // there and the caller's action runs again on the next attempt, same as calling the native model directly.
        return true;
    }

    /**
     * All four, traced through {@code DurableSubscriptionModel.subscribe} rather than assumed:
     * <ul>
     *     <li>{@code NOW} and {@code CHECKPOINT} are neither default nor dynamic, so
     *     {@code generateStartAtPositionFrom} falls to its {@code else} branch and passes the caller's {@code StartAt}
     *     straight to the wrapped model unchanged, exactly as {@code NativeMongoSubscriptionModelFixture} already
     *     proves both of those work.</li>
     *     <li>{@code SUBSCRIPTION_MODEL_DEFAULT} makes {@code subscribe} read the checkpoint storage first, and for
     *     a subscription id it has never seen it records the wrapped model's own {@code globalCheckpoint()} there
     *     before anything else happens. The {@code StartAt.dynamic(..)} handed to the wrapped model then reads that
     *     stored position back, which is a position the wrapped model accepts, so this variant is delivered
     *     to as well, just not from wherever the wrapped model's own default would have been.</li>
     *     <li>{@code DYNAMIC} resolves (per {@link StartAtVariant#startAt}) to
     *     {@code StartAt.dynamic(StartAt::subscriptionModelDefault)}. {@code generateStartAtPositionFrom} calls it,
     *     gets back {@code subscriptionModelDefault()}, and recurses, landing in the same branch as
     *     {@code SUBSCRIPTION_MODEL_DEFAULT} above. Delivered for the same reason.</li>
     * </ul>
     * The one start position that can make {@code subscribe} throw, the model default when nothing is stored and the
     * wrapped model's {@code globalCheckpoint()} answers {@code null}, never does so here, because
     * {@code NativeMongoSubscriptionModel} over the local MongoDB this suite runs against always answers. A dynamic
     * position resolving to {@code null} is the only case it treats specially, and even that is not a
     * refusal, it delegates the subscription to the wrapped model unchanged. That is how a caller opts a subscription
     * out of this wrapper rather than how it names a position the wrapper cannot honour, which is why
     * {@link StartAtVariant#DYNAMIC} deliberately does not build one.
     */
    @Override
    public Set<StartAtVariant> acceptedStartAtVariants() {
        return EnumSet.allOf(StartAtVariant.class);
    }

    /**
     * False, because the checkpoint {@code SUBSCRIPTION_MODEL_DEFAULT} resolves to (see
     * {@link #acceptedStartAtVariants()}) is read from the wrapped model's {@code globalCheckpoint()} at subscribe
     * time, which is necessarily at or after anything already published: {@code publish(..)} only returns once
     * {@code MongoEventStore.write} has committed, and the subscribe call this suite makes always happens afterwards.
     * So a subscription id this model has never seen starts from "now", not from the beginning, the same as the
     * wrapped {@code NativeMongoSubscriptionModel} on its own.
     */
    @Override
    public boolean replaysHistoryToANewSubscription() {
        return false;
    }

    /**
     * This model is checkpoint-aware and its own {@code globalCheckpoint()} simply delegates to the wrapped native
     * model's, so the honest answer is the same one {@code NativeMongoSubscriptionModelFixture} gives: whatever the
     * wrapped model reports.
     */
    @Override
    public Checkpoint aCheckpointToStartFrom() {
        return SubscriptionModelFixture.orGlobalPositionZero(durableModel.globalCheckpoint());
    }

    @Override
    public void close() {
        durableModel.shutdown();
        // Delete documents rather than dropping the collection: dropping kills a live change stream, and shutdown()
        // above has only just asked this model's own stream to close. checkpointStorage needs no cleanup of its own,
        // it dies with this fixture instance.
        eventCollection.deleteMany(new Document());
    }
}
