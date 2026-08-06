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

package org.occurrent.subscription.blocking.durable.catchup;

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
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
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

import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * What both suites wired against {@link CatchupSubscriptionModel} share: {@code Catchup(Durable(NativeMongo))}, the
 * stream-only catch-up model wrapping {@link DurableSubscriptionModel} wrapping {@link NativeMongoSubscriptionModel},
 * over a {@link MongoEventStore} that {@link #publish(List)} writes through. The catch-up model's own checkpoint
 * storage and the durable model's are the same {@link NativeMongoCheckpointStorage} instance, the pattern
 * {@code CatchupSubscriptionModelTest.catchup_subscription_continues_where_it_left_off_after_all_historic_events_have_been_consumed}
 * already builds by hand: reading it back is what lets the catch-up model resolve a caller's default start position
 * either into a fresh catch-up (nothing stored yet) or into whatever the durable model already resumed from (a
 * position previously read from the very same collection).
 * <p>
 * The event store defaults to writing a stream position ({@code new EventStoreConfig(timeRepresentation)}, same as
 * {@code DurableSubscriptionModelFixture}), which is what routes {@code StreamCatchupSubscriptionModel.subscribe}'s
 * position-mode branch rather than the legacy time-ordered one. It makes no difference to what this fixture declares:
 * none of the checkpoints in play here (a {@code MongoOperationTimeCheckpoint} from {@code globalCheckpoint()}, or a
 * {@code MongoResumeTokenCheckpoint} off a delivered event) is a {@code GlobalCheckpoint}, so
 * {@code classifyStreamStart} resolves every one of them to {@code LIVE} and the model delegates straight to the
 * wrapped {@link DurableSubscriptionModel} instead of running a replay. Forcing an actual replay needs a
 * {@code TimeBasedCheckpoint} or an explicit {@code GlobalCheckpoint}, which is what
 * {@code CatchupSubscriptionModelTest} and {@code StreamPositionCatchupSubscriptionModelMongoTest} already exercise
 * directly; this fixture is about the wrapper's contract as a {@code SubscriptionModel}, not about re-proving the
 * replay logic those cover.
 * <p>
 * The event store, the event collection and the checkpoint storage all outlive {@link #restart()}: only the
 * subscription trio (the native model, the durable wrapper, and the catch-up wrapper around that) is torn down and
 * rebuilt, the same as a process restart would leave the database untouched and only replace the running
 * application. Copied from {@code DurableSubscriptionModelFixture}, which is this same stack minus the catch-up
 * wrapper.
 */
class CatchupSubscriptionModelFixture implements RestartableSubscriptionModelFixture {

    private final MongoDatabase database;
    private final MongoCollection<Document> eventCollection;
    private final MongoEventStore eventStore;
    // One instance for the whole fixture, never rebuilt: a real MongoDB collection behind it, so it is the durable
    // state a restart is supposed to survive, the same reason the event collection above is never recreated either.
    private final NativeMongoCheckpointStorage storage;
    private final TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
    private final String streamId = UUID.randomUUID().toString();
    private final AtomicLong streamVersion = new AtomicLong(0);

    private ExecutorService subscriptionExecutor;
    private NativeMongoSubscriptionModel nativeModel;
    private DurableSubscriptionModel durableModel;
    private CatchupSubscriptionModel catchupModel;

    CatchupSubscriptionModelFixture(MongoClient mongoClient, MongoDatabase database) {
        this.database = database;
        this.eventCollection = database.getCollection("events-" + UUID.randomUUID());
        this.eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(timeRepresentation));
        this.storage = new NativeMongoCheckpointStorage(database, "checkpoints-" + UUID.randomUUID());
        buildModel();
    }

    /**
     * Builds a fresh native model, a fresh durable wrapper around it, and a fresh catch-up wrapper around that, over
     * the event collection and the checkpoint storage this fixture already has. Used both by the constructor and by
     * {@link #restart()}, which is exactly the point: a restart is this same construction run a second time over what
     * the first run left behind, nothing more.
     */
    private void buildModel() {
        this.subscriptionExecutor = Executors.newCachedThreadPool();
        // Same backoff NativeMongoSubscriptionModelFixture uses for retriesAFailingHandler(): short enough that a
        // handler retried a couple of times still lands inside SubscriptionModelConformance.DELIVERY_TIMEOUT
        // (10 seconds), long enough that a retry storm does not spam MongoDB.
        RetryStrategy retryStrategy = RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500), 2.0f);
        this.nativeModel = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, retryStrategy);
        this.durableModel = new DurableSubscriptionModel(nativeModel, storage);
        this.catchupModel = new CatchupSubscriptionModel(durableModel, eventStore, new CatchupSubscriptionModelConfig(useCheckpointStorage(storage)));
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return catchupModel;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        // Advanced only once the write has been accepted, so a rejected write does not leave the counter ahead of the
        // stream and turn the next publish into a version conflict that names the wrong problem. Writes through the
        // event store, not through any of the subscription models, since a change stream only ever observes a write
        // made this way and this same store keeps working across restart() untouched.
        long expectedVersion = streamVersion.get();
        eventStore.write(streamId, expectedVersion, events);
        streamVersion.addAndGet(events.size());
    }

    @Override
    public SubscriptionModel restart() {
        // shutdown() tears down the catch-up model's bookkeeping and delegates to the durable model, which delegates
        // to the native model's cursor and executor; storage is a field, not something buildModel() creates, so it
        // survives untouched, and so does the event collection.
        catchupModel.shutdown();
        buildModel();
        return catchupModel;
    }

    /**
     * True. The durable model saves a checkpoint (off the delivered event, or off the wrapped model's own
     * {@code globalCheckpoint()} the first time a subscription id resolves its default) into {@link #storage} after
     * every successful action, and {@link #storage} is backed by a MongoDB collection that outlives {@link #restart()}
     * untouched. Reading it back on the fresh model, {@code StreamCatchupSubscriptionModel.subscribe}'s
     * {@code startAt.isDefault()} branch finds it non-null, classifies it as {@code LIVE} (see the class javadoc), and
     * delegates straight to the fresh {@link DurableSubscriptionModel}, unwrapped, with that checkpoint as an explicit
     * {@code StartAt.checkpoint(..)}. The durable model passes it straight to the fresh native model, whose change
     * stream resumes from that token, redelivering whatever committed after it, including an event published while
     * nothing was running.
     */
    @Override
    public boolean resumesAfterARestart() {
        return true;
    }

    /**
     * True, for the same reason {@code DurableSubscriptionModelFixture} answers true: pausing and resuming are pure
     * delegation all the way down ({@code CatchupSubscriptionModel.pauseSubscription/resumeSubscription} call straight
     * through to {@code getDelegatedSubscriptionModel()}, which {@code DurableSubscriptionModel} again delegates
     * straight through), and the native model at the bottom resumes its change stream from the resume token it last
     * saw, which MongoDB still has queued in the oplog.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return true;
    }

    /**
     * True. Every accepted start position here reaches the wrapped {@link DurableSubscriptionModel} unwrapped (see the
     * class javadoc: {@code classifyStreamStart} resolves every checkpoint this fixture produces to {@code LIVE}), and
     * {@code DurableSubscriptionModel} adds no retry of its own around the action, so whether a throwing handler is
     * retried is entirely the wrapped {@link NativeMongoSubscriptionModel}'s own {@code RetryStrategy}, the same
     * answer {@code NativeMongoSubscriptionModelFixture} and {@code DurableSubscriptionModelFixture} give.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    /**
     * All four, traced through {@code StreamCatchupSubscriptionModel.subscribe} and {@code classifyStreamStart} rather
     * than assumed:
     * <ul>
     *     <li>{@code NOW} resolves to a bare {@code StartAt.Now}, which is not a {@code StartAtCheckpoint} at all, so
     *     {@code classifyStreamStart} falls straight to {@code LIVE} and the caller's {@code StartAt} is passed to the
     *     wrapped model unchanged.</li>
     *     <li>{@code SUBSCRIPTION_MODEL_DEFAULT} on a subscription id this model has never seen reads {@link #storage}
     *     and finds nothing, so {@code subscribe}'s own {@code checkpoint == null} branch delegates straight to the
     *     wrapped model with the caller's default {@code StartAt} untouched, never even reaching
     *     {@code classifyStreamStart}.</li>
     *     <li>{@code CHECKPOINT}, built from {@link #aCheckpointToStartFrom()} (a {@code MongoOperationTimeCheckpoint},
     *     see that method), is neither a {@code TimeBasedCheckpoint} nor a {@code GlobalCheckpoint}, so
     *     {@code classifyStreamStart} again falls to {@code LIVE} and the checkpoint is handed to the wrapped model as
     *     an explicit {@code StartAt.checkpoint(..)}.</li>
     *     <li>{@code DYNAMIC} resolves (per {@code StartAtVariant#startAt}) to
     *     {@code StartAt.dynamic(StartAt::subscriptionModelDefault)}. {@code subscribe}'s {@code startAt.isDynamic()}
     *     branch resolves it once to a bare {@code StartAt.Default}, which is not null, so it becomes
     *     {@code firstStartAt}; {@code classifyStreamStart} resolves that to {@code LIVE} for the same reason
     *     {@code NOW} does, and the model delegates to the wrapped model with that {@code Default}, which is exactly
     *     the {@code SUBSCRIPTION_MODEL_DEFAULT} case one level down.</li>
     * </ul>
     * Every one of the four therefore reaches a live, working subscription on the wrapped {@link DurableSubscriptionModel}.
     * This model refuses nothing, in the sense the suite means by refusal: no start position makes {@code subscribe}
     * throw. A dynamic one resolving to {@code null} is the only case it treats specially, and even that is not a
     * refusal, it delegates the subscription to the wrapped model unchanged. That is how a caller opts a subscription
     * out of this wrapper rather than how it names a position the wrapper cannot honour, which is why
     * {@link StartAtVariant#DYNAMIC} deliberately does not build one.
     */
    @Override
    public Set<StartAtVariant> acceptedStartAtVariants() {
        return EnumSet.allOf(StartAtVariant.class);
    }

    /**
     * False. On an empty {@link #storage} and the default start position, {@code subscribe}'s
     * {@code checkpoint == null} branch delegates straight to the wrapped {@link DurableSubscriptionModel} without
     * ever running a catch-up replay (see {@link #acceptedStartAtVariants()}), and the durable model resolves its own
     * default to the wrapped native model's current {@code globalCheckpoint()}, which is at or after anything already
     * published: {@link #publish(List)} only returns once {@code MongoEventStore.write} has committed, and the
     * subscribe call this suite makes always happens afterwards. So a subscription id this model has never seen
     * starts from "now", not from the beginning.
     */
    @Override
    public boolean replaysHistoryToANewSubscription() {
        return false;
    }

    /**
     * This model is checkpoint-aware only by delegation: {@code CatchupSubscriptionModel} has no
     * {@code globalCheckpoint()} of its own, so the honest position to hand back is the wrapped durable model's,
     * which itself delegates to the native model's. That can come back null on the rare server that refuses
     * {@code hostInfo}.
     */
    @Override
    public Checkpoint aCheckpointToStartFrom() {
        return SubscriptionModelFixture.orGlobalPositionZero(durableModel.globalCheckpoint());
    }

    @Override
    public void close() {
        catchupModel.shutdown();
        // Delete documents rather than dropping either collection: dropping kills a live change stream, and
        // shutdown() above has only just asked this model's own stream to close.
        eventCollection.deleteMany(new Document());
        storage.shutdown();
    }
}
