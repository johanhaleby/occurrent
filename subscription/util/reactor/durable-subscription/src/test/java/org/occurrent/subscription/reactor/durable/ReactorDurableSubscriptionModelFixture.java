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

package org.occurrent.subscription.reactor.durable;

import com.mongodb.reactivestreams.client.MongoClient;
import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.tck.eventstore.reactor.BlockingEventStoreOverReactive;
import org.occurrent.tck.latecommit.AppendHold;
import org.occurrent.tck.latecommit.HeldAppend;
import org.occurrent.tck.latecommit.LateCommits;
import org.occurrent.tck.subscription.blocking.RestartableSubscriptionModelFixture;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.tck.subscription.reactor.BlockingSubscriptionOverReactive;
import org.occurrent.testsupport.mongodb.HoldingTransactionalOperator;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * Feeds {@code Durable(Catchup(Mongo))}, the composition the Spring Boot starter actually wires
 * (see {@code ORCHESTRATOR.md}'s "Primary execution flows": Durable persists the checkpoint, Catchup replays history
 * for whatever start position Durable resolves, before handing over to the live Mongo model). Every fixture instance
 * gets its own event collection and its own checkpoint collection, named with a UUID, mirroring
 * {@code ReactorMongoSubscriptionModelFixture}.
 */
class ReactorDurableSubscriptionModelFixture implements RestartableSubscriptionModelFixture {

    private final String streamId = UUID.randomUUID().toString();

    private final ReactorMongoEventStore eventStore;
    private final ReactiveMongoTemplate reactiveMongoTemplate;
    private final ReactiveTransactionManager reactiveMongoTransactionManager;
    private final TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
    private final String eventCollectionName;
    private final String checkpointCollectionName;
    // One instance for the whole fixture, never rebuilt: the checkpoint collection IS the durable state a restart
    // is supposed to survive, the same reason the event collection is never recreated either.
    private final ReactorCheckpointStorage checkpointStorage;

    private ReactorDurableSubscriptionModel durableModel;
    private SubscriptionModel subscriptionModel;

    ReactorDurableSubscriptionModelFixture(MongoClient mongoClient, String databaseName) {
        this.eventCollectionName = "events-" + UUID.randomUUID();
        this.checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
        this.reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, databaseName);
        this.reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));
        this.eventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig(builder -> builder.transactionConfig(reactiveMongoTransactionManager)));
        this.checkpointStorage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        buildModel();
    }

    private EventStoreConfig eventStoreConfig(UnaryOperator<EventStoreConfig.Builder> transactions) {
        return transactions.apply(new EventStoreConfig.Builder()
                        .eventStoreCollectionName(eventCollectionName)
                        .timeRepresentation(timeRepresentation))
                .build();
    }

    /**
     * Builds a fresh {@code Durable(Catchup(Mongo))} over the event collection and the one checkpoint storage this
     * fixture has. Used by the constructor and by {@link #restart()}, so a restart is this same construction run a
     * second time over what the first run left behind.
     */
    private void buildModel() {
        ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, timeRepresentation);
        // A default filter, or the catch-up model refuses a subscription made with no SubscriptionFilter at all,
        // which is exactly what the TCK's convenience subscribe(id, action) overload passes.
        CheckpointAwareSubscriptionModel catchupModel = new ReactorCatchupSubscriptionModel(mongoModel, eventStore, Filter.all());
        this.durableModel = new ReactorDurableSubscriptionModel(catchupModel, checkpointStorage);
        this.subscriptionModel = BlockingSubscriptionOverReactive.of(durableModel);
    }

    ReactorDurableSubscriptionModel durableModel() {
        return durableModel;
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public SubscriptionModel restart() {
        durableModel.shutdown();
        buildModel();
        return subscriptionModel;
    }

    /**
     * False. A bare {@code GlobalCheckpoint} carries no live start, so the reactive catch-up model inside cannot tell
     * that nothing committed below it and refuses the start, see {@link #refusesABareGlobalPosition()}.
     */
    @Override
    public boolean honoursAGlobalPosition() {
        return false;
    }

    /**
     * True. The refusal is a {@code CatchupCannotResumeException}, and the checkpoint delivered with an event is what to
     * start from instead.
     */
    @Override
    public boolean refusesABareGlobalPosition() {
        return true;
    }

    /**
     * The model resumes from the checkpoint {@link #checkpointStorage} holds, and that outlives {@link #restart()}.
     */
    @Override
    public boolean resumesAfterARestart() {
        return true;
    }

    /**
     * The store in front of the model reserves an event's position before the transaction that writes it begins, so a
     * write can be held between the two. The hold is a {@link TransactionalOperator} that parks the first transaction
     * it is asked to wrap without blocking a thread, on the same event collection as {@link #eventStore}, so the
     * subscription sees the held write once it commits.
     */
    @Override
    public LateCommits lateCommits() {
        return LateCommits.holding(() -> {
            AppendHold hold = new AppendHold();
            TransactionalOperator holding = new HoldingTransactionalOperator(TransactionalOperator.create(reactiveMongoTransactionManager),
                    () -> hold.engage() ? Mono.fromFuture(hold.released()).timeout(AppendHold.RELEASE_TIMEOUT) : Mono.empty());
            ReactorMongoEventStore holdingStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig(builder -> builder.transactionConfig(holding)));
            return HeldAppend.of(hold, BlockingEventStoreOverReactive.of(holdingStore), () -> {
            });
        });
    }

    @Override
    public void publish(List<CloudEvent> events) {
        eventStore.write(streamId, Flux.fromIterable(events)).block();
    }

    /**
     * Resuming reuses the checkpoint of the last event this subscription actually delivered
     * ({@code ReactorDurableSubscriptionModel.source(..)}'s {@code doOnSuccess}), a concrete {@code StartAt} that
     * skips {@code resolveStartAt}'s default-position branch, so the wrapped catch-up model bulk-replays everything
     * committed after it, including whatever was published while paused.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return true;
    }

    /**
     * A failing action's {@code Mono} is retried. Since the catch-up model promotion (#550) the durable model
     * delegates this composition to the wrapped model's named {@code subscribe}, so the failing action lands inside
     * {@code ReactorMongoSubscriptionModel}'s handler retry and is re-invoked with the model's backoff. This
     * declaration said {@code false} while the composition ran the durable model's own unguarded pipeline, which is
     * exactly the divergence #547 recorded and this fixture's red run demonstrated.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    /**
     * A new subscription with no stored checkpoint starts where {@code globalCheckpointAsOfNow()} of the catch-up model
     * puts the moment of {@code subscribe(..)}. The catch-up model asks the {@link ReactorMongoSubscriptionModel} it
     * wraps, which can answer with a moment somewhat before the call. So the subscription can also deliver what was
     * written that long before the call, as far back as {@code ReactorMongoSubscriptionModelFixture} declares for the
     * same reason.
     */
    @Override
    public Duration howFarBackANewSubscriptionMayStart() {
        return ReactorDurableMongoSubscriptionModelFixture.HOW_FAR_BACK_THE_REACTIVE_MONGO_MODEL_MAY_START;
    }

    /**
     * The durable model passes {@code globalCheckpoint()} straight through to the wrapped catch-up model, which in
     * turn passes it through to the underlying {@link ReactorMongoSubscriptionModel}, so the honest answer is what
     * that reports. Its {@code Mono} can complete empty when the server refuses {@code hostInfo}, which blocks to
     * null here.
     * <p>
     * Bounded rather than a bare {@code block()}, copying {@code BlockingSubscriptionOverReactive}'s twenty seconds
     * for this same call: it is one command against the store, so a model that has not answered by then is not going
     * to, and an unbounded block would hang the shard for its whole timeout instead of failing the test.
     */
    @Override
    public Checkpoint aCheckpointToStartFrom() {
        return SubscriptionModelFixture.orGlobalPositionZero(durableModel.globalCheckpoint().block(Duration.ofSeconds(20)));
    }

    @Override
    public void close() {
        durableModel.shutdown();
        // Delete documents rather than dropping either collection: dropping kills a live change stream, and
        // shutdown() above has only just asked this model's own stream to close.
        reactiveMongoTemplate.remove(new Query(), eventCollectionName).block();
        reactiveMongoTemplate.remove(new Query(), checkpointCollectionName).block();
    }
}
