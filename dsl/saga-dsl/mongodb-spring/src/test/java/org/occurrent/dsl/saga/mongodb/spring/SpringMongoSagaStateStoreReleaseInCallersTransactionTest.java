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

package org.occurrent.dsl.saga.mongodb.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonValue;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaReadRefusedException;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. {@code SagaSubscription.release} called inside a MongoDB transaction of the caller's, driven through a
 * real saga runner, throws the store's refusal as it is and sends no command to the instances or to the records of
 * skipped inputs, for an instance that exists and for one that doesn't. The release's decoding checks would read the
 * instance again in that transaction, and for an absent instance the store writes and deletes a document there to read it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreReleaseInCallersTransactionTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-release-in-callers-transaction";
    private static final String SKIPPED_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item) implements OrderEvent {
    }

    record PackCommand(String item) {
    }

    // The command name and the collection it names, which every read and write of a collection has
    private record Sent(String command, String collection) {
    }

    private final List<Sent> sent = new CopyOnWriteArrayList<>();
    private final List<PackCommand> dispatched = new CopyOnWriteArrayList<>();
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private MongoClient client;
    private MongoTemplate template;
    private SagaStateStore<String> stateStore;
    private @Nullable SagaSubscription subscription;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-release-tx-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                BsonValue target = event.getCommand().get(event.getCommandName());
                sent.add(new Sent(event.getCommandName(), target != null && target.isString() ? target.asString().getValue() : ""));
            }
        }).build());
        template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(template, COLLECTION, String.class);
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
        client.close();
    }

    private Saga<OrderEvent, String, PackCommand> saga() {
        return Saga.<OrderEvent, String, PackCommand>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .evolve(ItemAdded.class, (state, e) -> "packing")
                .react(ItemAdded.class, (state, e) -> {
                    if (failing.contains(e.item())) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new PackCommand(e.item())));
                })
                .build();
    }

    private SagaSubscription run() {
        subscription = SagaRunner.<OrderEvent, PackCommand>agnostic(subscriptionModel, converter)
                .run(STREAM, saga(), stateStore, dispatched::add, null,
                        SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        return subscription;
    }

    private void write(OrderEvent event) {
        eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
    }

    private @Nullable Document stored() {
        return template.findOne(Query.query(where("_id").is(ORDER)), Document.class, COLLECTION);
    }

    private void aQuarantinedInstance() {
        write(new OrderPlaced(ORDER));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(stored()).isNotNull());
        failing.add("book");
        write(new ItemAdded(ORDER, "book"));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(stored())
                .as("the instance is quarantined on the input that keeps failing").isNotNull().extracting(document -> document.get("status")).isEqualTo("QUARANTINED_RECORDING"));
        failing.clear();
    }

    private Throwable releaseInATransactionOfTheCaller(SagaSubscription running, String sagaId) {
        TransactionTemplate callersTransaction = new TransactionTemplate(new MongoTransactionManager(template.getMongoDatabaseFactory()));
        sent.clear();
        return catchThrowable(() -> callersTransaction.executeWithoutResult(status -> running.release(sagaId)));
    }

    private List<Sent> sentToTheInstancesOrTheirRecords() {
        return sent.stream().filter(s -> s.collection().equals(COLLECTION) || s.collection().equals(SKIPPED_COLLECTION)).toList();
    }

    @Test
    void a_release_of_a_quarantined_instance_inside_a_transaction_of_the_caller_is_refused_as_it_is_and_sends_nothing_to_the_instances_or_their_records() {
        SagaSubscription running = run();
        aQuarantinedInstance();
        Document before = stored();

        Throwable thrown = releaseInATransactionOfTheCaller(running, ORDER);

        assertAll(
                () -> assertThat(thrown).isInstanceOf(SagaReadRefusedException.class).hasMessage(SpringMongoSagaStateStore.CALLERS_TRANSACTION_MESSAGE),
                () -> assertThat(sentToTheInstancesOrTheirRecords()).as("commands sent to the instances or their records while the release ran").isEmpty(),
                () -> assertThat(stored()).as("the instance is stored as it was, at its version and still quarantined").isEqualTo(before),
                () -> assertThat(dispatched).as("the release replayed nothing").isEmpty()
        );
    }

    @Test
    void a_release_of_an_absent_instance_inside_a_transaction_of_the_caller_is_refused_as_it_is_and_sends_nothing_to_the_instances_or_their_records() {
        SagaSubscription running = run();

        Throwable thrown = releaseInATransactionOfTheCaller(running, "nobody");

        assertAll(
                () -> assertThat(thrown).isInstanceOf(SagaReadRefusedException.class).hasMessage(SpringMongoSagaStateStore.CALLERS_TRANSACTION_MESSAGE),
                () -> assertThat(sentToTheInstancesOrTheirRecords()).as("commands sent to the instances or their records while the release ran").isEmpty(),
                () -> assertThat(template.findOne(Query.query(where("_id").is("nobody")), Document.class, COLLECTION)).as("no document left for the absent id").isNull()
        );
    }
}
