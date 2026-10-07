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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A subscription restarted because the change stream history for its stored checkpoint is gone records the position
 * it restarted from, so the next process start resumes from there. Otherwise that process reads the lost position
 * again, restarts from its own present, and skips every event written while nothing ran. The lost history is a
 * {@code failCommand} fail point answering the {@code aggregate} that opens the change stream with error code 286.
 * A reply to {@code ping} without an operation time gives no present to record, so the subscription opens at no
 * position other than the stored one until a reply has one. A subscription the durable model stores no checkpoint for
 * restarts from the present all the same.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionHistoryLostRestartTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private static final String SUBSCRIPTION_APP = "history-lost-subscription";
    private static final BsonTimestamp LOST = new BsonTimestamp(1, 0);

    private MongoClient client;
    private @Nullable MongoClient subscriptionClient;
    private MongoTemplate template;
    private SpringMongoEventStore eventStore;
    private DurableSubscriptionModel running;

    @AfterEach
    void shutdown() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        if (running != null) running.shutdown();
        if (subscriptionClient != null) subscriptionClient.close();
    }

    @Test
    void events_written_after_a_history_lost_restart_survive_a_process_restart() {
        connect();
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
        String t0 = checkpointOneEventInProcessOne(storage).asString();

        // Process 2 starts after T0 fell off the oplog, so its change stream history is lost and it restarts
        historyLostOnNextOpen();
        running = durable(storage);
        running.subscribe("X", __ -> {
        }).waitUntilStarted();
        await("the position process 2 restarted from is stored").atMost(5, SECONDS).until(() -> !t0.equals(storage.read("X").asString()));
        running.shutdown();

        // Written while no process runs, after process 2 had already restarted past T0
        String eDown = write();

        // Process 3 starts. Only T0 is gone from the oplog, so the history is lost again only if T0 is what it reads.
        if (t0.equals(storage.read("X").asString())) {
            historyLostOnNextOpen();
        }
        CopyOnWriteArrayList<CloudEvent> p3 = new CopyOnWriteArrayList<>();
        running = durable(storage);
        running.subscribe("X", p3::add).waitUntilStarted();
        String eAfter = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(p3).extracting(CloudEvent::getId).contains(eAfter));

        assertThat(p3).as("the event written while no process ran is still in the oplog and must be delivered").extracting(CloudEvent::getId).contains(eDown);
    }

    @Test
    void a_restart_after_lost_history_opens_at_no_position_but_the_stored_one_until_the_reply_to_ping_has_an_operation_time() {
        // Given
        ConnectionString cs = connect();
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
        Checkpoint t0 = checkpointOneEventInProcessOne(storage);
        List<Opening> openings = new CopyOnWriteArrayList<>();
        AtomicBoolean pingWithoutOperationTime = new AtomicBoolean(true);
        AtomicInteger pingsWithoutOperationTime = new AtomicInteger();
        MongoClient subscriptions = MongoClients.create(MongoClientSettings.builder().applyConnectionString(cs).applicationName(SUBSCRIPTION_APP)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        BsonDocument changeStream = changeStreamStage(event);
                        if (changeStream != null) {
                            openings.add(new Opening(openedAt(changeStream), storage.read("X")));
                        }
                    }
                }).build());
        subscriptionClient = subscriptions;
        MongoTemplate subscriptionTemplate = new MongoTemplate(subscriptions, requireNonNull(cs.getDatabase())) {
            @Override
            public Document executeCommand(Document command) {
                Document reply = super.executeCommand(command);
                if (command.containsKey("ping") && pingWithoutOperationTime.get()) {
                    reply.remove("operationTime");
                    pingsWithoutOperationTime.incrementAndGet();
                }
                return reply;
            }
        };
        historyLostOnEveryOpenBy(SUBSCRIPTION_APP);
        CopyOnWriteArrayList<CloudEvent> p2 = new CopyOnWriteArrayList<>();
        running = durable(storage, subscriptionTemplate);

        // When
        Subscription subscription = running.subscribe("X", p2::add);
        await("the restart asks for the present again").atMost(10, SECONDS).until(() -> pingsWithoutOperationTime.get() >= 3);

        // Then
        assertThat(storage.read("X")).as("the stored position while no reply to ping has an operation time").isEqualTo(t0);
        assertThat(openings).as("the positions the change stream opened at while no reply to ping has an operation time, null for the present")
                .isNotEmpty().allSatisfy(opening -> assertThat(opening.at()).isEqualTo(t0));

        // When
        pingWithoutOperationTime.set(false);
        await("the position the subscription restarts from is stored").atMost(10, SECONDS).until(() -> !t0.equals(storage.read("X")));
        historyLostOff();

        // Then
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(10))).as("started").isTrue();
        assertThat(openings).as("each position the change stream opened at, against the position stored as it opened")
                .allSatisfy(opening -> assertThat(opening.at()).isEqualTo(opening.stored()));
        assertThat(openings.getLast().at()).as("the position the change stream finally opened at").isInstanceOf(MongoOperationTimeCheckpoint.class);
        String eAfter = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(p2).extracting(CloudEvent::getId).contains(eAfter));
    }

    @Test
    void a_subscription_made_on_the_wrapped_model_restarts_from_the_present_after_lost_history_when_the_reply_to_ping_has_no_operation_time() {
        // Given
        connect();
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
        running = durable(storage, pingWithoutOperationTimeAndHistoryLostAt(LOST));
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();

        // When
        Subscription subscription = running.getWrappedSubscriptionModel().subscribe("X", null, StartAt.checkpoint(new MongoOperationTimeCheckpoint(LOST)), received::add);

        // Then
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(10))).as("restarted after lost history").isTrue();
        String eAfter = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(received).extracting(CloudEvent::getId).contains(eAfter));
        assertThat(storage.read("X")).as("checkpoint stored").isNull();
    }

    @Test
    void a_subscription_the_durable_model_stores_no_checkpoint_for_restarts_from_the_present_after_lost_history_when_the_reply_to_ping_has_no_operation_time() {
        // Given
        connect();
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
        running = durable(storage, pingWithoutOperationTimeAndHistoryLostAt(LOST));
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        // The durable model gets null from it and hands the subscription to the wrapped model, which then gets the
        // lost position from it
        AtomicInteger evaluations = new AtomicInteger();
        StartAt nothingThenLost = StartAt.dynamic(() -> evaluations.getAndIncrement() == 0 ? null : StartAt.checkpoint(new MongoOperationTimeCheckpoint(LOST)));

        // When
        Subscription subscription = running.subscribe("X", null, nothingThenLost, received::add);

        // Then
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(10))).as("restarted after lost history").isTrue();
        String eAfter = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(received).extracting(CloudEvent::getId).contains(eAfter));
        assertThat(storage.read("X")).as("checkpoint stored").isNull();
    }

    private ConnectionString connect() {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(cs);
        template = new MongoTemplate(client, requireNonNull(cs.getDatabase()));
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, requireNonNull(cs.getDatabase()))))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        return cs;
    }

    // Process 1 checkpoints an event at a token, the position the next process finds stored
    private Checkpoint checkpointOneEventInProcessOne(SpringMongoCheckpointStorage storage) {
        CopyOnWriteArrayList<CloudEvent> p1 = new CopyOnWriteArrayList<>();
        running = durable(storage);
        running.subscribe("X", p1::add).waitUntilStarted();
        String e0 = write();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(p1).extracting(CloudEvent::getId).contains(e0));
        await().pollDelay(Duration.ofMillis(300)).until(() -> true);
        Checkpoint t0 = storage.read("X");
        running.shutdown();
        return t0;
    }

    private DurableSubscriptionModel durable(SpringMongoCheckpointStorage storage) {
        return durable(storage, template);
    }

    private DurableSubscriptionModel durable(SpringMongoCheckpointStorage storage, MongoTemplate subscriptionTemplate) {
        return new DurableSubscriptionModel(new SpringMongoSubscriptionModel(subscriptionTemplate,
                SpringMongoSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true)
                        .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100)))), storage);
    }

    private void historyLostOnEveryOpenBy(String appName) {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "alwaysOn")
                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", 286).append("appName", appName)));
    }

    private void historyLostOff() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
    }

    // A copy, since the event's command is only readable while the listener runs
    private static @Nullable BsonDocument changeStreamStage(CommandStartedEvent event) {
        if (!event.getCommandName().equals("aggregate") || !event.getCommand().isArray("pipeline")) {
            return null;
        }
        return event.getCommand().getArray("pipeline").stream().findFirst()
                .filter(stage -> stage.isDocument() && stage.asDocument().isDocument("$changeStream"))
                .map(stage -> BsonDocument.parse(stage.asDocument().getDocument("$changeStream").toJson()))
                .orElse(null);
    }

    // null for a change stream that opens at the present
    private static @Nullable Checkpoint openedAt(BsonDocument changeStream) {
        if (changeStream.isDocument("startAfter")) {
            return new MongoResumeTokenCheckpoint(changeStream.getDocument("startAfter"));
        } else if (changeStream.isDocument("resumeAfter")) {
            return new MongoResumeTokenCheckpoint(changeStream.getDocument("resumeAfter"));
        } else if (changeStream.isTimestamp("startAtOperationTime")) {
            return new MongoOperationTimeCheckpoint(changeStream.getTimestamp("startAtOperationTime"));
        }
        return null;
    }

    private record Opening(@Nullable Checkpoint at, @Nullable Checkpoint stored) {
    }

    // Every reply to ping has no operation time, and a change stream told to open at lostAt fails with history lost, as
    // MongoDB answers once the oplog has dropped that position
    private MongoTemplate pingWithoutOperationTimeAndHistoryLostAt(BsonTimestamp lostAt) {
        return new MongoTemplate(client, template.getDb().getName()) {
            @Override
            public Document executeCommand(Document command) {
                Document reply = super.executeCommand(command);
                if (command.containsKey("ping")) {
                    reply.remove("operationTime");
                }
                return reply;
            }

            @Override
            public MongoDatabase getDb() {
                return proxy(MongoDatabase.class, super.getDb(), (method, args, result) -> method.getName().equals("getCollection")
                        ? proxy(MongoCollection.class, result, (m, a, r) -> m.getName().equals("watch") ? changeStreamLostAt(lostAt, r, false) : r)
                        : result);
            }
        };
    }

    private static Object changeStreamLostAt(BsonTimestamp lostAt, Object changeStream, boolean opensAtLostAt) {
        return proxy(ChangeStreamIterable.class, changeStream, (method, args, result) -> {
            if (method.getName().equals("startAtOperationTime")) {
                return changeStreamLostAt(lostAt, result, lostAt.equals(args[0]));
            } else if (opensAtLostAt && (method.getName().equals("cursor") || method.getName().equals("iterator"))) {
                throw new MongoCommandException(new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(286))
                        .append("codeName", new BsonString("ChangeStreamHistoryLost")), new ServerAddress());
            }
            return result instanceof ChangeStreamIterable ? changeStreamLostAt(lostAt, result, opensAtLostAt) : result;
        });
    }

    @FunctionalInterface
    private interface AfterCall {
        Object apply(java.lang.reflect.Method method, Object[] args, Object result);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Object target, AfterCall afterCall) {
        return (T) java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
            Object result;
            try {
                result = method.invoke(target, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
            return afterCall.apply(method, args, result);
        });
    }

    private void historyLostOnNextOpen() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", 286)));
    }

    private String write() {
        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
        eventStore.write(UUID.randomUUID().toString(), List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
        return event.eventId();
    }
}
