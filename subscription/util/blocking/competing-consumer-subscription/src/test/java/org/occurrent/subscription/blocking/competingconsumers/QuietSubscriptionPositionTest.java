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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
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
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A subscription whose filter matches no event for a while still moves its position, so a process restart, a pause
 * or a lease handover doesn't go back to the position of the last event that did match.
 * <p>
 * The oplog of a test container can't be made to drop that position, so the tests that need lost history use a
 * {@code failCommand} fail point that answers the next {@code aggregate} with error code 286, and arm it only when
 * the position about to be read is the one of the last event that matched. The subscription models are told not to
 * restart after lost history, so a subscription that reads that position never delivers again.
 */
@Testcontainers
@Timeout(90)
@DisplayNameGeneration(ReplaceUnderscores.class)
class QuietSubscriptionPositionTest {

    private static final SubscriptionFilter NAME_DEFINED_ONLY = AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName()));
    private static final Duration QUIET_POSITION_INTERVAL = Duration.ofMillis(200);

    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    enum Model {SPRING, NATIVE}

    private final List<SubscriptionModel> started = new ArrayList<>();
    private final CopyOnWriteArrayList<BsonDocument> changeStreamsOpened = new CopyOnWriteArrayList<>();
    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoEventStore eventStore;
    private String eventCollection;
    private SpringMongoCheckpointStorage storage;

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        eventCollection = "events-" + UUID.randomUUID();
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (event.getCommandName().equals("aggregate") && event.getCommand().get("aggregate").isString() && event.getCommand().getString("aggregate").getValue().equals(eventCollection)) {
                    BsonValue firstStage = event.getCommand().getArray("pipeline").getFirst();
                    if (firstStage.asDocument().containsKey("$changeStream")) {
                        changeStreamsOpened.add(firstStage.asDocument().getDocument("$changeStream").clone());
                    }
                }
            }
        }).build());
        String database = requireNonNull(connectionString.getDatabase());
        template = new MongoTemplate(client, database);
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName(eventCollection)
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        storage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
    }

    @AfterEach
    void shutdown() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        started.forEach(SubscriptionModel::shutdown);
        client.close();
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void a_durable_subscription_that_matched_nothing_for_a_while_is_restarted_from_a_position_the_oplog_still_has(Model model) {
        // Given a process whose subscription handles one event and then matches nothing
        DurableSubscriptionModel firstProcess = durable(model);
        CopyOnWriteArrayList<CloudEvent> handledByTheFirstProcess = new CopyOnWriteArrayList<>();
        firstProcess.subscribe("quiet", NAME_DEFINED_ONLY, handledByTheFirstProcess::add).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined lastMatched = nameDefined();
        eventStore.write("matched", serialize(lastMatched));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handledByTheFirstProcess).extracting(CloudEvent::getId).containsExactly(lastMatched.eventId()));
        await().atMost(10, SECONDS).until(() -> storage.read("quiet") instanceof MongoResumeTokenCheckpoint);
        String positionOfLastMatched = requireNonNull(storage.read("quiet")).asString();
        eventStore.write("not-matched", serialize(nameWasChanged()));
        // The wait a quiet position is saved in, so a test without one fails on what the next process does
        await().atMost(5, SECONDS).pollDelay(Duration.ofSeconds(3)).until(() -> true);
        firstProcess.shutdown();

        // When the oplog has dropped the position of the last event that matched, and the next process starts
        NameDefined writtenWhileNoProcessRan = nameDefined();
        eventStore.write("written-while-no-process-ran", serialize(writtenWhileNoProcessRan));
        if (positionOfLastMatched.equals(requireNonNull(storage.read("quiet")).asString())) {
            historyLostOnNextOpen();
        }
        DurableSubscriptionModel secondProcess = durable(model);
        CopyOnWriteArrayList<CloudEvent> handledByTheSecondProcess = new CopyOnWriteArrayList<>();
        secondProcess.subscribe("quiet", NAME_DEFINED_ONLY, handledByTheSecondProcess::add);

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handledByTheSecondProcess).as("handled by the process that started after the quiet period")
                .extracting(CloudEvent::getId).containsExactly(writtenWhileNoProcessRan.eventId()));
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void a_paused_subscription_that_matched_nothing_for_a_while_resumes_from_its_quiet_position(Model model) {
        // Given
        CheckpointAwareSubscriptionModel subscriptionModel = model(model);
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        QuietPositionReportingSubscriptions.findIn(subscriptionModel).orElseThrow().addQuietPositionListener(subscriptionId -> quietPositions::add);
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("quiet", NAME_DEFINED_ONLY, StartAt.now(), handled::add).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined lastMatched = nameDefined();
        eventStore.write("matched", serialize(lastMatched));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(lastMatched.eventId()));
        BsonDocument positionOfLastMatched = ((MongoResumeTokenCheckpoint) CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(handled.getFirst())).resumeToken;
        eventStore.write("not-matched", serialize(nameWasChanged()));
        int reportedBeforeTheQuietPeriod = quietPositions.size();
        await().atMost(10, SECONDS).until(() -> quietPositions.size() > reportedBeforeTheQuietPeriod + 2);

        // When
        subscriptionModel.pauseSubscription("quiet");
        changeStreamsOpened.clear();
        NameDefined writtenWhilePaused = nameDefined();
        eventStore.write("written-while-paused", serialize(writtenWhilePaused));
        subscriptionModel.resumeSubscription("quiet").waitUntilStarted(Duration.ofSeconds(10));

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(lastMatched.eventId(), writtenWhilePaused.eventId()));
        assertThat(changeStreamsOpened).hasSize(1);
        BsonDocument resumedFrom = changeStreamsOpened.getFirst().getDocument("startAfter");
        assertThat(resumedFrom).as("the position the resume opened the change stream at").isNotEqualTo(positionOfLastMatched);
        assertThat(quietPositions).extracting(quietPosition -> ((MongoResumeTokenCheckpoint) quietPosition).resumeToken).contains(resumedFrom);
    }

    @Test
    void the_node_that_takes_over_a_subscription_that_matched_nothing_for_a_while_starts_from_a_position_the_oplog_still_has() {
        // Given a node that holds the lease, handles one event and then matches nothing
        String locks = "locks-" + UUID.randomUUID();
        SpringMongoLeaseCompetingConsumerStrategy strategyA = leaseStrategy(locks);
        SpringMongoLeaseCompetingConsumerStrategy strategyB = leaseStrategy(locks);
        CompetingConsumerSubscriptionModel nodeA = node(strategyA);
        CompetingConsumerSubscriptionModel nodeB = node(strategyB);
        CopyOnWriteArrayList<CloudEvent> handledByA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> handledByB = new CopyOnWriteArrayList<>();
        nodeA.subscribe("quiet", NAME_DEFINED_ONLY, handledByA::add).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined lastMatched = nameDefined();
        eventStore.write("matched", serialize(lastMatched));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handledByA).extracting(CloudEvent::getId).containsExactly(lastMatched.eventId()));
        await().atMost(10, SECONDS).until(() -> storage.read("quiet") instanceof MongoResumeTokenCheckpoint);
        String positionOfLastMatched = requireNonNull(storage.read("quiet")).asString();
        OptionalLong tokenA = strategyA.fencingToken("quiet");
        assertThat(tokenA).isPresent();
        eventStore.write("not-matched", serialize(nameWasChanged()));
        await().atMost(5, SECONDS).pollDelay(Duration.ofSeconds(3)).until(() -> true);
        assertThat(storage.writeVersion("quiet")).as("the lease version the stored position was written with").isEqualTo(tokenA);

        // When node A gives the lease up, the oplog has dropped the position of the last event that matched, and
        // node B takes the lease
        nodeA.pauseSubscription("quiet");
        NameDefined writtenDuringTheHandover = nameDefined();
        eventStore.write("written-during-the-handover", serialize(writtenDuringTheHandover));
        if (positionOfLastMatched.equals(requireNonNull(storage.read("quiet")).asString())) {
            historyLostOnNextOpen();
        }
        nodeB.subscribe("quiet", NAME_DEFINED_ONLY, handledByB::add);

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handledByB).as("handled by the node that took the lease over")
                .extracting(CloudEvent::getId).containsExactly(writtenDuringTheHandover.eventId()));
        assertThat(handledByA).extracting(CloudEvent::getId).containsExactly(lastMatched.eventId());
    }

    @Test
    void a_node_whose_lease_was_taken_over_cannot_save_its_quiet_position_and_stops_delivering() {
        // Given a node that holds the lease and matches nothing, and a lease time long enough that it never learns
        // from a refresh that the lease is gone
        String locks = "locks-" + UUID.randomUUID();
        SpringMongoLeaseCompetingConsumerStrategy strategyA = leaseStrategy(locks);
        SpringMongoLeaseCompetingConsumerStrategy strategyB = leaseStrategy(locks);
        CompetingConsumerSubscriptionModel nodeA = node(strategyA);
        CompetingConsumerSubscriptionModel nodeB = node(strategyB);
        CopyOnWriteArrayList<CloudEvent> handledByA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> handledByB = new CopyOnWriteArrayList<>();
        nodeA.subscribe("quiet", NAME_DEFINED_ONLY, handledByA::add).waitUntilStarted(Duration.ofSeconds(10));
        OptionalLong tokenA = strategyA.fencingToken("quiet");
        assertThat(tokenA).isPresent();
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(storage.writeVersion("quiet")).isEqualTo(tokenA));

        // When the lease expires without node A noticing, and node B takes it
        template.getCollection(locks).updateOne(eq("_id", "quiet"), set("expiresAt", Instant.now().minusSeconds(2)));
        nodeB.subscribe("quiet", NAME_DEFINED_ONLY, handledByB::add).waitUntilStarted(Duration.ofSeconds(10));
        OptionalLong tokenB = strategyB.fencingToken("quiet");
        assertThat(tokenB.orElseThrow()).isGreaterThan(tokenA.getAsLong());
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(storage.writeVersion("quiet")).isEqualTo(tokenB));

        // Then node A's saves are refused, so the stored version stays node B's
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6)).untilAsserted(() -> assertThat(storage.writeVersion("quiet")).isEqualTo(tokenB));
        // and the refusal ended node A's delivery, so an event written now is handled by node B alone
        NameDefined writtenAfterTheTakeover = nameDefined();
        eventStore.write("written-after-the-takeover", serialize(writtenAfterTheTakeover));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handledByB).extracting(CloudEvent::getId).containsExactly(writtenAfterTheTakeover.eventId()));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handledByA).as("handled by the node that lost the lease").isEmpty());
    }

    private SpringMongoLeaseCompetingConsumerStrategy leaseStrategy(String locks) {
        return new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(60)).collectionName(locks).build();
    }

    private CompetingConsumerSubscriptionModel node(SpringMongoLeaseCompetingConsumerStrategy strategy) {
        DurableSubscriptionModel durable = new DurableSubscriptionModel(model(Model.SPRING), storage,
                new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(QUIET_POSITION_INTERVAL), strategy::fencingToken);
        CompetingConsumerSubscriptionModel node = new CompetingConsumerSubscriptionModel(durable, strategy);
        // Before the models it wraps, which the node shuts down itself
        started.addFirst(node);
        return node;
    }

    private DurableSubscriptionModel durable(Model model) {
        DurableSubscriptionModel durable = new DurableSubscriptionModel(model(model), storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(QUIET_POSITION_INTERVAL));
        started.addFirst(durable);
        return durable;
    }

    private CheckpointAwareSubscriptionModel model(Model model) {
        CheckpointAwareSubscriptionModel subscriptionModel = switch (model) {
            case SPRING -> new SpringMongoSubscriptionModel(template, SpringMongoSubscriptionModelConfig.withConfig(eventCollection, TimeRepresentation.RFC_3339_STRING)
                    .restartSubscriptionsOnChangeStreamHistoryLost(false).maxAwaitTime(Duration.ofMillis(100)));
            case NATIVE -> new NativeMongoSubscriptionModel(template.getDb(), eventCollection, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool(),
                    NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(false).maxAwaitTime(Duration.ofMillis(100)));
        };
        started.add(subscriptionModel);
        return subscriptionModel;
    }

    private void historyLostOnNextOpen() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", 286)));
    }

    private static NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
    }

    private static NameWasChanged nameWasChanged() {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "changed");
    }

    private static List<CloudEvent> serialize(DomainEvent event) {
        return List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(event.getClass().getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build());
    }
}
