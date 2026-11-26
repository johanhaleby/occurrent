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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * What a resume at a position on one of several instances loses or repeats, after the change stream history of every
 * instance is lost. Each instance is the catch-up over durable over MongoDB stack the blocking Spring Boot starter builds
 * below its competing consumer, with no competing consumer around it, so every instance runs the subscription. The
 * instances have a handler each and share one event store and one checkpoint storage, and all subscribe with the model
 * default start. The lost history is a {@code failCommand} fail point answering the {@code aggregate}
 * that opens the change stream with error code 286, after the probe that finds the stored position readable. T is the
 * position of the last event every instance delivered before that, still in the oplog, which the application keeps as
 * a {@link StringBasedCheckpoint}. An event is lost when no instance delivers it and a restart of every instance from
 * the stored checkpoint would not deliver it either, and repeated when an instance delivers it twice or a restart
 * delivers it again because the stored checkpoint went back.
 */
@Testcontainers
@DisplayName("ResumeAtAPositionOnOneInstanceOfSeveralAfterHistoryLost")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ResumeAtAPositionOnOneInstanceOfSeveralAfterHistoryLostTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private static final String SUBSCRIPTION_APP = "history-lost-instance";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private MongoClient mongoClient;
    private SpringMongoEventStore eventStore;
    private SpringMongoCheckpointStorage checkpointStorage;
    private String streamId;
    private String subscriptionId;
    private int written;
    private final List<Instance> instances = new ArrayList<>();
    // The event whose checkpoint each token stored for the subscription is
    private final Map<String, String> eventOfToken = new HashMap<>();
    private Instance a;
    private Instance b;

    @BeforeEach
    void one_event_store_and_one_checkpoint_storage() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager transactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        eventStore = new SpringMongoEventStore(mongoTemplate, new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection())
                .transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        streamId = UUID.randomUUID().toString();
        subscriptionId = UUID.randomUUID().toString();
        checkpointStorage = new SpringMongoCheckpointStorage(mongoTemplate, "checkpoints-" + UUID.randomUUID());
    }

    @AfterEach
    void shutdown() {
        try {
            historyLostOff();
            instances.forEach(Instance::shutdown);
        } finally {
            mongoClient.close();
        }
    }

    @Test
    @Timeout(120)
    void a_resume_at_a_position_on_one_of_two_paused_instances_loses_no_event_and_leaves_the_other_paused_with_its_handler_missing_what_the_first_delivered() {
        // Given two instances paused after their history is lost
        Checkpoint t = twoInstancesPausedAfterTheirHistoryIsLost();

        // When A alone is resumed at T, with an event written while both were paused and two after
        historyLostOff();
        write("gap");
        a.resumeAt(t);
        awaitCheckpointOfEventStored(a, "gap");
        write("w1");
        awaitCheckpointOfEventStored(a, "w1");
        write("w2");
        awaitCheckpointOfEventStored(a, "w2");
        Throwable resumeOfB = catchThrowable(() -> b.catchup.resumeSubscription(subscriptionId));
        quiet();

        // Then A delivers the three events, since T is the position before the first of them, so none is lost
        // And the checkpoint is the position of the last of them, which a restart of every instance resumes from
        // And B delivers none of them, stays paused, and still refuses to resume
        assertThat(a.ids()).as("A resumed at T, the position of the last event delivered before the loss, so it delivers every event written after T, the one written while paused too").containsExactly("gap", "w1", "w2");
        assertThat(label(checkpointStorage.read(subscriptionId))).as("A stores the position of each event it delivers in the one checkpoint every instance reads, so a restart of every instance resumes after w2").isEqualTo("token of w2");
        assertThat(b.ids()).as("B is paused, since the resume was on A only, so it delivers nothing").isEmpty();
        assertThat(b.catchup.isPaused(subscriptionId)).as("B is paused until it is resumed or cancelled").isTrue();
        assertThat(resumeOfB).as("B recorded its own loss and a resume on A does not clear it").isInstanceOf(CatchupCannotResumeException.class);
    }

    @Test
    @Timeout(120)
    void a_restart_of_the_paused_instance_after_a_resume_at_a_position_on_the_other_continues_from_what_the_other_stored_and_misses_what_the_other_delivered() {
        // Given A resumed at T alone and having delivered an event written while both were paused and two after
        Checkpoint t = twoInstancesPausedAfterTheirHistoryIsLost();
        historyLostOff();
        write("gap");
        a.resumeAt(t);
        awaitCheckpointOfEventStored(a, "gap");
        write("w1");
        awaitCheckpointOfEventStored(a, "w1");
        write("w2");
        awaitCheckpointOfEventStored(a, "w2");

        // When B is restarted and an event is written
        b.shutdown();
        Instance restartedB = new Instance("B");
        restartedB.start();
        write("w3");
        awaitCheckpointOfEventStored(a, "w3");
        await("w3 is delivered to both").atMost(10, SECONDS).untilAsserted(() -> {
            assertThat(restartedB.ids()).contains("w3");
            assertThat(a.ids()).contains("w3");
        });
        quiet();

        // Then B starts from the checkpoint A stored, which is the position of w2, so it delivers w3 only
        // And what A delivered while B was paused is never delivered to B's handler
        assertThat(restartedB.ids()).as("B reads the one checkpoint every instance reads, which A stored at the position of w2, so it starts after w2 and gap, w1 and w2 are never delivered to B's handler").containsExactly("w3");
        assertThat(a.ids()).as("A delivers every event once, in steady state").containsExactly("gap", "w1", "w2", "w3");
        assertThat(label(checkpointStorage.read(subscriptionId))).as("both instances store the position of w3").isEqualTo("token of w3");
    }

    @Test
    @Timeout(120)
    void a_resume_at_the_same_position_on_the_other_instance_stores_the_position_again_so_a_restart_of_the_first_delivers_what_it_delivered_before() {
        // Given A resumed at T alone and having delivered an event written while both were paused and two after
        Checkpoint t = twoInstancesPausedAfterTheirHistoryIsLost();
        historyLostOff();
        write("gap");
        a.resumeAt(t);
        awaitCheckpointOfEventStored(a, "gap");
        write("w1");
        awaitCheckpointOfEventStored(a, "w1");
        write("w2");
        awaitCheckpointOfEventStored(a, "w2");

        // When B is resumed at T and its handler is held on the first event it replays, then A is restarted
        b.holdItsHandler();
        b.resumeAt(t);
        assertThat(label(checkpointStorage.read(subscriptionId))).as("a resume at a position stores the position as the checkpoint of the subscription, and B's handler is held before it stores anything after it, so the checkpoint went back from w2 to T").isEqualTo("token of e2");
        a.shutdown();
        Instance restartedA = new Instance("A");
        restartedA.start();
        await("the restarted A delivers what the stored checkpoint leads to").atMost(10, SECONDS).untilAsserted(() -> assertThat(restartedA.ids()).hasSize(3));
        b.releaseItsHandler();
        await("B delivers what it replays").atMost(10, SECONDS).untilAsserted(() -> assertThat(b.ids()).containsExactly("gap", "w1", "w2"));
        quiet();

        // Then the checkpoint went back to T when B was resumed, behind what A had stored, and the restarted A resumed from it
        // And A delivered gap, w1 and w2 a second time
        assertThat(restartedA.ids()).as("the restarted A resumes from the checkpoint B's resume put back at T, so it delivers gap, w1 and w2 a second time").containsExactly("gap", "w1", "w2");
        assertThat(b.ids()).as("B replays everything after T, which it would have missed, so its handler gets each of them once").containsExactly("gap", "w1", "w2");
    }

    @Test
    @Timeout(120)
    void a_resume_at_the_same_position_on_the_other_instance_replays_what_it_missed_and_ends_with_the_checkpoint_at_the_last_event_so_a_restart_of_the_first_delivers_nothing_again() {
        // Given A resumed at T alone and having delivered an event written while both were paused and two after
        Checkpoint t = twoInstancesPausedAfterTheirHistoryIsLost();
        historyLostOff();
        write("gap");
        a.resumeAt(t);
        awaitCheckpointOfEventStored(a, "gap");
        write("w1");
        awaitCheckpointOfEventStored(a, "w1");
        write("w2");
        awaitCheckpointOfEventStored(a, "w2");

        // When B is resumed at T and has delivered what it replays, then A is restarted and an event is written
        b.resumeAt(t);
        await("B delivers what it replays").atMost(10, SECONDS).untilAsserted(() -> assertThat(b.ids()).containsExactly("gap", "w1", "w2"));
        await("the checkpoint is at w2 again").atMost(10, SECONDS).until(() -> label(checkpointStorage.read(subscriptionId)).equals("token of w2"));
        a.shutdown();
        Instance restartedA = new Instance("A");
        restartedA.start();
        write("w3");
        awaitCheckpointOfEventStored(b, "w3");
        await("w3 is delivered to both").atMost(10, SECONDS).untilAsserted(() -> {
            assertThat(restartedA.ids()).contains("w3");
            assertThat(b.ids()).contains("w3");
        });

        // Then the restarted A resumes from the position of w2, so it delivers w3 only
        assertThat(restartedA.ids()).as("B stored the position of each event it replayed, so the checkpoint is at w2 again and the restarted A starts after it").containsExactly("w3");
        assertThat(b.ids()).as("B delivers w3 live after what it replayed").containsExactly("gap", "w1", "w2", "w3");
    }

    @Test
    @Timeout(120)
    void a_cancel_of_the_paused_instance_deletes_the_checkpoint_the_other_instance_reads_until_the_other_stores_its_next_event() {
        // Given A resumed at T alone and having delivered an event written while both were paused and two after
        Checkpoint t = twoInstancesPausedAfterTheirHistoryIsLost();
        historyLostOff();
        write("gap");
        a.resumeAt(t);
        awaitCheckpointOfEventStored(a, "gap");
        write("w1");
        awaitCheckpointOfEventStored(a, "w1");
        write("w2");
        awaitCheckpointOfEventStored(a, "w2");

        // When B is cancelled and an event is written
        b.catchup.cancelSubscription(subscriptionId);
        String storedRightAfterTheCancel = label(checkpointStorage.read(subscriptionId));
        write("w3");
        awaitCheckpointOfEventStored(a, "w3");
        quiet();

        // Then the checkpoint is gone right after the cancel, and A stores one again for the next event it delivers
        assertThat(storedRightAfterTheCancel).as("a cancel deletes the checkpoint stored for the subscription id, which A reads as well, so a restart of every instance at this point starts from now and skips what is written meanwhile").isEqualTo("nothing");
        assertThat(label(checkpointStorage.read(subscriptionId))).as("A stores the position of w3, the next event it delivers, in the checkpoint B's cancel deleted").isEqualTo("token of w3");
        assertThat(a.ids()).as("A is not cancelled, so it delivers w3").containsExactly("gap", "w1", "w2", "w3");
        assertThat(b.catchup.isPaused(subscriptionId)).as("B no longer holds the subscription, so it is not paused any more, which it was before the cancel").isFalse();
        assertThat(b.ids()).as("B was paused while A delivered gap, w1 and w2, and cancelled before w3, so it delivered none of them").isEmpty();
    }

    @Test
    @Timeout(120)
    void with_one_instance_a_resume_at_a_position_loses_no_event_and_a_restart_continues_from_the_stored_checkpoint() {
        // Given one instance paused after its history is lost
        a = new Instance("A");
        a.start();
        write("e1");
        write("e2");
        await("e1 and e2 are delivered").atMost(10, SECONDS).untilAsserted(() -> assertThat(a.ids()).containsExactly("e1", "e2"));
        awaitCheckpointOfEventStored(a, "e2");
        Checkpoint t = new StringBasedCheckpoint(requireNonNull(checkpointStorage.read(subscriptionId)).asString());
        a.shutdown();
        a = new Instance("A");
        a.restartAfterLoss();

        // When it is resumed at T with an event written while it was paused and two after
        historyLostOff();
        write("gap");
        a.resumeAt(t);
        awaitCheckpointOfEventStored(a, "gap");
        write("w1");
        awaitCheckpointOfEventStored(a, "w1");
        write("w2");
        awaitCheckpointOfEventStored(a, "w2");
        a.shutdown();
        Instance restarted = new Instance("A");
        restarted.start();
        write("w3");
        awaitCheckpointOfEventStored(restarted, "w3");
        await("w3 is delivered").atMost(10, SECONDS).untilAsserted(() -> assertThat(restarted.ids()).contains("w3"));

        // Then it delivers the three events after T, and a restart continues after the last of them
        assertThat(a.ids()).as("the resume at T, the position of the last event delivered before the loss, delivers every event written after T").containsExactly("gap", "w1", "w2");
        assertThat(restarted.ids()).as("the checkpoint is at w2, the position of the last event A delivered, so a restart starts after it").containsExactly("w3");
    }

    // Both instances have delivered e1 and e2 live, which is steady state, and are shut down and started again with the
    // history of the position stored for them lost. Returns the position of e2
    private Checkpoint twoInstancesPausedAfterTheirHistoryIsLost() {
        a = new Instance("A");
        b = new Instance("B");
        a.start();
        b.start();
        write("e1");
        write("e2");
        await("both deliver e1 and e2").atMost(10, SECONDS).untilAsserted(() -> {
            assertThat(a.ids()).containsExactly("e1", "e2");
            assertThat(b.ids()).containsExactly("e1", "e2");
        });
        awaitCheckpointOfEventStored(a, "e2");
        Checkpoint t = new StringBasedCheckpoint(requireNonNull(checkpointStorage.read(subscriptionId)).asString());
        a.shutdown();
        b.shutdown();
        a = new Instance("A");
        b = new Instance("B");
        a.restartAfterLoss();
        b.restartAfterLoss();
        assertThat(a.catchup.isPaused(subscriptionId)).as("A is paused after losing its history").isTrue();
        assertThat(b.catchup.isPaused(subscriptionId)).as("B is paused after losing its history").isTrue();
        assertThat(catchThrowable(() -> a.catchup.resumeSubscription(subscriptionId))).as("A refuses to resume").isInstanceOf(CatchupCannotResumeException.class);
        assertThat(catchThrowable(() -> b.catchup.resumeSubscription(subscriptionId))).as("B refuses to resume").isInstanceOf(CatchupCannotResumeException.class);
        return t;
    }

    // Waits for the instance to deliver the event and for the stored checkpoint to be the one delivered with it, never for any checkpoint to exist, as one stored for an earlier event or a quiet position is not the event's
    private void awaitCheckpointOfEventStored(Instance instance, String eventId) {
        await("the checkpoint delivered with " + eventId + " is stored").atMost(10, SECONDS).until(() -> {
            Checkpoint stored = checkpointStorage.read(subscriptionId);
            return stored != null && instance.checkpointDeliveredWith(eventId).map(c -> c.asString().equals(stored.asString())).orElse(false);
        });
        eventOfToken.put(instance.checkpointDeliveredWith(eventId).orElseThrow().asString(), eventId);
    }

    private String label(Checkpoint checkpoint) {
        return checkpoint == null ? "nothing" : "token of " + eventOfToken.getOrDefault(checkpoint.asString(), "an unknown event");
    }

    // A window for an event that would arrive, so a count that stays the same is one that has settled
    private void quiet() {
        await().pollDelay(Duration.ofSeconds(2)).until(() -> true);
    }

    private void write(String id) {
        eventStore.write(streamId, List.of(CloudEventBuilder.v1().withId(id).withSource(URI.create("http://name")).withType(NameWasChanged.class.getName())
                .withTime(LocalDateTime.of(2026, 1, 1, 0, 0, written++).atOffset(UTC))
                .withSubject("name").withDataContentType("application/json")
                .withData(unchecked(OBJECT_MAPPER::writeValueAsBytes).apply(new NameWasChanged(id, LocalDateTime.of(2026, 1, 1, 0, 0), "name", "x"))).build()));
    }

    // The first aggregate of an app is the probe that finds the stored position readable, which passes, so the change stream's own opening at it fails
    private void historyLostOnEveryOpenAfterTheFirstBy(String appName) {
        mongoClient.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("skip", 1))
                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", 286).append("appName", appName)));
    }

    private void historyLostOff() {
        mongoClient.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
    }

    // One instance of the application, with its own client, MongoDB model, durable model, catch-up model and handler
    private final class Instance {
        private final String name;
        private final List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch handlerHeld = new CountDownLatch(0);
        private MongoClient client;
        private CatchupSubscriptionModel catchup;

        Instance(String name) {
            this.name = name;
        }

        void start() {
            build();
            catchup.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), this::deliver).waitUntilStarted();
        }

        void restartAfterLoss() {
            historyLostOnEveryOpenAfterTheFirstBy(appName());
            build();
            catchup.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), this::deliver);
            await(name + " is paused after losing its history").atMost(20, SECONDS).until(() -> catchup.isPaused(subscriptionId));
        }

        // The resume an application makes on a catch-up model it calls directly
        void resumeAt(Checkpoint position) {
            RepositionableSubscriptions.findIn(catchup).orElseThrow().resumeSubscription(subscriptionId, StartAt.checkpoint(position));
        }

        void holdItsHandler() {
            handlerHeld = new CountDownLatch(1);
        }

        void releaseItsHandler() {
            handlerHeld.countDown();
        }

        List<String> ids() {
            return delivered.stream().map(CloudEvent::getId).toList();
        }

        Optional<Checkpoint> checkpointDeliveredWith(String eventId) {
            return delivered.stream().filter(e -> e.getId().equals(eventId)).findFirst().map(CheckpointAwareCloudEvent::getCheckpointOrThrowIAE);
        }

        void shutdown() {
            releaseItsHandler();
            if (catchup != null) {
                catchup.shutdown();
            }
            catchup = null;
            if (client != null) {
                client.close();
                client = null;
            }
        }

        private void deliver(CloudEvent event) {
            unchecked((CountDownLatch latch) -> latch.await(30, SECONDS)).apply(handlerHeld);
            delivered.add(event);
        }

        private String appName() {
            return SUBSCRIPTION_APP + "-" + name;
        }

        private void build() {
            ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
            client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).applicationName(appName()).build());
            MongoTemplate template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
            SpringMongoSubscriptionModel mongo = new SpringMongoSubscriptionModel(template, requireNonNull(connectionString.getCollection()), TimeRepresentation.RFC_3339_STRING);
            DurableSubscriptionModel durable = new DurableSubscriptionModel(mongo, checkpointStorage);
            catchup = new CatchupSubscriptionModel(durable, eventStore, new CatchupSubscriptionModelConfig(useCheckpointStorage(checkpointStorage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1000)));
            instances.add(this);
        }
    }
}
