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
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.filter.Filter.type;

/**
 * A durable model over {@link ReactorCatchupSubscriptionModel} over {@link ReactorMongoSubscriptionModel}, the model
 * the reactive Spring Boot starter builds, saves the quiet position the Mongo model reports. The Mongo model learns
 * the id of a subscription only when its replay has handed over, so nothing about the quiet position happens before
 * the replay has delivered the history.
 */
@Testcontainers
@Timeout(180)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableCatchupQuietPositionTest {

    private static final String DATABASE = "reactordurablecatchupquietposition";
    private static final String SUBSCRIPTION_ID = "sub";
    private static final int HISTORY = 20;
    private static final int QUIET_POSITIONS_TO_WAIT_FOR = 3;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REPLAY_ENDS_WITHIN = Duration.ofSeconds(60);
    private static final Duration QUIET_POSITIONS_ARE_REPORTED_WITHIN = Duration.ofSeconds(60);
    private static final SubscriptionFilter MATCHING_ONLY = AgnosticSubscriptionFilter.filter(type("Matching"));

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static MongoClient mongoClient;

    // A collection of its own for every test, since a subscription started at the present can also receive what was
    // written up to 16 seconds before it
    private final String eventCollectionName = "events-" + UUID.randomUUID();
    private final String checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
    private final CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ReactorDurableSubscriptionModel> models = new CopyOnWriteArrayList<>();
    private final ReactiveMongoTemplate template;
    private final ReactorMongoEventStore eventStore;
    private final ReactorCheckpointStorage checkpointStorage;
    private final OtherEvents otherEvents = new OtherEvents();

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    ReactorDurableCatchupQuietPositionTest() {
        template = new ReactiveMongoTemplate(mongoClient, DATABASE);
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, DATABASE)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        eventStore = new ReactorMongoEventStore(template, eventStoreConfig);
        checkpointStorage = new ReactorCheckpointStorage(template, checkpointCollectionName);
    }

    @AfterEach
    void shutdown() throws InterruptedException {
        otherEvents.stop();
        models.forEach(ReactorDurableSubscriptionModel::shutdown);
        template.remove(new Query(), eventCollectionName).block(TIMEOUT);
        template.remove(new Query(), checkpointCollectionName).block(TIMEOUT);
    }

    @Test
    void the_quiet_position_is_asked_for_and_reported_only_after_the_replay_has_delivered_the_history() throws InterruptedException {
        // Given
        writeMatchingEvents(HISTORY);
        RecordingMongoModel mongoModel = new RecordingMongoModel(template, eventCollectionName, delivered);
        ReactorDurableSubscriptionModel model = durableOverCatchup(mongoModel);
        CopyOnWriteArrayList<Checkpoint> quietPositions = quietPositionsOf(mongoModel);
        // A subscription that is live all along, so the Mongo model reads while the replay runs
        model.subscribe("live", AgnosticSubscriptionFilter.filter(type("Never")), StartAt.subscriptionModelDefault(), event -> Mono.empty()).waitUntilStarted(TIMEOUT).block();
        otherEvents.start();

        // When
        model.subscribe(SUBSCRIPTION_ID, MATCHING_ONLY, StartAt.checkpoint(GlobalCheckpoint.of(0)),
                event -> Mono.delay(Duration.ofMillis(150)).then(Mono.fromRunnable(() -> delivered.add(event))));

        // Then
        await().atMost(REPLAY_ENDS_WITHIN).until(() -> delivered.size() >= HISTORY);
        await().atMost(QUIET_POSITIONS_ARE_REPORTED_WITHIN).until(() -> quietPositions.size() >= QUIET_POSITIONS_TO_WAIT_FOR);
        assertThat(mongoModel.deliveredWhenAsked).as("events delivered each time the Mongo model asked the durable model about the subscription").isNotEmpty().allMatch(count -> count >= HISTORY);
        assertThat(mongoModel.deliveredWhenReported).as("events delivered each time the Mongo model reported a quiet position to the durable model").isNotEmpty().allMatch(count -> count >= HISTORY);
    }

    @Test
    void a_restart_from_a_quiet_position_saved_through_the_catch_up_model_delivers_an_event_written_while_it_was_down() throws InterruptedException {
        // Given
        writeMatchingEvents(HISTORY);
        RecordingMongoModel mongoModel = new RecordingMongoModel(template, eventCollectionName, delivered);
        ReactorDurableSubscriptionModel model = durableOverCatchup(mongoModel);
        CopyOnWriteArrayList<Checkpoint> quietPositions = quietPositionsOf(mongoModel);
        model.subscribe(SUBSCRIPTION_ID, MATCHING_ONLY, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> Mono.fromRunnable(() -> delivered.add(event)));
        await().atMost(REPLAY_ENDS_WITHIN).until(() -> delivered.size() >= HISTORY);
        otherEvents.start();
        await().atMost(QUIET_POSITIONS_ARE_REPORTED_WITHIN).until(() -> quietPositions.size() >= QUIET_POSITIONS_TO_WAIT_FOR);
        otherEvents.stop();
        model.shutdown();
        assertThat(mongoModel.positionsReported).as("quiet positions reported to the durable model").extracting(Checkpoint::asString).contains(storedPosition());

        // When
        String writtenWhileDown = writeMatchingEvents(1).getFirst();
        CopyOnWriteArrayList<CloudEvent> deliveredAfterRestart = new CopyOnWriteArrayList<>();
        ReactorDurableSubscriptionModel restarted = durableOverCatchup(new ReactorMongoSubscriptionModel(template, eventCollectionName, TimeRepresentation.RFC_3339_STRING));
        restarted.subscribe(SUBSCRIPTION_ID, MATCHING_ONLY, StartAt.subscriptionModelDefault(), event -> Mono.fromRunnable(() -> deliveredAfterRestart.add(event))).waitUntilStarted(TIMEOUT).block();

        // Then
        await().atMost(TIMEOUT).until(() -> !deliveredAfterRestart.isEmpty());
        assertThat(deliveredAfterRestart).as("events delivered after the restart from the quiet position").extracting(CloudEvent::getId).containsExactly(writtenWhileDown);
    }

    @Test
    void a_dual_mode_catch_up_model_has_the_listener_of_the_durable_model_added_once() {
        // Given
        RecordingMongoModel mongoModel = new RecordingMongoModel(template, eventCollectionName, delivered);

        // When
        models.add(new ReactorDurableSubscriptionModel(new ReactorCatchupSubscriptionModel(mongoModel, eventStore, eventStore, DcbCriteria.all(), Filter.all()), checkpointStorage, quickToSave()));

        // Then
        assertThat(mongoModel.added).as("listeners added to the Mongo model under a dual mode catch-up model").hasSize(1);
    }

    private ReactorDurableSubscriptionModel durableOverCatchup(ReactorMongoSubscriptionModel mongoModel) {
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new ReactorCatchupSubscriptionModel(mongoModel, eventStore, Filter.all()), checkpointStorage, quickToSave());
        models.add(model);
        return model;
    }

    private static ReactorDurableSubscriptionModelConfig quickToSave() {
        return new ReactorDurableSubscriptionModelConfig(1).saveQuietPositionEvery(Duration.ofMillis(200));
    }

    // The quiet positions the Mongo model reports for the subscription, to a listener of the test's own
    private static CopyOnWriteArrayList<Checkpoint> quietPositionsOf(RecordingMongoModel mongoModel) {
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        mongoModel.listenDirectly(subscriptionId -> SUBSCRIPTION_ID.equals(subscriptionId) ? Mono.just(quietPosition -> Mono.fromRunnable(() -> quietPositions.add(quietPosition))) : Mono.empty());
        return quietPositions;
    }

    private List<String> writeMatchingEvents(int count) {
        List<String> ids = new CopyOnWriteArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(write("Matching"));
        }
        return ids;
    }

    private String write(String type) {
        String id = UUID.randomUUID().toString();
        CloudEvent event = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .build();
        eventStore.write(UUID.randomUUID().toString(), Flux.just(event)).block(TIMEOUT);
        return id;
    }

    private @Nullable String storedPosition() {
        return checkpointStorage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT);
    }

    // Writes an event no subscription matches every 100 ms, so the change stream moves on without an event to deliver
    private final class OtherEvents {
        private final AtomicBoolean writing = new AtomicBoolean();
        private @Nullable Thread thread;

        void start() {
            writing.set(true);
            thread = Thread.ofPlatform().start(() -> {
                while (writing.get()) {
                    write("Other");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
        }

        void stop() throws InterruptedException {
            writing.set(false);
            if (thread != null) {
                thread.join();
            }
        }
    }

    // Records, for every listener added to it, how many events were delivered each time it asks that listener about
    // the subscription and each time it reports a quiet position to it, and which position it reported
    private static final class RecordingMongoModel extends ReactorMongoSubscriptionModel {
        private final List<CloudEvent> delivered;
        private final List<QuietPositionListener> added = new CopyOnWriteArrayList<>();
        private final Map<QuietPositionListener, QuietPositionListener> recordingListeners = new ConcurrentHashMap<>();
        private final List<Integer> deliveredWhenAsked = new CopyOnWriteArrayList<>();
        private final List<Integer> deliveredWhenReported = new CopyOnWriteArrayList<>();
        private final List<Checkpoint> positionsReported = new CopyOnWriteArrayList<>();

        private RecordingMongoModel(ReactiveMongoTemplate template, String eventCollectionName, List<CloudEvent> delivered) {
            super(template, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
            this.delivered = delivered;
        }

        @Override
        public void addQuietPositionListener(QuietPositionListener listener) {
            added.add(listener);
            QuietPositionListener recording = subscriptionId -> {
                if (!SUBSCRIPTION_ID.equals(subscriptionId)) {
                    return listener.beforeReading(subscriptionId);
                }
                deliveredWhenAsked.add(delivered.size());
                return listener.beforeReading(subscriptionId).map(save -> quietPosition -> {
                    deliveredWhenReported.add(delivered.size());
                    positionsReported.add(quietPosition);
                    return save.apply(quietPosition);
                });
            };
            recordingListeners.put(listener, recording);
            super.addQuietPositionListener(recording);
        }

        @Override
        public void removeQuietPositionListener(QuietPositionListener listener) {
            QuietPositionListener recording = recordingListeners.remove(listener);
            if (recording != null) {
                super.removeQuietPositionListener(recording);
            }
        }

        // Adds a listener of the test's own, which the model asks like any other but doesn't record
        void listenDirectly(QuietPositionListener listener) {
            super.addQuietPositionListener(listener);
        }
    }
}
