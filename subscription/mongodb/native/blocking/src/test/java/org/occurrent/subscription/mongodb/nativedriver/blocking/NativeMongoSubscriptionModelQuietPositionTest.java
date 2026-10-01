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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * The position a subscription reports when a read returned no event for it. Every test writes events the
 * subscription's filter doesn't match, since those are what move the change stream on without an event to deliver.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelQuietPositionTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true).withReplicaSet();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private MongoClient mongoClient;
    private MongoEventStore mongoEventStore;
    private ExecutorService subscriptionExecutor;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createSubscriptionModel() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".quiet");
        mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
        subscriptionExecutor = Executors.newCachedThreadPool();
        subscriptionModel = new NativeMongoSubscriptionModel(database, requireNonNull(connectionString.getCollection()), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                NativeMongoSubscriptionModelConfig.withConfig().maxAwaitTime(Duration.ofMillis(100)));
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        mongoClient.close();
    }

    @Test
    void a_subscription_that_matches_nothing_reports_a_position_after_the_events_it_did_not_match() {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> subscriptionId.equals("quiet") ? quietPositions::add : null);
        CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("quiet", AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName())), StartAt.now(), delivered::add).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined matched = nameDefined();
        mongoEventStore.write("matched", 0, serialize(matched));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).extracting(CloudEvent::getId).containsExactly(matched.eventId()));
        Checkpoint positionOfLastEvent = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(delivered.getFirst());

        // When
        CopyOnWriteArrayList<CloudEvent> everyEvent = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("every-event", StartAt.now(), everyEvent::add).waitUntilStarted(Duration.ofSeconds(10));
        NameWasChanged notMatched = nameWasChanged();
        mongoEventStore.write("not-matched", 0, serialize(notMatched));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(everyEvent).extracting(CloudEvent::getId).containsExactly(notMatched.eventId()));
        // Positions reported once the event that did not match has been read
        int reportedBeforeTheRead = quietPositions.size();
        await().atMost(10, SECONDS).until(() -> quietPositions.size() > reportedBeforeTheRead + 2);
        Checkpoint quietPosition = quietPositions.getLast();

        // Then
        assertThat(quietPosition.asString()).isNotEqualTo(positionOfLastEvent.asString());
        CopyOnWriteArrayList<CloudEvent> fromTheQuietPosition = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("from-the-quiet-position", StartAt.checkpoint(quietPosition), fromTheQuietPosition::add).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined writtenLater = nameDefined();
        mongoEventStore.write("written-later", 0, serialize(writtenLater));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(fromTheQuietPosition).extracting(CloudEvent::getId).contains(writtenLater.eventId()));
        // The position MongoDB sends with an empty batch can come before an event written at the same time as the
        // last one it read, so the event that did not match may be read again. The one that matched is not.
        assertThat(fromTheQuietPosition).as("read from the quiet position").extracting(CloudEvent::getId).doesNotContain(matched.eventId());
    }

    @Test
    void no_quiet_position_is_reported_while_the_action_runs_for_an_event() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> quietPositions::add);
        CountDownLatch actionRunning = new CountDownLatch(1);
        CountDownLatch finishAction = new CountDownLatch(1);
        subscriptionModel.subscribe("busy", AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName())), StartAt.now(), __ -> {
            actionRunning.countDown();
            try {
                finishAction.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("matched", 0, serialize(nameDefined()));
        assertThat(actionRunning.await(10, SECONDS)).isTrue();
        int reportedBeforeTheAction = quietPositions.size();

        // When
        mongoEventStore.write("not-matched", 0, serialize(nameWasChanged()));

        // Then
        try {
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(quietPositions).hasSize(reportedBeforeTheAction));
        } finally {
            finishAction.countDown();
        }
        await().atMost(10, SECONDS).until(() -> quietPositions.size() > reportedBeforeTheAction);
    }

    @Test
    void a_paused_subscription_reports_no_quiet_position_and_a_resumed_one_does() {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> quietPositions::add);
        subscriptionModel.subscribe("paused", StartAt.now(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        await().atMost(10, SECONDS).until(() -> !quietPositions.isEmpty());

        // When
        subscriptionModel.pauseSubscription("paused");
        int reportedWhenPaused = quietPositions.size();

        // Then
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(quietPositions).hasSize(reportedWhenPaused));
        subscriptionModel.resumeSubscription("paused").waitUntilStarted(Duration.ofSeconds(10));
        await().atMost(10, SECONDS).until(() -> quietPositions.size() > reportedWhenPaused);
    }

    @Test
    void a_refused_write_of_the_quiet_position_ends_delivery_and_the_subscription_stays_running() {
        // Given
        AtomicInteger refused = new AtomicInteger();
        subscriptionModel.addQuietPositionListener(subscriptionId -> quietPosition -> {
            refused.incrementAndGet();
            throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(2), CheckpointWriteCondition.notOlderThan(1));
        });
        CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("refused", StartAt.now(), delivered::add).waitUntilStarted(Duration.ofSeconds(10));
        // The refusal ends up on the executor thread, which is this model's way of ending delivery
        await().dontCatchUncaughtExceptions().atMost(10, SECONDS).until(() -> refused.get() == 1);

        // When
        mongoEventStore.write("written-after-the-refusal", 0, serialize(nameDefined()));

        // Then
        await().dontCatchUncaughtExceptions().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(delivered).isEmpty());
        assertThat(refused).as("writes of the quiet position that were refused").hasValue(1);
        assertThat(subscriptionModel.isRunning("refused")).isTrue();
    }

    @Test
    void a_listener_that_is_removed_is_not_asked_again() {
        // Given
        AtomicInteger asked = new AtomicInteger();
        org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions.QuietPositionListener listener = subscriptionId -> {
            asked.incrementAndGet();
            return null;
        };
        subscriptionModel.addQuietPositionListener(listener);
        subscriptionModel.subscribe("asked", StartAt.now(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        await().atMost(10, SECONDS).until(() -> asked.get() > 1);

        // When
        subscriptionModel.removeQuietPositionListener(listener);
        // The read that was under way when the listener was removed has already asked it
        await().pollDelay(Duration.ofMillis(500)).until(() -> true);
        int askedWhenRemoved = asked.get();

        // Then
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(asked).hasValue(askedWhenRemoved));
    }

    private NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
    }

    private NameWasChanged nameWasChanged() {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2");
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}
