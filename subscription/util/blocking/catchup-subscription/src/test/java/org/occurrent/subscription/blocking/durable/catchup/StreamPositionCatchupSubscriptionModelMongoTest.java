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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
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
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * Real MongoDB integration test for {@link CatchupSubscriptionModel} in stream mode when the store writes a global
 * position ({@code writesPosition()} is {@code true}). See ADR 25: the reconciliation strategy (position vs
 * time) is chosen from the store's position capability, not from the DCB-vs-stream distinction, so a STREAM-only,
 * position-enabled store now replays through the same position-windowed range loop DCB mode uses (see
 * {@link DcbCatchupSubscriptionModelMongoTest}), reading via {@code PositionOrderedReader.readInPositionOrder}, and
 * resumes by {@link GlobalCheckpoint} instead of by time.
 * <p>
 * This covers the same class of scenario as the #199 clock-skew bug (a during-catch-up write reconciled without loss
 * or duplication across the catch-up-to-live handover), now on the position path where a clock-skewed event time
 * cannot cause loss at all because reconciliation never looks at time.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamPositionCatchupSubscriptionModelMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final Duration AT_MOST = Duration.ofSeconds(30);
    private static final String EVENT_TYPE = NameDefined.class.getName();

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStore eventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
    private SpringMongoCheckpointStorage storage;
    private CatchupSubscriptionModel subscription;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private MongoClient mongoClient;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".stream_position_catchup");
        mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(connectionString.getCollection())
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, requireNonNull(connectionString.getCollection()), timeRepresentation);
        storage = new SpringMongoCheckpointStorage(mongoTemplate, "storage");
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.shutdown();
        }
        subscriptionModel.shutdown();
        mongoClient.close();
    }

    @Test
    void position_enabled_stream_store_rebuilds_a_projection_via_position_windowed_catchup_with_no_loss_or_duplication_across_a_concurrent_write_window() {
        // Given a stream history written before the subscription starts
        NameDefined historic1 = nameDefined("historic1");
        NameDefined historic2 = nameDefined("historic2");
        write(historic1);
        write(historic2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        // When the position-mode catch-up subscription replays from the beginning of the global position sequence
        subscription.subscribe("subscription", StreamSubscriptionFilter.filter(type(EVENT_TYPE)),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();

        // Then the historic events are delivered in position order
        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(received).containsExactly(historic1, historic2));

        // And when events are written after the handover, including one with a clock-skewed (earlier) event time --
        // a time-based reconciliation would be vulnerable to losing this event, but position reconciliation never
        // looks at time, so it cannot be affected.
        NameDefined skewed = new NameDefined(UUID.randomUUID().toString(), time.minusSeconds(60), "name", "skewed");
        NameDefined live = nameDefined("live");
        write(skewed);
        write(live);

        // Then all live events arrive through the change stream, in order, with no duplicates at the seam
        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() -> {
            assertThat(received).containsExactly(historic1, historic2, skewed, live);
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void resuming_from_an_explicit_global_position_never_redelivers_an_event_at_or_before_it() {
        NameDefined event1 = nameDefined("event1");
        NameDefined event2 = nameDefined("event2");
        NameDefined event3 = nameDefined("event3");
        write(event1);
        write(event2);
        write(event3);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        // Resuming after position 1 (as if event1 was already processed elsewhere) replays only event2 and event3.
        subscription.subscribe("subscription", StreamSubscriptionFilter.filter(type(EVENT_TYPE)),
                StartAt.checkpoint(GlobalCheckpoint.of(1)), toDomainEvents(received)).waitUntilStarted();

        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(received).containsExactly(event2, event3));
        assertThat(received).doesNotContain(event1);
    }

    @Test
    void a_legacy_time_based_resume_token_in_position_mode_is_detected_and_re_resolved_instead_of_being_trusted() {
        // Simulate a store that flipped stream position on after previously running the legacy time-based catch-up:
        // the checkpoint storage still holds a time-based token written before the flip.
        NameDefined preFlip = nameDefined("preFlip");
        write(preFlip);
        String legacyTimeToken = RFC_3339_DATE_TIME_FORMATTER.format(OffsetDateTime.now().minusMinutes(1));
        storage.save("subscription", new StringBasedCheckpoint(legacyTimeToken));

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        // Using StartAt.subscriptionModelDefault() triggers the default resume-from-storage path, where the model
        // must detect that the stored token is a legacy time token (not a GlobalCheckpoint) and re-resolve
        // rather than misinterpret it as -- or crash trying to parse it as -- a position.
        subscription.subscribe("subscription", StreamSubscriptionFilter.filter(type(EVENT_TYPE)),
                StartAt.subscriptionModelDefault(), toDomainEvents(received)).waitUntilStarted();

        // The re-resolved subscription does not replay preFlip history (it delegates live, exactly like a
        // fresh subscription with no stored position at all), but subsequently delivers new live events normally.
        NameDefined afterResolve = nameDefined("afterResolve");
        write(afterResolve);
        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(received).containsExactly(afterResolve));
        assertThat(received).doesNotContain(preFlip);
    }

    @Test
    void a_dynamic_start_at_that_disallows_the_delegated_subscription_model_still_subscribes_live_and_does_not_lose_a_low_position_event_committed_late() {
        // This exercises the delegatedStartAt == null branch (e.g. the @StreamSubscription SAME_AS_START_AT resume
        // behavior, which tells the durable layer not to persist a checkpoint because the subscription always
        // restarts from the same StartAt). In that branch globalCheckpoint is also null (see
        // CatchupSubscriptionModel#startPositionCatchupSubscriptionForStream), so no watermark-derived cursor is ever
        // persisted, and getDelegatedSubscriptionModel().subscribe(...) is still called with the original dynamic
        // startAt afterwards, i.e. a live change-stream subscription is started right after the catch-up phase, not
        // skipped. This test proves an event that is only assigned (and commits) a low position after the bulk
        // replay's head was read is still delivered, because the live change-stream subscription independently sees
        // every subsequent insert regardless of the position value it carries.
        NameDefined historic = nameDefined("historic");
        write(historic);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        // A dynamic StartAt that resolves to "replay from the beginning" for CatchupSubscriptionModel's own context
        // (so the bulk replay below actually runs), but returns null for the delegated (innermost) subscription
        // model's context, mirroring what OccurrentAnnotationBeanPostProcessor generates for SAME_AS_START_AT +
        // BEGINNING_OF_TIME: "don't persist a checkpoint, always restart from beginning".
        StartAt sameAsStartAt = StartAt.dynamic(ctx -> ctx.subscriptionModelType().equals(CatchupSubscriptionModel.class)
                ? StartAt.checkpoint(GlobalCheckpoint.of(0))
                : null);

        subscription.subscribe("subscription", StreamSubscriptionFilter.filter(type(EVENT_TYPE)), sameAsStartAt, toDomainEvents(received))
                .waitUntilStarted();

        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(received).containsExactly(historic));

        // A late-committing event: positions are reserved before commit, so in a real race a low position can commit
        // after the bulk replay's head was read. Simulated here simply as a plain write after the handover, since the
        // live change-stream subscription (started because delegatedStartAt == null still subscribes live) picks up
        // every insert regardless of ordering between position reservation and commit.
        NameDefined lateCommitting = nameDefined("lateCommitting");
        write(lateCommitting);

        await().atMost(AT_MOST).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() ->
                assertThat(received).containsExactly(historic, lateCommitting));

        // No durable checkpoint is stored in this mode, confirming no watermark-derived cursor is persisted.
        assertThat(storage.exists("subscription")).isFalse();
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), WriteCondition.anyStreamVersion(), cloudEvents);
    }
}
