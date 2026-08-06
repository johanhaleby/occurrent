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

package org.occurrent.subscription.reactor.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.*;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Real MongoDB integration test for the dual-mode {@link ReactorCatchupSubscriptionModel}, backed by a single
 * combined STREAM+DCB {@link ReactorMongoEventStore} that fills both the {@code PositionOrderedReader} and
 * {@code DcbEventStore} roles. Proves the routing fix end to end: a stream subscription and a DCB subscription
 * registered on the very same dispatcher instance each replay their own history and go live correctly, with no
 * cross-talk between the two catch-up paths.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupSubscriptionModelMongoTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactorMongoEventStore eventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;
    private CloudEventConverter<DomainEvent> converter;
    private MongoClient mongoClient;
    private final CopyOnWriteArrayList<Disposable> disposables = new CopyOnWriteArrayList<>();

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dualcatchup");
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager tx = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(EventStoreCapability.STREAM, EventStoreCapability.DCB)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
        subscriptionModel = new ReactorMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    void a_stream_subscription_on_the_dual_mode_dispatcher_replays_stream_history_from_the_beginning_with_no_loss_or_duplication_across_a_concurrent_write_window() {
        appendToStream("stream-1", name("h1"));
        appendToStream("stream-2", name("ignoredHistoric"));
        appendToStream("stream-1", name("h2"));

        ReactorCatchupSubscriptionModel catchup = dualMode();
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(StreamSubscriptionFilter.filter(Filter.streamId("stream-1")), StartAt.checkpoint(GlobalCheckpoint.of(0))), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("h1", "h2"));

        // A clock-skewed (earlier event time) event, plus a normal one, committed after the handover. Position-based
        // reconciliation never looks at event time, so a skewed timestamp cannot cause loss here (see #199 / ADR 25).
        NameDefined skewed = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now().minusSeconds(60), "name", "skewed");
        appendToStream("stream-1", skewed);
        appendToStream("stream-2", name("ignoredLive"));
        appendToStream("stream-1", name("live"));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactly("h1", "h2", "skewed", "live");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void a_dcb_subscription_on_the_same_dual_mode_dispatcher_replays_dcb_history_from_the_beginning_with_no_loss_or_duplication_across_a_concurrent_write_window() {
        appendTagged(name("h1"), "name:1");
        appendTagged(name("ignoredHistoric"), "other:1");
        appendTagged(name("h2"), "name:1");

        ReactorCatchupSubscriptionModel catchup = dualMode();
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), DcbStartAt.beginning().toStartAt()), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("h1", "h2"));

        appendTagged(name("live1"), "name:1");
        appendTagged(name("ignoredLive"), "other:1");

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactly("h1", "h2", "live1");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void a_stream_and_a_dcb_subscription_registered_on_the_same_dual_mode_dispatcher_each_replay_only_their_own_history() {
        appendToStream("stream-1", name("streamHistoric"));
        appendTagged(name("dcbHistoric"), "name:1");

        ReactorCatchupSubscriptionModel catchup = dualMode();

        CopyOnWriteArrayList<String> streamReceived = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(StreamSubscriptionFilter.filter(Filter.streamId("stream-1")), StartAt.checkpoint(GlobalCheckpoint.of(0))), streamReceived);

        CopyOnWriteArrayList<String> dcbReceived = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(DcbSubscriptionFilter.filter(DcbCriteria.tags(Tag.parse("name:1"))), DcbStartAt.beginning().toStartAt()), dcbReceived);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(streamReceived).containsExactly("streamHistoric");
            assertThat(dcbReceived).containsExactly("dcbHistoric");
        });

        appendToStream("stream-1", name("streamLive"));
        appendTagged(name("dcbLive"), "name:1");

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(streamReceived).containsExactly("streamHistoric", "streamLive");
            assertThat(dcbReceived).containsExactly("dcbHistoric", "dcbLive");
        });
    }

    private ReactorCatchupSubscriptionModel dualMode() {
        return new ReactorCatchupSubscriptionModel(subscriptionModel, eventStore, eventStore, null, null);
    }

    private void subscribe(Flux<CloudEvent> flux, CopyOnWriteArrayList<String> received) {
        disposables.add(flux.map(ce -> ((NameDefined) converter.toDomainEvent(ce)).name()).doOnNext(received::add).subscribe());
        // Give the change-stream subscription a moment to start before the test writes more events.
        sleep(700);
    }

    private NameDefined name(String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), name, name);
    }

    private void appendToStream(String streamId, DomainEvent event) {
        CloudEvent cloudEvent = converter.toCloudEvents(List.of(event)).get(0);
        eventStore.write(streamId, WriteCondition.anyStreamVersion(), Flux.just(cloudEvent)).block();
    }

    private void appendTagged(DomainEvent event, String tag) {
        CloudEvent cloudEvent = converter.toCloudEvents(List.of(event)).get(0);
        eventStore.append(List.of(DcbCloudEvents.withTags(cloudEvent, Set.of(Tag.parse(tag))))).block();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
