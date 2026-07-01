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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDcbCatchupSubscriptionModelMongoTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
            .withReplicaSet()
            .withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flush = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbcatchup"));

    private ReactorMongoEventStore eventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;
    private CloudEventConverter<DomainEvent> converter;
    private MongoClient mongoClient;
    private final CopyOnWriteArrayList<Disposable> disposables = new CopyOnWriteArrayList<>();

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbcatchup");
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
    void replays_dcb_history_from_the_beginning_then_delivers_live_events_without_duplicates() {
        NameDefined h1 = name("h1");
        NameDefined h2 = name("h2");
        appendTagged(h1, "name:1");
        appendTagged(name("ignoredHistoric"), "other:1");
        appendTagged(h2, "name:1");

        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(subscriptionModel, eventStore);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(DcbQuery.tags("name:1"), DcbStartAt.beginning()), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("h1", "h2"));

        NameDefined l1 = name("l1");
        NameDefined l2 = name("l2");
        appendTagged(l1, "name:1");
        appendTagged(name("ignoredLive"), "other:1");
        appendTagged(l2, "name:1");

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactly("h1", "h2", "l1", "l2");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void a_dcb_event_committed_while_the_replay_is_running_is_delivered_exactly_once() {
        // Two historic matching events, plus a non-matching one.
        appendTagged(name("h1"), "name:1");
        appendTagged(name("h2"), "name:1");

        // Delay the first replay read so an event can commit while the replay is in flight. The live resume token is
        // captured before the replay, so the during-replay event must still be delivered, exactly once.
        AtomicBoolean firstReadStarted = new AtomicBoolean(false);
        DelayFirstReadDcbEventStore delaying = new DelayFirstReadDcbEventStore(eventStore, Duration.ofSeconds(2), firstReadStarted);

        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(subscriptionModel, delaying);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(DcbQuery.tags("name:1"), DcbStartAt.beginning()), received);

        // Wait until the replay read is in flight, then commit a new matching event during the delay.
        await().atMost(Duration.ofSeconds(40)).untilTrue(firstReadStarted);
        appendTagged(name("duringReplay"), "name:1");

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactlyInAnyOrder("h1", "h2", "duringReplay");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void only_events_matching_the_query_are_delivered_during_catchup_and_live() {
        appendTagged(name("matchHistoric"), "name:1");
        appendTagged(name("ignoredHistoric"), "other:1");

        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(subscriptionModel, eventStore);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(DcbQuery.tags("name:1"), DcbStartAt.beginning()), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("matchHistoric"));

        appendTagged(name("matchLive"), "name:1");
        appendTagged(name("ignoredLive"), "other:1");

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("matchHistoric", "matchLive"));
    }

    @Test
    void replays_every_event_with_a_small_window_and_cache_then_goes_live_without_loss() {
        // More matching events than both the window and the handover cache, so the bulk replay pages across many
        // windows and the cache evicts during the replay. The handover must still deliver every event exactly once.
        for (int i = 0; i < 5; i++) {
            appendTagged(name("h" + i), "name:1");
        }

        ReactorDcbCatchupSubscriptionModel catchup = new ReactorDcbCatchupSubscriptionModel(subscriptionModel, eventStore, 1, 1);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(DcbQuery.tags("name:1"), DcbStartAt.beginning()), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("h0", "h1", "h2", "h3", "h4"));

        appendTagged(name("live0"), "name:1");

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactly("h0", "h1", "h2", "h3", "h4", "live0");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    private void subscribe(Flux<CloudEvent> flux, CopyOnWriteArrayList<String> received) {
        disposables.add(flux.map(ce -> ((NameDefined) converter.toDomainEvent(ce)).name()).doOnNext(received::add).subscribe());
        // Give the change-stream subscription a moment to start before the test writes more events.
        sleep(700);
    }

    private NameDefined name(String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), name, name);
    }

    private void appendTagged(DomainEvent event, String tag) {
        CloudEvent cloudEvent = converter.toCloudEvents(Stream.of(event)).collect(Collectors.toList()).get(0);
        eventStore.append(List.of(DcbCloudEvents.withTags(cloudEvent, Set.of(tag)))).block();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Delays the first bulk window read so an event can commit while the replay is in flight. The model's first read is
    // the head probe (afterPosition == upToPosition), so the delay is applied to the first real window read after it.
    // The event committed during the delay lands above the captured bulk head and must be recovered exactly once
    // through reconciliation or the live subscription, which resumes from a token captured before the replay.
    private static final class DelayFirstReadDcbEventStore implements DcbEventStore {
        private final DcbEventStore delegate;
        private final Duration delay;
        private final AtomicBoolean firstReadStarted;
        private final AtomicBoolean windowDelayed = new AtomicBoolean(false);

        private DelayFirstReadDcbEventStore(DcbEventStore delegate, Duration delay, AtomicBoolean firstReadStarted) {
            this.delegate = delegate;
            this.delay = delay;
            this.firstReadStarted = firstReadStarted;
        }

        @Override
        public Mono<DcbEventStream> read(DcbQuery query, DcbReadOptions options) {
            boolean isHeadProbe = options.afterSequencePosition().orElse(0) == options.upToSequencePosition().orElse(0);
            if (!isHeadProbe && windowDelayed.compareAndSet(false, true)) {
                return Mono.defer(() -> {
                    firstReadStarted.set(true);
                    return delegate.read(query, options).delayElement(delay);
                });
            }
            return delegate.read(query, options);
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return delegate.append(events);
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return delegate.append(events, condition);
        }
    }
}
