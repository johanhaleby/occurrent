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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
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
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * These tests combine STREAM with DCB to prove the reactive stream catch-up path works when stream events share the
 * global {@code position} sequence with DCB events.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelMongoTest {

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
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".streamcatchup");
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveMongoTransactionManager tx = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(tx)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
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
    void replays_stream_history_from_the_beginning_then_delivers_live_events_without_duplicates() {
        NameDefined h1 = name("h1");
        NameDefined h2 = name("h2");
        appendToStream("stream-1", h1);
        appendToStream("stream-2", name("ignoredHistoric"));
        appendToStream("stream-1", h2);

        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, asReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(Filter.streamId("stream-1"), StartAt.checkpoint(GlobalCheckpoint.of(0))), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("h1", "h2"));

        NameDefined l1 = name("l1");
        NameDefined l2 = name("l2");
        appendToStream("stream-1", l1);
        appendToStream("stream-2", name("ignoredLive"));
        appendToStream("stream-1", l2);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactly("h1", "h2", "l1", "l2");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void a_stream_event_committed_while_the_replay_is_running_is_delivered_exactly_once() {
        // Two historic matching events in the same stream.
        appendToStream("stream-1", name("h1"));
        appendToStream("stream-1", name("h2"));

        // Delay the first replay read so an event can commit while the replay is in flight. The live resume token is
        // captured before the replay, so the during-replay event must still be delivered, exactly once.
        AtomicBoolean firstReadStarted = new AtomicBoolean(false);
        DelayFirstReadPositionOrderedReader delaying = new DelayFirstReadPositionOrderedReader(asReader(), Duration.ofSeconds(2), firstReadStarted);

        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, delaying);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(Filter.streamId("stream-1"), StartAt.checkpoint(GlobalCheckpoint.of(0))), received);

        // Wait until the replay read is in flight, then commit a new matching event during the delay.
        await().atMost(Duration.ofSeconds(40)).untilTrue(firstReadStarted);
        appendToStream("stream-1", name("duringReplay"));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactlyInAnyOrder("h1", "h2", "duringReplay");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void a_small_cache_still_re_delivers_a_write_a_history_window_read_after_the_head() {
        // The #891 shape against a real store, with the handover cache at 1 so nothing survives in it by accident.
        // The reader reports a head one above what is committed, which ADR 84 allows since currentPosition is a
        // high-watermark rather than a fence, and that is exactly what a position reserved by an uncommitted write
        // looks like. The event written during the delayed first window read then lands at or below that head, so a
        // history window reads it. Nothing a history window delivers is recorded, so the live stream has to deliver
        // it again. Feed the cache from the history windows again and the second delivery disappears.
        appendToStream("stream-1", name("h1"));

        AtomicBoolean firstReadStarted = new AtomicBoolean(false);
        CountDownLatch releaseTheFirstRead = new CountDownLatch(1);
        PositionOrderedReader reader = new HeadAheadOfCommittedPositionOrderedReader(asReader(), 1, releaseTheFirstRead, firstReadStarted);

        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, reader, null, 1000, 1);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(Filter.streamId("stream-1"), StartAt.checkpoint(GlobalCheckpoint.of(0))), received);

        await().atMost(Duration.ofSeconds(40)).untilTrue(firstReadStarted);
        appendToStream("stream-1", name("committedAfterTheHeadRead"));
        releaseTheFirstRead.countDown();

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                assertThat(received).filteredOn("committedAfterTheHeadRead"::equals).hasSizeGreaterThanOrEqualTo(2));
        assertThat(received).contains("h1");
    }

    @Test
    void only_events_matching_the_filter_are_delivered_during_catchup_and_live() {
        appendToStream("stream-1", name("matchHistoric"));
        appendToStream("stream-2", name("ignoredHistoric"));

        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, asReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(Filter.streamId("stream-1"), StartAt.checkpoint(GlobalCheckpoint.of(0))), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("matchHistoric"));

        appendToStream("stream-1", name("matchLive"));
        appendToStream("stream-2", name("ignoredLive"));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("matchHistoric", "matchLive"));
    }

    @Test
    void replays_every_event_with_a_small_window_and_cache_then_goes_live_without_loss() {
        // More matching events than both the window and the handover cache, so the bulk replay pages across many
        // windows and the cache evicts during the replay. The handover must still deliver every event exactly once.
        for (int i = 0; i < 5; i++) {
            appendToStream("stream-1", name("h" + i));
        }

        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, asReader(), 1, 1);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(Filter.streamId("stream-1"), StartAt.checkpoint(GlobalCheckpoint.of(0))), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("h0", "h1", "h2", "h3", "h4"));

        appendToStream("stream-1", name("live0"));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactly("h0", "h1", "h2", "h3", "h4", "live0");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void resumes_correctly_from_a_global_subscription_position() {
        appendToStream("stream-1", name("h1"));
        appendToStream("stream-1", name("h2"));
        appendToStream("stream-1", name("h3"));

        // Resolve the position of h1 so the resumed replay should skip it and start with h2.
        CloudEvent h1Event = requireNonNull(requireNonNull(eventStore.read("stream-1", 0, Integer.MAX_VALUE).block())
                .events().blockFirst());
        long h1Position = OccurrentCloudEventExtension.getPosition(h1Event);

        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, asReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(Filter.streamId("stream-1"), StartAt.checkpoint(GlobalCheckpoint.of(h1Position))), received);

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("h2", "h3"));

        appendToStream("stream-1", name("live0"));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(received).containsExactly("h2", "h3", "live0");
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void stream_subscription_does_not_receive_dcb_only_events_during_catchup_and_live_delivery() {
        // Given - one stream-only event and one DCB-tagged event written before the subscription starts. Both are
        // NameDefined events, so a leaked DCB event would deserialize and show up in the received list.
        appendToStream("stream-1", name("streamHistoric"));
        appendDcb(name("dcbHistoric"));

        // When - a stream catch-up subscription with no stream constraint (Filter.all()) replays from the beginning
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(subscriptionModel, asReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        subscribe(catchup.subscribe(Filter.all(), StartAt.checkpoint(GlobalCheckpoint.of(0))), received);

        // Then - only the stream event is replayed, the DCB event is excluded by the stream-capability guard
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("streamHistoric"));

        // When - after handover to live, a DCB event and then a stream event arrive
        appendDcb(name("dcbLive"));
        appendToStream("stream-1", name("streamLive"));

        // Then - the live stream event is delivered but neither DCB event ever is, proving the guard covers both the
        // replay and the live handover phase
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(received).containsExactly("streamHistoric", "streamLive"));
    }

    private PositionOrderedReader asReader() {
        return eventStore;
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

    private void appendDcb(DomainEvent event) {
        CloudEvent cloudEvent = converter.toCloudEvents(List.of(event)).get(0);
        CloudEvent dcbEvent = DcbCloudEvents.withTags(cloudEvent, List.of(Tag.parse("kind:dcb")));
        eventStore.append(List.of(dcbEvent)).block();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Delays the first bulk window read so an event can commit while the replay is in flight. currentPosition is the
    // head probe, so the delay is applied to the first window read after it.
    // Reports a head above the highest committed position, which is what a position reserved by a write that has not
    // committed yet looks like to a reader. ADR 84 allows it, currentPosition is a high-watermark. The first window
    // read is held until the test releases it, rather than for a fixed time, so the query runs once the test has
    // written and actually sees that write inside the window the head already covers. A fixed delay would race the
    // write on a slow machine and fail without saying why.
    private record HeadAheadOfCommittedPositionOrderedReader(PositionOrderedReader delegate, long ahead, CountDownLatch releaseFirstRead,
                                                             AtomicBoolean firstReadStarted) implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.defer(() -> {
                if (!firstReadStarted.compareAndSet(false, true)) {
                    return delegate.readInPositionOrder(filter, range);
                }
                return Mono.fromCallable(() -> releaseFirstRead.await(60, TimeUnit.SECONDS))
                        .subscribeOn(Schedulers.boundedElastic())
                        .thenMany(delegate.readInPositionOrder(filter, range));
            });
        }

        @Override
        public Mono<Long> currentPosition() {
            return delegate.currentPosition().map(position -> position + ahead);
        }

        @Override
        public boolean writesPosition() {
            return delegate.writesPosition();
        }
    }

    private static final class DelayFirstReadPositionOrderedReader implements PositionOrderedReader {
        private final PositionOrderedReader delegate;
        private final Duration delay;
        private final AtomicBoolean firstReadStarted;
        private final AtomicBoolean windowDelayed = new AtomicBoolean(false);

        private DelayFirstReadPositionOrderedReader(PositionOrderedReader delegate, Duration delay, AtomicBoolean firstReadStarted) {
            this.delegate = delegate;
            this.delay = delay;
            this.firstReadStarted = firstReadStarted;
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            if (windowDelayed.compareAndSet(false, true)) {
                return Flux.defer(() -> {
                    firstReadStarted.set(true);
                    return delegate.readInPositionOrder(filter, range).delayElements(delay);
                });
            }
            return delegate.readInPositionOrder(filter, range);
        }

        @Override
        public Mono<Long> currentPosition() {
            return delegate.currentPosition();
        }

        @Override
        public boolean writesPosition() {
            return delegate.writesPosition();
        }
    }
}
