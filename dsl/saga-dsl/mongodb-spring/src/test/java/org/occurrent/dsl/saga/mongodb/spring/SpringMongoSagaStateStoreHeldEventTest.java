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
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.mongodb.cloudevent.DocumentCloudEventReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A quarantined instance holds the events of the inputs it skips in the records collection, and a
 * release applies those held events, with no read of the event store. Checks against a MongoDB replica set that a held
 * event, with its data, its time and the extensions the event store gave it, reads back with the id, type, source,
 * data, time and extension names of the event the subscription delivered, and that the release applies what was read
 * back.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreHeldEventTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-held-event";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";
    private static final String OTHER_ORDER = "order-2";
    private static final int MONGODB_DOCUMENT_LIMIT = 16 * 1024 * 1024;
    private static final OffsetDateTime HAPPENED_AT = OffsetDateTime.parse("2026-01-01T10:15:30.123Z");

    sealed interface OrderEvent permits OrderPlaced, ItemAdded, Stamped {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item, int quantity) implements OrderEvent {
    }

    record Stamped(String orderId, Date at) implements OrderEvent {
    }

    record Pack(String item, int quantity) {
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private SpringMongoSagaStateStore<String> stateStore;
    private MongoTemplate template;
    private final List<Pack> dispatched = new CopyOnWriteArrayList<>();
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    private SagaSubscription subscription;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test"))
                .timeMapper(event -> HAPPENED_AT).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-held-" + UUID.randomUUID()));
        template = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(template, COLLECTION, String.class);
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
    }

    private Saga<OrderEvent, String, Pack> saga() {
        return Saga.<OrderEvent, String, Pack>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .evolve(ItemAdded.class, (state, e) -> "packing")
                .react(ItemAdded.class, (state, e) -> {
                    if (failing.contains(e.item())) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.item(), e.quantity())));
                })
                .evolve(Stamped.class, (state, e) -> "stamped")
                .react(Stamped.class, (state, e) -> {
                    if (failing.contains("stamped")) {
                        throw new IllegalStateException("cannot stamp");
                    }
                    return List.of(SagaEffect.issue(new Pack("stamped", 1)));
                })
                .build();
    }

    private void write(OrderEvent event) {
        eventStore.write(STREAM, converter.toCloudEvents(List.of(event)));
    }

    // An absent instance is a failed assertion, so an await that polls before the saga has stored it retries
    private SagaEnvelope<String> instance() {
        return instance(ORDER);
    }

    private List<CloudEvent> stored() {
        return eventStore.read(STREAM).eventList();
    }

    @Test
    void a_held_event_with_data_a_time_and_extensions_reads_back_equal_and_is_applied_by_a_release() {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("book");
        write(new ItemAdded(ORDER, "book", 2));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        write(new ItemAdded(ORDER, "pen", 5));
        List<CloudEvent> delivered = stored();
        CloudEvent book = delivered.get(1);
        CloudEvent pen = delivered.get(2);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(held()).hasSize(2));
        List<SkippedInput> held = held();

        assertAll(
                () -> assertThat(held).extracting(input -> input.event().getId()).containsExactly(book.getId(), pen.getId()),
                () -> assertThat(held).extracting(input -> input.event().getType()).containsExactly(book.getType(), pen.getType()),
                () -> assertThat(held).extracting(input -> input.event().getSource()).containsExactly(book.getSource(), pen.getSource()),
                () -> assertThat(held).extracting(input -> input.event().getExtensionNames()).containsExactly(book.getExtensionNames(), pen.getExtensionNames()),
                () -> assertThat(held).extracting(input -> requireNonNull(input.event().getData()).toBytes()).containsExactly(requireNonNull(book.getData()).toBytes(), requireNonNull(pen.getData()).toBytes()),
                () -> assertThat(held.get(1).event().getTime()).isEqualTo(HAPPENED_AT),
                () -> assertThat(held.get(1).event().getExtension(OccurrentCloudEventExtension.STREAM_ID)).isEqualTo(STREAM),
                () -> assertThat(held.get(1).event().getExtension(OccurrentCloudEventExtension.STREAM_VERSION)).hasToString(String.valueOf(pen.getExtension(OccurrentCloudEventExtension.STREAM_VERSION))),
                () -> assertThat(new String(requireNonNull(held.get(1).event().getData()).toBytes())).contains("\"pen\"")
        );

        failing.clear();
        subscription.release(ORDER);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE));
        assertThat(dispatched).as("the release applied the events as they were read back").containsExactly(new Pack("book", 2), new Pack("pen", 5));
    }

    @Test
    void a_release_that_quarantines_the_instance_again_on_a_held_event_can_be_followed_by_a_release_that_applies_it() {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("book");
        write(new ItemAdded(ORDER, "book", 2));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        write(new ItemAdded(ORDER, "pen", 5));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(held()).hasSize(2));

        // Stays releasing until the budget of the held event is used up, which a release after that sees
        AtomicReference<ReleaseResult> whileFailing = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            whileFailing.set(subscription.release(ORDER));
            assertThat(whileFailing.get()).as("the held event kept failing past its budget").isEqualTo(ReleaseResult.QUARANTINED_AGAIN);
        });
        SagaEnvelope<String> again = instance();
        List<SkippedInput> heldAgain = held();
        failing.clear();
        ReleaseResult afterItWasFixed = subscription.release(ORDER);

        assertAll(
                                () -> assertThat(again.isReleasing()).as("quarantined again is not releasing").isFalse(),
                () -> assertThat(heldAgain).as("the held event it stopped on first, and the one after").extracting(input -> input.event().getExtension(OccurrentCloudEventExtension.STREAM_VERSION).toString())
                        .containsExactly(String.valueOf(stored().get(1).getExtension(OccurrentCloudEventExtension.STREAM_VERSION)), String.valueOf(stored().get(2).getExtension(OccurrentCloudEventExtension.STREAM_VERSION))),
                () -> assertThat(afterItWasFixed).as("the second release goes through the held event as it was read back from MongoDB").isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(dispatched).as("each held event is applied once, in order").containsExactly(new Pack("book", 2), new Pack("pen", 5))
        );
    }

    @Test
    void a_release_that_fails_every_time_on_a_held_event_with_a_byte_array_extension_runs_out_of_its_budget_and_quarantines_the_instance_again() {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("book");
        eventStore.write(STREAM, List.of(CloudEventBuilder.v1(converter.toCloudEvent(new ItemAdded(ORDER, "book", 2))).withExtension("blob", new byte[]{1, 2}).build()));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        write(new ItemAdded(ORDER, "pen", 5));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(held()).hasSize(2));
        assertThat(held().getFirst().event().getExtension("blob")).as("the held event keeps its byte array extension").isEqualTo(new byte[]{1, 2});

        AtomicReference<ReleaseResult> whileFailing = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            whileFailing.set(subscription.release(ORDER));
            assertThat(whileFailing.get()).as("the held event kept failing past its budget").isEqualTo(ReleaseResult.QUARANTINED_AGAIN);
        });
        failing.clear();
        ReleaseResult afterItWasFixed = subscription.release(ORDER);

        assertAll(
                () -> assertThat(afterItWasFixed).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(dispatched).containsExactly(new Pack("book", 2), new Pack("pen", 5))
        );
    }

    @Test
    void a_13_MiB_binary_event_quarantines_the_instance_it_fails_on_and_stalls_neither_the_subscription_nor_the_release() {
        byte[] data = new byte[13 * 1024 * 1024];
        new Random(7).nextBytes(data);
        String blob = blobItem(data);
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, withBinaryEvents())
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add(blob);
        eventStore.write(STREAM, List.of(binaryEvent(data)));

        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(instance(ORDER).status()).as("the instance the 13 MiB binary event fails on is quarantined, which stores it in a record under the 16 MiB limit").isEqualTo(SagaStatus.QUARANTINED));
        write(new ItemAdded(ORDER, "nib", 1));
        write(new OrderPlaced(OTHER_ORDER));
        write(new ItemAdded(OTHER_ORDER, "pen", 5));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(dispatched).as("another instance gets the event written after the 13 MiB binary event, so the subscription did not stall on it").containsExactly(new Pack("pen", 5)));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(held()).as("the binary event and the input after it").hasSize(2));
        List<SkippedInput> held = held();
        CloudEvent heldBlob = held.get(0).event();
        byte[] heldBytes = requireNonNull(heldBlob.getData()).toBytes();
        int jsonSize = new JsonFormat().serialize(binaryEvent(data)).length;
        List<Integer> recordSizes = recordSizes();

        assertAll(
                () -> assertThat(jsonSize).as("the window: the event as CloudEvents JSON with base64 data is over MongoDB's 16 MiB limit").isGreaterThan(MONGODB_DOCUMENT_LIMIT),
                () -> assertThat(recordSizes).as("the window: the stored record of the binary event, in $bsonSize, takes more than its data and at most 16 MiB")
                        .anySatisfy(size -> assertThat(size).isGreaterThan(data.length).isLessThanOrEqualTo(MONGODB_DOCUMENT_LIMIT)),
                () -> assertThat(held).extracting(input -> input.event().getType()).containsExactly(binaryEvent(data).getType(), converter.getCloudEventType(ItemAdded.class)),
                () -> assertThat(heldBlob.getDataContentType()).isEqualTo("application/octet-stream"),
                () -> assertThat(heldBytes.length).as("the held binary data reads back to its length").isEqualTo(data.length),
                () -> assertThat(sha256(heldBytes)).as("the held binary data reads back to its bytes").isEqualTo(sha256(data))
        );

        failing.clear();
        subscription.release(ORDER);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(instance(ORDER).status()).isEqualTo(SagaStatus.ACTIVE));
        assertThat(dispatched).as("the release applies the held events in order, and the binary event reaches the saga with the length and the hash of its bytes")
                .containsExactly(new Pack("pen", 5), new Pack(blob, data.length), new Pack("nib", 1));
    }

    private CloudEvent binaryEvent(byte[] data) {
        return CloudEventBuilder.v1().withId("binary-1").withSource(URI.create("urn:test")).withType(converter.getCloudEventType(ItemAdded.class))
                .withSubject(ORDER).withTime(HAPPENED_AT).withDataContentType("application/octet-stream").withData(data).build();
    }

    // Reads a binary event as an item named after the hash of its bytes, and a quantity that is their length
    private CloudEventConverter<OrderEvent> withBinaryEvents() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                return converter.toCloudEvent(domainEvent);
            }

            @Override
            public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
                if (!"application/octet-stream".equals(cloudEvent.getDataContentType())) {
                    return converter.toDomainEvent(cloudEvent);
                }
                byte[] bytes = requireNonNull(cloudEvent.getData()).toBytes();
                return new ItemAdded(requireNonNull(cloudEvent.getSubject()), blobItem(bytes), bytes.length);
            }

            @Override
            public String getCloudEventType(Class<? extends OrderEvent> type) {
                return converter.getCloudEventType(type);
            }
        };
    }

    private static String blobItem(byte[] bytes) {
        return "blob-" + sha256(bytes);
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<Integer> recordSizes() {
        return template.getCollection(COLLECTION + "_skipped_inputs")
                .aggregate(List.of(new Document("$project", new Document("size", new Document("$bsonSize", "$$ROOT")))))
                .map(document -> document.getInteger("size")).into(new java.util.ArrayList<>());
    }

    private SagaEnvelope<String> instance(String sagaId) {
        return SagaEnvelopes.of(stateStore, null).find(sagaId).orElseThrow(() -> new AssertionError("instance '" + sagaId + "' is not stored"));
    }

    @Test
    void a_json_event_whose_data_holds_an_extended_json_object_is_released_with_the_bytes_it_was_delivered_with() {
        byte[] data = "{\"orderId\":\"order-1\",\"value\":{\"$numberLong\": \"42\"}}".getBytes(UTF_8);
        List<CloudEvent> converted = new CopyOnWriteArrayList<>();
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, recording("extended-1", converted, event -> new ItemAdded(requireNonNull(event.getSubject()), "extended", 1)))
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("extended");
        eventStore.write(STREAM, List.of(CloudEventBuilder.v1().withId("extended-1").withSource(URI.create("urn:test")).withType(converter.getCloudEventType(ItemAdded.class))
                .withSubject(ORDER).withTime(HAPPENED_AT).withDataContentType("application/json").withData(data).build()));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(held()).hasSize(1));
        CloudEvent delivered = stored().get(1);
        CloudEvent heldEvent = held().get(0).event();
        int convertedBeforeRelease = converted.size();

        failing.clear();
        subscription.release(ORDER);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE));
        List<CloudEvent> convertedByRelease = converted.subList(convertedBeforeRelease, converted.size());

        assertAll(
                () -> assertThat(requireNonNull(heldEvent.getData()).toBytes()).as("held data bytes equal the delivered bytes, so extended JSON is not parsed into another value").isEqualTo(data),
                () -> assertThat(requireNonNull(delivered.getData()).toBytes()).as("the delivered bytes are the ones written").isEqualTo(data),
                () -> assertThat(heldEvent.getDataContentType()).as("held content type equals the delivered one").isEqualTo(delivered.getDataContentType()),
                () -> assertThat(typedExtensions(heldEvent)).as("held extensions equal the delivered ones by value and type").isEqualTo(typedExtensions(delivered)),
                () -> assertThat(convertedByRelease).as("the converter is given the delivered bytes when the release replays the event").isNotEmpty().allSatisfy(event -> assertThat(requireNonNull(event.getData()).toBytes()).isEqualTo(data)),
                () -> assertThat(dispatched).as("the release dispatches the command of the held event").containsExactly(new Pack("extended", 1))
        );
    }

    @Test
    void an_event_with_typed_extensions_is_released_with_the_extensions_it_was_delivered_with() {
        OffsetDateTime stamp = OffsetDateTime.parse("2026-03-04T05:06:07.123456789+05:30");
        List<CloudEvent> converted = new CopyOnWriteArrayList<>();
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, recording("typed-1", converted, event -> new ItemAdded(requireNonNull(event.getSubject()), "typed", 1)))
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("typed");
        eventStore.write(STREAM, List.of(CloudEventBuilder.v1().withId("typed-1").withSource(URI.create("urn:test")).withType(converter.getCloudEventType(ItemAdded.class))
                .withSubject(ORDER).withTime(HAPPENED_AT).withDataContentType("application/json").withData("{}".getBytes(UTF_8))
                .withExtension("longext", 1_234_567_890_123L).withExtension("uriext", URI.create("urn:ext:1")).withExtension("timeext", stamp)
                .withExtension("bytesext", new byte[]{1, 2, 3, (byte) 200}).withExtension("intext", 7).withExtension("boolext", true).withExtension("strext", "text").build()));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(held()).hasSize(1));
        CloudEvent delivered = stored().get(1);
        CloudEvent heldEvent = held().get(0).event();
        int convertedBeforeRelease = converted.size();

        failing.clear();
        subscription.release(ORDER);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE));
        List<CloudEvent> convertedByRelease = converted.subList(convertedBeforeRelease, converted.size());

        assertAll(
                () -> assertThat(delivered.getExtensionNames()).as("the window: the delivered event has an extension of each type").contains("longext", "uriext", "timeext", "bytesext", "intext", "boolext", "strext"),
                () -> assertThat(typedExtensions(delivered).get("timeext")).as("the window: the delivered timestamp keeps its offset and nanoseconds").isEqualTo(List.of(OffsetDateTime.class, stamp)),
                () -> assertThat(typedExtensions(heldEvent)).as("held extensions equal the delivered ones by value and type").isEqualTo(typedExtensions(delivered)),
                () -> assertThat(convertedByRelease).as("the converter is given the delivered extensions when the release replays the event").isNotEmpty()
                        .allSatisfy(event -> assertThat(typedExtensions(event)).isEqualTo(typedExtensions(delivered))),
                () -> assertThat(dispatched).as("the release dispatches the command of the held event").containsExactly(new Pack("typed", 1))
        );
    }

    @Test
    void an_event_delivered_with_its_data_as_a_map_of_the_event_store_is_released_as_the_same_domain_event_and_the_same_map() {
        Date at = new Date(1_700_000_000_123L);
        Document json = new Document("orderId", ORDER).append("at", at);
        CloudEvent delivered = DocumentCloudEventReader.toCloudEvent(new Document("specversion", "1.0").append("id", "pojo-1").append("source", "urn:test")
                .append("type", converter.getCloudEventType(Stamped.class)).append("subject", ORDER).append("datacontenttype", "application/json").append("data", json));
        List<CloudEvent> seen = new CopyOnWriteArrayList<>();
        List<OrderEvent> converted = new CopyOnWriteArrayList<>();
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, recording("pojo-1", seen, event -> {
                    OrderEvent domainEvent = converter.toDomainEvent(event);
                    converted.add(domainEvent);
                    return domainEvent;
                }))
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("stamped");
        eventStore.write(STREAM, List.of(delivered));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(held()).hasSize(1));
        CloudEvent firstDelivery = seen.get(0);
        OrderEvent convertedOnDelivery = converted.get(0);
        int seenBeforeRelease = seen.size();

        failing.clear();
        subscription.release(ORDER);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(instance().status()).as("the release converted the held event and dispatched its command").isEqualTo(SagaStatus.ACTIVE));
        CloudEvent convertedByRelease = seen.subList(seenBeforeRelease, seen.size()).getLast();

        assertAll(
                () -> assertThat(firstDelivery.getData()).as("the window: the saga is delivered the data as a map, the way the MongoDB event store delivers it")
                        .isInstanceOfSatisfying(PojoCloudEventData.class, data -> assertThat(data.getValue()).isInstanceOf(Map.class)),
                () -> assertThat(convertedOnDelivery).as("the window: the delivery converted the map to the domain event").isEqualTo(new Stamped(ORDER, at)),
                () -> assertThat(converted.getLast()).as("the release converts to the same domain event the delivery did").isEqualTo(convertedOnDelivery),
                () -> assertThat(convertedByRelease.getData()).as("the converter is given the data as a map when the release replays the event")
                        .isInstanceOfSatisfying(PojoCloudEventData.class, data -> assertThat(data.getValue()).isEqualTo(((PojoCloudEventData<?>) requireNonNull(firstDelivery.getData())).getValue())),
                () -> assertThat(requireNonNull(convertedByRelease.getData()).toBytes()).as("the released data bytes equal the delivered bytes").isEqualTo(requireNonNull(firstDelivery.getData()).toBytes()),
                () -> assertThat(dispatched).as("the release dispatches the command of the held event").containsExactly(new Pack("stamped", 1))
        );
    }

    // Values and Java types of the extensions, with bytes by their contents
    private static Map<String, List<Object>> typedExtensions(CloudEvent event) {
        return event.getExtensionNames().stream().collect(Collectors.toMap(name -> name, name -> {
            Object value = requireNonNull(event.getExtension(name));
            return List.of(value.getClass(), value instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : value);
        }));
    }

    // Keeps the events with the id it is given, and reads them with the function
    private CloudEventConverter<OrderEvent> recording(String id, List<CloudEvent> seen, Function<CloudEvent, OrderEvent> read) {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                return converter.toCloudEvent(domainEvent);
            }

            @Override
            public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
                if (!id.equals(cloudEvent.getId())) {
                    return converter.toDomainEvent(cloudEvent);
                }
                seen.add(cloudEvent);
                return read.apply(cloudEvent);
            }

            @Override
            public String getCloudEventType(Class<? extends OrderEvent> type) {
                return converter.getCloudEventType(type);
            }
        };
    }

    private List<SkippedInput> held() {
        return stateStore.findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE);
    }
}
