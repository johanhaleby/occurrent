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

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonCloudEventData;
import io.cloudevents.jackson.JsonFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * For every event a quarantined instance skips, whatever the class and the size of its data, the store holds it, and a
 * release hands the converter an event equal to the one delivered. The events come from an in-memory event store, which
 * delivers data of any class and any size, where a MongoDB event store cannot hold an event over 16 MiB. A store that
 * holds the data as its bytes hands the converter a {@code BytesCloudEventData} with the same bytes, or a
 * {@code JsonCloudEventData} for data that was one.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(180)
abstract class SkippedDataReleaseContract {

    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";
    private static final String OTHER_ORDER = "order-2";
    private static final String TRIGGER = "trigger";
    private static final OffsetDateTime HAPPENED_AT = OffsetDateTime.parse("2026-01-01T10:15:30.123Z");
    private static final int OVER_A_DOCUMENT = 17 * 1024 * 1024;

    sealed interface OrderEvent permits OrderPlaced, Probe {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record Probe(String orderId, String id) implements OrderEvent {
    }

    record Pack(String id) {
    }

    record Address(String city) {
    }

    static final class CustomData implements CloudEventData {
        private final byte[] bytes;

        CustomData(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public byte[] toBytes() {
            return bytes.clone();
        }
    }

    /** A store of the kind under test, empty and used by one test */
    abstract SagaStateStore<String> newStore();

    /** Whether the store copies a map that holds a BigDecimal or a key that is not a String as a PojoCloudEventData, where the other falls back to bytes */
    abstract boolean copiesMapsBsonCannotHold();

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private SagaStateStore<String> store;
    private SagaSubscription subscription;
    private final List<Pack> dispatched = new CopyOnWriteArrayList<>();
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    private final List<CloudEvent> converted = new CopyOnWriteArrayList<>();

    private final CloudEventConverter<OrderEvent> converter = new CloudEventConverter<>() {
        @Override
        public CloudEvent toCloudEvent(OrderEvent domainEvent) {
            return placed(domainEvent.orderId());
        }

        @Override
        public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
            if ("placed".equals(cloudEvent.getType())) {
                return new OrderPlaced(requireNonNull(cloudEvent.getSubject()));
            }
            converted.add(cloudEvent);
            return new Probe(requireNonNull(cloudEvent.getSubject()), cloudEvent.getId());
        }

        @Override
        public String getCloudEventType(Class<? extends OrderEvent> type) {
            return type == OrderPlaced.class ? "placed" : "probe";
        }
    };

    @BeforeEach
    void startTheSaga() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        store = newStore();
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), store, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
    }

    @AfterEach
    void shutdown() {
        subscription.close();
        subscriptionModel.shutdown();
    }

    @Test
    void json_data_is_held_released_and_equal_to_the_original_as_JsonCloudEventData() {
        CloudEvent delivered = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE).deserialize(("{\"specversion\":\"1.0\",\"id\":\"case\",\"source\":\"urn:test\","
                + "\"type\":\"probe\",\"subject\":\"" + ORDER + "\",\"datacontenttype\":\"application/json\",\"data\":{\"orderId\":\"" + ORDER + "\",\"value\":42,\"nested\":{\"amount\":1.5}}}").getBytes(UTF_8));
        assertThat(delivered.getData()).as("the event is delivered with JsonCloudEventData").isInstanceOf(JsonCloudEventData.class);

        assertHeldAndReleasedAsDelivered(delivered, JsonCloudEventData.class);
    }

    @Test
    void a_map_holding_a_BigDecimal_at_depth_is_held_released_and_equal_to_the_original() {
        Map<String, Object> amount = new LinkedHashMap<>();
        amount.put("amount", new BigDecimal("1234567890.123456789"));
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("lines", List.of(Map.of("name", "pen"), amount));

        assertHeldAndReleasedAsDelivered(event("case", PojoCloudEventData.wrap(data, ignored -> "{\"lines\":2}".getBytes(UTF_8))),
                copiesMapsBsonCannotHold() ? PojoCloudEventData.class : BytesCloudEventData.class);
    }

    @Test
    void a_map_with_a_key_that_is_not_a_String_is_held_released_and_equal_to_the_original() {
        Map<Object, Object> data = new LinkedHashMap<>();
        data.put(7, "pen");

        assertHeldAndReleasedAsDelivered(event("case", PojoCloudEventData.wrap(data, ignored -> "{\"7\":\"pen\"}".getBytes(UTF_8))),
                copiesMapsBsonCannotHold() ? PojoCloudEventData.class : BytesCloudEventData.class);
    }

    @Test
    void a_map_holding_two_equal_keys_is_held_released_and_equal_to_the_original_as_bytes() {
        Map<Object, Object> data = new IdentityHashMap<>();
        data.put(new String("item"), "first");
        data.put(new String("item"), "second");
        assertThat(data).hasSize(2);

        assertHeldAndReleasedAsDelivered(event("case", PojoCloudEventData.wrap(data, ignored -> "{\"item\":[\"first\",\"second\"]}".getBytes(UTF_8))), BytesCloudEventData.class);
    }

    @Test
    void a_plain_object_is_held_released_and_equal_to_the_original_as_bytes() {
        assertHeldAndReleasedAsDelivered(event("case", PojoCloudEventData.wrap(new Address("Malmo"), address -> ("{\"city\":\"" + address.city() + "\"}").getBytes(UTF_8))), BytesCloudEventData.class);
    }

    @Test
    void data_of_a_custom_class_is_held_released_and_equal_to_the_original_as_bytes() {
        assertHeldAndReleasedAsDelivered(event("case", new CustomData(new byte[]{0, 1, 2, (byte) 0xff, 'a'})), BytesCloudEventData.class);
    }

    @Test
    void an_event_over_16_MiB_is_held_released_and_equal_to_the_original() {
        byte[] bytes = new byte[OVER_A_DOCUMENT];
        new Random(11).nextBytes(bytes);

        assertHeldAndReleasedAsDelivered(event("case", BytesCloudEventData.wrap(bytes)), BytesCloudEventData.class);
    }

    @Test
    void a_map_from_another_feed_held_as_a_document_and_its_bytes_whose_record_goes_over_16_MiB_is_held_released_and_equal_to_the_original() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("blob", "x".repeat(9 * 1024 * 1024));
        byte[] bytes = new byte[8 * 1024 * 1024];
        new Random(13).nextBytes(bytes);

        assertHeldAndReleasedAsDelivered(event("case", PojoCloudEventData.wrap(data, ignored -> bytes.clone())), PojoCloudEventData.class);
    }

    @Test
    void an_event_over_16_MiB_does_not_stop_the_subscription_and_the_instance_keeps_skipping_what_comes_after_it() {
        quarantineTheInstance();
        write(event("big", BytesCloudEventData.wrap(new byte[OVER_A_DOCUMENT])));

        write(event("after", BytesCloudEventData.wrap(new byte[]{1})));
        write(placed(OTHER_ORDER));
        write(eventFor(OTHER_ORDER, "later", BytesCloudEventData.wrap(new byte[]{2})));

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(dispatched).as("another instance is still served").containsExactly(new Pack("later")));
        assertThat(held()).as("the instance holds the event that failed, the oversized one and the one after it").extracting(SkippedInput::key).hasSize(3);
    }

    @Test
    void data_that_does_not_round_trip_does_not_stop_the_subscription_and_the_instance_keeps_skipping_what_comes_after_it() {
        quarantineTheInstance();
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("price", new BigDecimal("1.10"));
        write(event("decimal", PojoCloudEventData.wrap(data, ignored -> "{}".getBytes(UTF_8))));

        write(event("after", BytesCloudEventData.wrap(new byte[]{1})));
        write(placed(OTHER_ORDER));
        write(eventFor(OTHER_ORDER, "later", BytesCloudEventData.wrap(new byte[]{2})));

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(dispatched).as("another instance is still served").containsExactly(new Pack("later")));
        assertThat(held()).as("the instance holds the event that failed, the one that does not round trip and the one after it").hasSize(3);
    }

    private void assertHeldAndReleasedAsDelivered(CloudEvent delivered, Class<? extends CloudEventData> releasedAs) {
        quarantineTheInstance();
        write(delivered);
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(held()).as("the trigger and the event").hasSize(2));
        CloudEvent original = stored(delivered.getId());
        CloudEvent held = held().stream().map(SkippedInput::event).filter(event -> event.getId().equals(delivered.getId())).findFirst().orElseThrow();

        converted.clear();
        failing.clear();
        subscription.release(ORDER);
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE));
        CloudEvent released = converted.stream().filter(event -> event.getId().equals(delivered.getId())).findFirst().orElseThrow();

        assertAll(
                () -> assertSameEvent("held", original, held, releasedAs),
                () -> assertSameEvent("released", original, released, releasedAs),
                () -> assertThat(dispatched).as("the release applied the trigger and the event").containsExactly(new Pack(TRIGGER), new Pack(delivered.getId()))
        );
    }

    private static void assertSameEvent(String which, CloudEvent original, CloudEvent actual, Class<? extends CloudEventData> dataClass) {
        CloudEventData data = requireNonNull(actual.getData(), which + " data");
        CloudEventData originalData = requireNonNull(original.getData());
        assertAll(
                () -> assertThat(actual.getId()).as(which + " id").isEqualTo(original.getId()),
                () -> assertThat(actual.getType()).as(which + " type").isEqualTo(original.getType()),
                () -> assertThat(actual.getSource()).as(which + " source").isEqualTo(original.getSource()),
                () -> assertThat(actual.getSubject()).as(which + " subject").isEqualTo(original.getSubject()),
                () -> assertThat(actual.getTime()).as(which + " time").isEqualTo(original.getTime()),
                () -> assertThat(actual.getDataContentType()).as(which + " data content type").isEqualTo(original.getDataContentType()),
                () -> assertThat(actual.getExtensionNames()).as(which + " extension names").isEqualTo(original.getExtensionNames()),
                () -> assertThat(original.getExtensionNames()).as(which + " extension values")
                        .allSatisfy(name -> assertThat(Objects.deepEquals(actual.getExtension(name), original.getExtension(name))).as(name).isTrue()),
                () -> assertThat(data.getClass()).as(which + " data class").isEqualTo(dataClass),
                () -> assertThat(data.toBytes()).as(which + " data bytes").isEqualTo(originalData.toBytes()),
                () -> {
                    if (data instanceof JsonCloudEventData json) {
                        assertThat(json.getNode()).as(which + " data node").isEqualTo(((JsonCloudEventData) originalData).getNode());
                    } else if (data instanceof PojoCloudEventData<?> pojo) {
                        // Mongo reads a document back as an org.bson.Document, which is a Map of the same content but not equal to other Map classes
                        assertThat(pojo.getValue()).as(which + " data value").isInstanceOf(Map.class);
                        assertThat(new LinkedHashMap<>((Map<?, ?>) pojo.getValue())).as(which + " data value content").isEqualTo(new LinkedHashMap<>((Map<?, ?>) ((PojoCloudEventData<?>) originalData).getValue()));
                    }
                }
        );
    }

    private void quarantineTheInstance() {
        write(placed(ORDER));
        failing.add(TRIGGER);
        write(event(TRIGGER, BytesCloudEventData.wrap(new byte[]{9})));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
    }

    private List<SkippedInput> held() {
        SagaEnvelope<String> instance = instance();
        if (instance.failure() == null || instance.failure().quarantineId() == null) {
            return List.of();
        }
        return ((SagaStateStoreSkippedInputs) store).findSkippedInputs(ORDER, instance.failure().quarantineId(), 100);
    }

    // An absent instance is a failed assertion, so an await that polls before the saga has stored it retries
    private SagaEnvelope<String> instance() {
        return SagaEnvelopes.of(store, null).find(ORDER).orElseThrow(() -> new AssertionError("the instance is not stored yet"));
    }

    private CloudEvent stored(String id) {
        return eventStore.read(STREAM).eventList().stream().filter(event -> event.getId().equals(id)).findFirst().orElseThrow();
    }

    private void write(CloudEvent event) {
        eventStore.write(STREAM, List.of(event));
    }

    private static CloudEvent placed(String orderId) {
        return CloudEventBuilder.v1().withId("placed-" + orderId).withSource(URI.create("urn:test")).withType("placed").withSubject(orderId).build();
    }

    private static CloudEvent event(String id, CloudEventData data) {
        return eventFor(ORDER, id, data);
    }

    private static CloudEvent eventFor(String orderId, String id, CloudEventData data) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("probe").withSubject(orderId).withTime(HAPPENED_AT)
                .withDataContentType("application/json").withExtension("tenant", "acme").withExtension("attempt", 3).withData(data).build();
    }

    private Saga<OrderEvent, String, Pack> saga() {
        return Saga.<OrderEvent, String, Pack>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .evolve(Probe.class, (state, e) -> "probed")
                .react(Probe.class, (state, e) -> {
                    if (failing.contains(e.id())) {
                        throw new IllegalStateException("cannot apply " + e.id());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.id())));
                })
                .build();
    }
}
