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
import io.cloudevents.CloudEventExtension;
import io.cloudevents.CloudEventExtensions;
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
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A release hands the converter a held event exactly as it was delivered, or the record of the input is
 * refused, naming the instance, the input and the value, before anything is written. Checks against a MongoDB replica
 * set that each class of extension the record holds reads back as the same class with the same value, that an extension
 * of any other class is refused, that a map with a value BSON reads back as another class, a {@code BigDecimal} for one,
 * is refused, and that data of a class a release can't rebuild, such as {@code JsonCloudEventData}, is refused while
 * {@code BytesCloudEventData} is held and read back as itself.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreHeldEventClassTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-held-event-class";
    private static final String SKIPPED_INPUTS = COLLECTION + "_skipped_inputs";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";
    private static final String QUARANTINE_ID = "q-1";
    private static final String KEY = "order-1@3";
    private static final Instant NOW = Instant.parse("2026-03-04T05:16:17.456Z");
    private static final BigDecimal LONG_DECIMAL = new BigDecimal("1234567890.123456789");

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item, Object price) implements OrderEvent {
    }

    record Pack(String item) {
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
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-held-class-" + UUID.randomUUID()));
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

    @Test
    void a_saga_input_whose_map_data_holds_a_BigDecimal_is_refused_with_the_field_named_and_nothing_written_or_dispatched() {
        List<SkippedInput> attempted = new CopyOnWriteArrayList<>();
        List<IllegalStateException> refused = new CopyOnWriteArrayList<>();
        @SuppressWarnings("unchecked")
        SpringMongoSagaStateStore<String> observed = Mockito.mock(SpringMongoSagaStateStore.class, Mockito.withSettings().spiedInstance(stateStore).defaultAnswer(invocation -> {
            try {
                return invocation.callRealMethod();
            } catch (Throwable e) {
                if ("compareAndSaveSkipped".equals(invocation.getMethod().getName()) && e instanceof IllegalStateException refusal) {
                    attempted.add((SkippedInput) invocation.getArguments()[3]);
                    refused.add(refusal);
                }
                throw e;
            }
        }));
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, mapDataConverter())
                .run("orders", saga(), observed, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        eventStore.write(STREAM, converter.toCloudEvents(List.of(new OrderPlaced(ORDER))));
        failing.add("priced");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("price", LONG_DECIMAL);
        eventStore.write(STREAM, List.of(CloudEventBuilder.v1().withId("priced-1").withSource(URI.create("urn:test")).withType(converter.getCloudEventType(ItemAdded.class))
                .withSubject(ORDER).withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build()));

        await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(refused).isNotEmpty());
        eventStore.write(STREAM, converter.toCloudEvents(List.of(new ItemAdded(ORDER, "pen", 5))));

        await().dontCatchUncaughtExceptions().ignoreExceptions().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(dispatched).as("nothing is dispatched for the event or for the one after it").isEmpty());
        IllegalStateException thrown = refused.get(0);
        assertAll(
                () -> assertThat(thrown).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("'" + attempted.get(0).key() + "'").hasMessageContaining("'price'")
                        .hasMessageContaining("Nothing is written for the input"),
                () -> assertThat(instance().status()).as("the instance is not quarantined, since its first skipped input has no record").isNotEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(template.getCollection(SKIPPED_INPUTS).countDocuments()).as("no record is written").isZero()
        );
    }

    @Test
    void a_map_data_value_BSON_reads_back_as_another_class_is_refused_at_a_nested_path() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("amount", LONG_DECIMAL);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("lines", List.of(Map.of("name", "pen"), nested));
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("'" + KEY + "'").hasMessageContaining("'lines[1].amount'"),
                () -> assertNothingWritten()
        );
    }

    @Test
    void a_String_extension_with_an_unpaired_surrogate_is_refused_with_the_extension_named_and_nothing_written() {
        CloudEvent event = baseEvent().withExtension("surrogateext", "a\uD800b").build();
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("'" + KEY + "'")
                        .hasMessageContaining("'surrogateext'").hasMessageContaining(String.class.getName()),
                () -> assertNothingWritten()
        );
    }

    @Test
    void map_data_whose_nested_map_has_a_key_that_is_not_a_String_is_refused_with_the_path_named_and_nothing_written() {
        Map<Object, Object> nested = new LinkedHashMap<>();
        nested.put(7, "pen");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("line", nested);
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("'" + KEY + "'")
                        .hasMessageContaining("'line' of its data"),
                () -> assertNothingWritten()
        );
    }

    @Test
    void map_data_with_maps_inside_a_list_is_held_and_read_back_with_every_leaf_of_the_same_class_and_value() {
        Map<String, Object> pen = new LinkedHashMap<>();
        pen.put("name", "pen");
        pen.put("quantity", Integer.valueOf(3));
        pen.put("sku", Long.valueOf(9_000_000_000L));
        pen.put("price", Double.valueOf(1.25d));
        pen.put("gift", Boolean.TRUE);
        Map<String, Object> ink = new LinkedHashMap<>();
        ink.put("name", "ink");
        ink.put("quantity", Integer.valueOf(-1));
        ink.put("sku", Long.valueOf(-5L));
        ink.put("price", Double.valueOf(0.1d));
        ink.put("gift", Boolean.FALSE);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("lines", List.of(pen, ink));
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();

        CloudEvent held = heldAndRead(event);

        assertThat(held.getData()).isInstanceOf(PojoCloudEventData.class);
        Object value = ((PojoCloudEventData<?>) requireNonNull(held.getData())).getValue();
        assertThat(value).isInstanceOf(Map.class);
        Map<?, ?> map = (Map<?, ?>) value;
        assertThat(map.keySet()).isEqualTo(Set.of("orderId", "lines"));
        assertThat(map.get("lines")).isInstanceOf(List.class);
        List<?> lines = (List<?>) map.get("lines");
        assertThat(lines).hasSize(2);
        assertAll(
                () -> assertThat(map.get("orderId")).isInstanceOf(String.class).isEqualTo(ORDER),
                () -> assertLeaves(lines.get(0), pen),
                () -> assertLeaves(lines.get(1), ink)
        );
    }

    @Test
    void integral_extensions_are_restored_to_their_class_and_value() {
        CloudEvent held = heldAndRead(baseEvent()
                .withExtension("byteext", (Number) Byte.valueOf((byte) -7))
                .withExtension("shortext", (Number) Short.valueOf((short) 31_000))
                .withExtension("intext", Integer.valueOf(-123_456))
                .withExtension("longext", (Number) Long.valueOf(Long.MIN_VALUE))
                .withExtension("biginteger", (Number) new BigInteger("123456789012345678901234567890"))
                .build());

        assertAll(
                restored(held, "byteext", Byte.valueOf((byte) -7)),
                restored(held, "shortext", Short.valueOf((short) 31_000)),
                restored(held, "intext", Integer.valueOf(-123_456)),
                restored(held, "longext", Long.valueOf(Long.MIN_VALUE)),
                restored(held, "biginteger", new BigInteger("123456789012345678901234567890"))
        );
    }

    @Test
    void a_Float_extension_is_restored_to_a_Float_with_its_value() {
        CloudEvent held = heldAndRead(baseEvent().withExtension("floatext", (Number) Float.valueOf(1.5f)).withExtension("floatsmall", (Number) Float.valueOf(1.0e-30f)).build());

        assertAll(
                restored(held, "floatext", Float.valueOf(1.5f)),
                restored(held, "floatsmall", Float.valueOf(1.0e-30f))
        );
    }

    @Test
    void a_Float_extension_that_is_not_a_number_infinite_or_negative_zero_is_restored_to_a_Float_with_the_same_bits() {
        CloudEvent held = heldAndRead(baseEvent()
                .withExtension("floatnan", (Number) Float.NaN)
                .withExtension("floatpositiveinfinity", (Number) Float.POSITIVE_INFINITY)
                .withExtension("floatnegativeinfinity", (Number) Float.NEGATIVE_INFINITY)
                .withExtension("floatnegativezero", (Number) Float.valueOf(-0.0f))
                .build());

        assertAll(
                restored(held, "floatnan", Float.NaN),
                restored(held, "floatpositiveinfinity", Float.POSITIVE_INFINITY),
                restored(held, "floatnegativeinfinity", Float.NEGATIVE_INFINITY),
                restored(held, "floatnegativezero", Float.valueOf(-0.0f))
        );
    }

    @Test
    void a_Double_extension_including_not_a_number_and_the_infinities_is_restored_to_a_Double_with_its_value() {
        CloudEvent held = heldAndRead(baseEvent()
                .withExtension("doubleext", (Number) Double.valueOf(0.1d))
                .withExtension("doublenan", (Number) Double.NaN)
                .withExtension("doublepositiveinfinity", (Number) Double.POSITIVE_INFINITY)
                .withExtension("doublenegativeinfinity", (Number) Double.NEGATIVE_INFINITY)
                .build());

        assertAll(
                restored(held, "doubleext", Double.valueOf(0.1d)),
                restored(held, "doublenan", Double.NaN),
                restored(held, "doublepositiveinfinity", Double.POSITIVE_INFINITY),
                restored(held, "doublenegativeinfinity", Double.NEGATIVE_INFINITY)
        );
    }

    @Test
    void a_BigDecimal_extension_is_restored_to_a_BigDecimal_with_its_value_and_scale() {
        CloudEvent held = heldAndRead(baseEvent()
                .withExtension("decimalext", (Number) LONG_DECIMAL)
                .withExtension("decimalnegativescale", (Number) new BigDecimal("1E+3"))
                .build());

        assertAll(
                restored(held, "decimalext", LONG_DECIMAL),
                restored(held, "decimalnegativescale", new BigDecimal("1E+3"))
        );
    }

    @Test
    void a_String_a_Boolean_a_byte_array_a_URI_and_an_OffsetDateTime_extension_are_restored_to_their_class_and_value() {
        byte[] bytes = {0, 1, 2, (byte) 0xff, (byte) 0x80};
        OffsetDateTime time = OffsetDateTime.parse("2026-03-04T05:06:07.123456789+05:30");
        CloudEvent held = heldAndRead(baseEvent()
                .withExtension("stringext", "value")
                .withExtension("booleanext", Boolean.TRUE)
                .withExtension("bytesext", bytes)
                .withExtension("uriext", URI.create("urn:test:extension?a=b#c"))
                .withExtension("timeext", time)
                .build());

        assertAll(
                restored(held, "stringext", "value"),
                restored(held, "booleanext", Boolean.TRUE),
                restored(held, "bytesext", bytes),
                restored(held, "uriext", URI.create("urn:test:extension?a=b#c")),
                restored(held, "timeext", time)
        );
    }

    @Test
    void an_extension_of_a_class_the_record_does_not_hold_is_refused_with_the_extension_and_its_class_named_and_nothing_written() {
        CloudEvent event = baseEvent().withExtension("counter", (Number) new AtomicLong(7)).build();
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("'" + KEY + "'")
                        .hasMessageContaining("'counter'").hasMessageContaining(AtomicLong.class.getName()),
                () -> assertNothingWritten()
        );
    }

    @Test
    void an_extension_with_a_UUID_value_from_a_CloudEventExtension_is_refused_with_the_extension_and_its_class_named_and_nothing_written() {
        CloudEvent event = baseEvent().withExtension(new UuidExtension("correlation", UUID.randomUUID())).build();
        assertThat(event.getExtension("correlation")).as("the event the store is handed carries the UUID").isInstanceOf(UUID.class);
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("'" + KEY + "'")
                        .hasMessageContaining("'correlation'").hasMessageContaining(UUID.class.getName()),
                () -> assertNothingWritten()
        );
    }

    @Test
    void json_data_the_cloudevents_json_format_deserialized_is_refused_with_nothing_written() {
        CloudEvent event = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE).deserialize(("{\"specversion\":\"1.0\",\"id\":\"json-1\",\"source\":\"urn:test\","
                + "\"type\":\"test\",\"subject\":\"" + ORDER + "\",\"datacontenttype\":\"application/json\",\"data\":{\"orderId\":\"" + ORDER + "\",\"value\":42}}").getBytes(UTF_8));
        assertThat(requireNonNull(event.getData())).as("the event the store is handed carries JsonCloudEventData").isInstanceOf(JsonCloudEventData.class);
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("'" + KEY + "'")
                        .hasMessageContaining(JsonCloudEventData.class.getName()),
                () -> assertNothingWritten()
        );
    }

    @Test
    void data_that_is_a_BytesCloudEventData_is_held_and_read_back_as_a_BytesCloudEventData_with_its_bytes_and_content_type() {
        byte[] bytes = {0, 1, 2, (byte) 0xff, (byte) 0x80, 'a'};
        CloudEvent event = baseEvent().withDataContentType("application/octet-stream").withData(BytesCloudEventData.wrap(bytes)).build();

        CloudEvent held = heldAndRead(event);

        assertAll(
                () -> assertThat(requireNonNull(held.getData()).getClass()).isEqualTo(BytesCloudEventData.class),
                () -> assertThat(Arrays.equals(requireNonNull(held.getData()).toBytes(), bytes)).as("the bytes").isTrue(),
                () -> assertThat(held.getDataContentType()).isEqualTo("application/octet-stream")
        );
    }

    @Test
    void map_data_holding_two_equal_keys_is_refused_and_nothing_written() {
        assertRefusedAsCollapsing(identityMapWithTwoEqualKeys());
    }

    @Test
    void map_data_holding_two_equal_keys_in_a_nested_map_is_refused_and_nothing_written() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("inner", identityMapWithTwoEqualKeys());
        assertRefusedAsCollapsing(data);
    }

    @Test
    void map_data_holding_two_equal_keys_in_a_map_inside_a_list_is_refused_and_nothing_written() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("parts", List.of(identityMapWithTwoEqualKeys()));
        assertRefusedAsCollapsing(data);
    }

    @Test
    void map_data_that_is_a_comparator_ordered_map_holding_two_equal_keys_is_refused_and_nothing_written() {
        Map<Object, Object> data = new TreeMap<>((a, b) -> -1);
        data.put("item", "first");
        data.put("item", "second");
        assertThat(data).hasSize(2);
        assertRefusedAsCollapsing(data);
    }

    private void assertRefusedAsCollapsing(Map<?, ?> data) {
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("'" + KEY + "'")
                        .hasMessageContaining("keys [item] where it was delivered with [item, item]"),
                () -> assertNothingWritten()
        );
    }

    private static Map<Object, Object> identityMapWithTwoEqualKeys() {
        Map<Object, Object> map = new IdentityHashMap<>();
        map.put(new String("item"), "first");
        map.put(new String("item"), "second");
        assertThat(map).hasSize(2);
        return map;
    }

    private CloudEventBuilder baseEvent() {
        return CloudEventBuilder.v1().withId("held-1").withSource(URI.create("urn:test")).withType("test").withSubject(ORDER);
    }

    private StoredSaga anInstance() {
        SagaEnvelope<String> envelope = new SagaEnvelope<>(ORDER, "placed", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);
        assertThat(SagaEnvelopes.of(stateStore, null).compareAndSave(ORDER, envelope, 0)).isTrue();
        return stateStore.find(ORDER).orElseThrow();
    }

    private CloudEvent heldAndRead(CloudEvent event) {
        StoredSaga instance = anInstance();
        AtomicReference<Boolean> saved = new AtomicReference<>();
        Throwable savedFailure = catchThrowable(() -> saved.set(stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event))));
        assertThat(savedFailure).as("recording the input").isNull();
        assertThat(saved.get()).as("the record is written").isTrue();
        AtomicReference<List<SkippedInput>> read = new AtomicReference<>();
        Throwable readFailure = catchThrowable(() -> read.set(stateStore.findSkippedInputs(ORDER, QUARANTINE_ID, 10)));
        assertThat(readFailure).as("reading the record back").isNull();
        assertThat(read.get()).hasSize(1);
        return read.get().get(0).event();
    }

    private void assertNothingWritten() {
        assertAll(
                () -> assertThat(template.getCollection(SKIPPED_INPUTS).countDocuments()).as("no record is written").isZero(),
                () -> assertThat(stateStore.find(ORDER).orElseThrow().version()).as("the instance is not written").isEqualTo(1L)
        );
    }

    private static void assertLeaves(Object restoredLine, Map<String, Object> delivered) {
        assertThat(restoredLine).isInstanceOf(Map.class);
        Map<?, ?> line = (Map<?, ?>) restoredLine;
        assertThat(line.keySet()).isEqualTo(delivered.keySet());
        assertAll(delivered.entrySet().stream().map(leaf -> (Executable) () -> {
            Object actual = line.get(leaf.getKey());
            assertThat(actual).as(leaf.getKey()).isNotNull();
            assertThat(actual.getClass()).as(leaf.getKey() + " class").isEqualTo(leaf.getValue().getClass());
            assertThat(actual).as(leaf.getKey()).isEqualTo(leaf.getValue());
        }));
    }

    private static Executable restored(CloudEvent held, String name, Object expected) {
        return () -> {
            Object actual = held.getExtension(name);
            assertThat(actual).as("extension '" + name + "'").isNotNull();
            assertThat(requireNonNull(actual).getClass()).as("class of extension '" + name + "'").isEqualTo(expected.getClass());
            if (expected instanceof byte[] bytes) {
                assertThat(Arrays.equals((byte[]) actual, bytes)).as("value of extension '" + name + "'").isTrue();
            } else {
                assertThat(actual).as("value of extension '" + name + "'").isEqualTo(expected);
            }
        };
    }

    private SagaEnvelope<String> instance() {
        return SagaEnvelopes.of(stateStore, null).find(ORDER).orElseThrow();
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
                    return List.of(SagaEffect.issue(new Pack(e.item())));
                })
                .build();
    }

    // Reads the event "priced-1", whose data is a map, as an item named "priced", and every other event with the converter
    private CloudEventConverter<OrderEvent> mapDataConverter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                return converter.toCloudEvent(domainEvent);
            }

            @Override
            public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
                if ("priced-1".equals(cloudEvent.getId()) && requireNonNull(cloudEvent.getData()) instanceof PojoCloudEventData<?> pojo && pojo.getValue() instanceof Map<?, ?> map) {
                    return new ItemAdded((String) map.get("orderId"), "priced", map.get("price"));
                }
                return converter.toDomainEvent(cloudEvent);
            }

            @Override
            public String getCloudEventType(Class<? extends OrderEvent> type) {
                return converter.getCloudEventType(type);
            }
        };
    }

    private record UuidExtension(String name, UUID value) implements CloudEventExtension {
        @Override
        public void readFrom(CloudEventExtensions extensions) {
        }

        @Override
        public Object getValue(String key) {
            return value;
        }

        @Override
        public Set<String> getKeys() {
            return Set.of(name);
        }
    }
}
