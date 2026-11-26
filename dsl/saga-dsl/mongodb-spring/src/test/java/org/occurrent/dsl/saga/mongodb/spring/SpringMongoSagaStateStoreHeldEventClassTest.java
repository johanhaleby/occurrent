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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.CloudEventExtension;
import io.cloudevents.CloudEventExtensions;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonCloudEventData;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
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
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.io.UncheckedIOException;
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
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A release hands the converter a held event as it was delivered, or as the bytes of its data when the
 * data doesn't come back from BSON as the same values of the same classes. A record whose attribute or extension doesn't
 * come back is refused, naming the instance, the input and the value, before anything is written. Checks against a
 * MongoDB replica set that each class of extension the record holds reads back as the same class with the same value,
 * that a {@code String} with an unpaired surrogate reads back as the same {@code String}, that a {@code Number} of
 * another class reads back with its value as the class the store documents, with a warning, that an extension of any
 * other class is refused, and that data BSON reads back as another class, such as a
 * {@code BigDecimal} in a map, is held as its bytes and read back as {@code BytesCloudEventData}, where
 * {@code JsonCloudEventData} is read back as itself.
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
    void a_saga_input_whose_map_data_holds_a_BigDecimal_is_held_as_its_bytes_and_quarantines_the_instance_without_stopping_the_subscription() {
        List<IllegalStateException> refused = new CopyOnWriteArrayList<>();
        @SuppressWarnings("unchecked")
        SpringMongoSagaStateStore<String> observed = Mockito.mock(SpringMongoSagaStateStore.class, Mockito.withSettings().spiedInstance(stateStore).defaultAnswer(invocation -> {
            try {
                return invocation.callRealMethod();
            } catch (Throwable e) {
                if ("compareAndSaveSkipped".equals(invocation.getMethod().getName()) && e instanceof IllegalStateException refusal) {
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

        await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        eventStore.write(STREAM, converter.toCloudEvents(List.of(new ItemAdded(ORDER, "pen", 5))));

        await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(template.getCollection(SKIPPED_INPUTS).countDocuments()).as("the event it failed on and the one after it are held").isEqualTo(2));
        assertAll(
                () -> assertThat(refused).as("no record is refused, so the subscription is not stopped").isEmpty(),
                () -> assertThat(dispatched).as("nothing is dispatched while the instance is quarantined").isEmpty()
        );
    }

    @Test
    void a_map_data_value_BSON_reads_back_as_another_class_is_held_as_the_bytes_of_the_data_and_read_back_as_bytes() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("amount", LONG_DECIMAL);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("lines", List.of(Map.of("name", "pen"), nested));
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();

        CloudEvent held = heldAndRead(event);

        assertAll(
                () -> assertThat(held.getData()).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(requireNonNull(held.getData()).toBytes()).isEqualTo("{}".getBytes(UTF_8))
        );
    }

    @Test
    void a_String_with_an_unpaired_surrogate_in_map_data_is_held_as_the_bytes_of_the_data_and_read_back_as_the_delivered_bytes_with_a_warning() throws Exception {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("holder-key-3f9a", "a\uD800b");
        byte[] delivered = json(data);
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, SpringMongoSagaStateStoreHeldEventClassTest::json)).build();

        Held held = heldAndReadWarned(event);

        CloudEventData read = requireNonNull(held.event().getData());
        assertAll(
                () -> assertThat(read).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(read.toBytes()).as("the delivered bytes").isEqualTo(delivered),
                () -> assertThat(new ObjectMapper().readValue(read.toBytes(), Map.class).get("holder-key-3f9a")).as("the String the bytes decode to").isEqualTo("a\uD800b"),
                () -> assertThat(held.warnings()).singleElement().satisfies(warning -> assertThat(warning)
                        .contains("can hold the data of the input '" + KEY + "' it skips only as its bytes")
                        .contains("The value at 'entry 1' of its data, a java.lang.String, comes back as ")
                        .doesNotContain("holder-key-3f9a").doesNotContain("\uD800"))
        );
    }

    private static byte[] json(Object value) {
        try {
            return new ObjectMapper().writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void a_String_extension_with_an_unpaired_surrogate_is_held_and_released_as_the_same_String() {
        CloudEvent held = heldAndRead(baseEvent().withExtension("surrogateext", "a\uD800b").build());

        assertAll(
                restored(held, "surrogateext", "a\uD800b"),
                () -> assertThat(String.valueOf(held.getExtension("surrogateext")).toCharArray()).as("the lone surrogate is still there").containsExactly('a', '\uD800', 'b'),
                () -> assertThat(storedExtension("surrogateext")).as("the stored extension").containsEntry("type", "utf16"),
                () -> assertThat(storedExtension("surrogateext").get("value") instanceof Binary binary ? binary.getData() : storedExtension("surrogateext").get("value"))
                        .as("the UTF-16BE code units of the stored extension").isEqualTo(new byte[]{0, 'a', (byte) 0xD8, 0, 0, 'b'})
        );
    }

    @Test
    void String_extensions_with_a_lone_low_surrogate_a_reversed_pair_and_a_lone_high_surrogate_after_a_pair_are_held_and_released_as_the_same_String() {
        String lowOnly = "\uDC00";
        String reversed = "x\uDC00\uD800y";
        String afterPair = "😀\uD800";
        CloudEvent held = heldAndRead(baseEvent().withExtension("lowonly", lowOnly).withExtension("reversed", reversed).withExtension("afterpair", afterPair).build());

        assertAll(
                restored(held, "lowonly", lowOnly),
                restored(held, "reversed", reversed),
                restored(held, "afterpair", afterPair)
        );
    }

    @Test
    void an_id_with_an_unpaired_surrogate_is_held_and_released_as_the_same_id() {
        assertAttributeHeldAndReleased(baseEvent().withId("a\uD800b"), CloudEvent::getId, "a\uD800b");
    }

    @Test
    void a_type_with_an_unpaired_surrogate_is_held_and_released_as_the_same_type() {
        assertAttributeHeldAndReleased(baseEvent().withType("a\uD800b"), CloudEvent::getType, "a\uD800b");
    }

    @Test
    void a_subject_with_an_unpaired_surrogate_is_held_and_released_as_the_same_subject() {
        assertAttributeHeldAndReleased(baseEvent().withSubject("a\uD800b"), CloudEvent::getSubject, "a\uD800b");
    }

    @Test
    void a_source_URI_with_an_unpaired_surrogate_is_held_and_released_as_the_same_URI() {
        URI source = URI.create("urn:a\uD800b");
        assertAttributeHeldAndReleased(baseEvent().withSource(source), CloudEvent::getSource, source);
    }

    @Test
    void a_dataschema_URI_with_an_unpaired_surrogate_is_held_and_released_as_the_same_URI() {
        URI schema = URI.create("urn:a\uD800b");
        assertAttributeHeldAndReleased(baseEvent().withDataSchema(schema), CloudEvent::getDataSchema, schema);
    }

    @Test
    void map_data_whose_nested_map_has_a_key_that_is_not_a_String_is_held_as_the_bytes_of_the_data_and_read_back_as_bytes() {
        Map<Object, Object> nested = new LinkedHashMap<>();
        nested.put(7, "pen");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("line", nested);
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();

        CloudEvent held = heldAndRead(event);

        assertAll(
                () -> assertThat(held.getData()).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(requireNonNull(held.getData()).toBytes()).isEqualTo("{}".getBytes(UTF_8))
        );
    }

    @Test
    void the_warning_for_data_held_as_its_bytes_names_the_position_and_classes_of_the_value_and_neither_a_key_nor_a_value_of_the_event() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("balance-key-4c8d", new BigDecimal("9876543210.987654321"));
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("holder-key-7b2e", inner);
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();
        Logger storeLog = (Logger) LoggerFactory.getLogger(SpringMongoSagaStateStore.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        storeLog.addAppender(appender);
        try {
            heldAndRead(event);
        } finally {
            storeLog.detachAppender(appender);
            appender.stop();
        }

        List<String> warnings = appender.list.stream().filter(logged -> logged.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(warnings).singleElement().satisfies(warning -> assertThat(warning)
                .contains("can hold the data of the input '" + KEY + "' it skips only as its bytes")
                .contains("The value at 'entry 1 > entry 0' of its data, a java.math.BigDecimal, comes back as a ")
                .doesNotContain("balance-key-4c8d").doesNotContain("holder-key-7b2e").doesNotContain("9876543210"));
    }

    @Test
    void the_refusal_of_json_data_that_comes_back_as_another_value_names_the_position_and_classes_and_neither_a_field_name_nor_a_value_of_the_event() {
        ObjectNode inner = new ObjectMapper().createObjectNode();
        inner.putArray("list-field-8a1d").add("text-value-5e11").add(new byte[]{1, 2, 3});
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.set("holder-field-2f6e", inner);
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(JsonCloudEventData.wrap(node)).build();
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("The value at 'field 0 > field 0 > element 1' of its JSON data")
                        .hasMessageContaining("BinaryNode")
                        .message().doesNotContain("holder-field-2f6e").doesNotContain("list-field-8a1d").doesNotContain("text-value-5e11"),
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
    void an_AtomicLong_extension_is_held_and_released_as_a_Long_with_its_value_and_a_warning_that_names_both_classes_and_no_value() {
        assertReleasedAs(new AtomicLong(1_234_567), Long.valueOf(1_234_567));

        assertThat(storedExtension("number")).as("the stored extension").isEqualTo(new Document("type", "number").append("class", AtomicLong.class.getName()).append("value", 1_234_567L));
    }

    @Test
    void an_integral_extension_of_a_custom_Number_class_is_held_and_released_as_a_Long_with_its_value() {
        assertReleasedAs(new Reading("-9223372036854775808"), Long.valueOf(Long.MIN_VALUE));
    }

    @Test
    void a_fractional_extension_of_a_custom_Number_class_is_held_as_its_decimal_string_and_released_as_a_BigDecimal_with_its_value_and_scale() {
        Object released = assertReleasedAs(new Reading("1234567890.123456789"), new BigDecimal("1234567890.123456789"));

        assertAll(
                () -> assertThat(((BigDecimal) released).scale()).as("the scale of the released value").isEqualTo(9),
                () -> assertThat(storedExtension("number").get("value")).as("the stored value").isEqualTo("1234567890.123456789")
        );
    }

    @Test
    void a_fractional_extension_of_a_custom_Number_class_with_trailing_zeros_is_released_as_a_BigDecimal_with_the_same_scale() {
        Object released = assertReleasedAs(new Reading("2.50"), new BigDecimal("2.50"));

        assertAll(
                () -> assertThat(((BigDecimal) released).scale()).as("the scale of the released value").isEqualTo(2),
                () -> assertThat(storedExtension("number").get("value")).as("the stored value").isEqualTo("2.50")
        );
    }

    @Test
    void an_extension_that_is_a_subclass_of_BigDecimal_with_trailing_zeros_is_held_under_the_bigDecimal_tag_and_released_as_a_plain_BigDecimal_with_its_scale_and_a_warning() {
        assertSubclassedBigDecimalReleasedAsPlain("10.00");
    }

    @Test
    void an_extension_that_is_a_subclass_of_BigDecimal_with_a_fraction_is_held_under_the_bigDecimal_tag_and_released_as_a_plain_BigDecimal_with_its_scale_and_a_warning() {
        assertSubclassedBigDecimalReleasedAsPlain("10.50");
    }

    @Test
    void an_extension_that_is_a_subclass_of_BigInteger_inside_the_Long_range_is_held_under_the_bigInteger_tag_and_released_as_a_plain_BigInteger_and_a_warning() {
        assertSubclassedBigIntegerReleasedAsPlain("12345");
    }

    @Test
    void an_extension_that_is_a_subclass_of_BigInteger_outside_the_Long_range_is_held_under_the_bigInteger_tag_and_released_as_a_plain_BigInteger_and_a_warning() {
        assertSubclassedBigIntegerReleasedAsPlain("123456789012345678901234567890");
    }

    private void assertSubclassedBigDecimalReleasedAsPlain(String value) {
        Held held = heldAndReadWarned(baseEvent().withExtension("number", (Number) new SubclassedBigDecimal(value)).build());

        Object released = held.event().getExtension("number");
        assertAll(
                () -> assertThat(released).as("the released extension").isExactlyInstanceOf(BigDecimal.class).isEqualTo(new BigDecimal(value)),
                () -> assertThat(((BigDecimal) released).scale()).as("the scale of the released value").isEqualTo(new BigDecimal(value).scale()),
                () -> assertThat(held.warnings()).containsExactly(warning("number", SubclassedBigDecimal.class, BigDecimal.class)),
                () -> assertThat(storedExtension("number")).as("the stored extension").isEqualTo(new Document("type", "bigDecimal").append("value", value))
        );
    }

    private void assertSubclassedBigIntegerReleasedAsPlain(String value) {
        Held held = heldAndReadWarned(baseEvent().withExtension("number", (Number) new SubclassedBigInteger(value)).build());

        assertAll(
                () -> assertThat(held.event().getExtension("number")).as("the released extension").isExactlyInstanceOf(BigInteger.class).isEqualTo(new BigInteger(value)),
                () -> assertThat(held.warnings()).containsExactly(warning("number", SubclassedBigInteger.class, BigInteger.class)),
                () -> assertThat(storedExtension("number")).as("the stored extension").isEqualTo(new Document("type", "bigInteger").append("value", value))
        );
    }

    @Test
    void an_extension_of_a_custom_Number_class_with_40_significant_digits_is_held_and_released_as_a_BigDecimal_with_its_exact_value() {
        assertReleasedAs(new Reading("1234567890123456789012345678901234567890.5"), new BigDecimal("1234567890123456789012345678901234567890.5"));
    }

    @Test
    void an_integral_extension_of_a_custom_Number_class_past_a_Long_is_held_and_released_as_a_BigDecimal_with_its_exact_value() {
        assertReleasedAs(new Reading("12345678901234567890123456789012345678901234567890"), new BigDecimal("12345678901234567890123456789012345678901234567890"));
    }

    @Test
    void an_extension_of_a_custom_Number_class_whose_toString_is_NaN_is_held_and_released_as_the_Double_NaN() {
        assertReleasedAs(new Reading("NaN"), Double.NaN);
    }

    @Test
    void each_extension_of_a_class_the_record_does_not_hold_as_that_class_is_warned_about_once_and_a_class_it_holds_is_not() {
        Held held = heldAndReadWarned(baseEvent()
                .withExtension("first", (Number) new AtomicLong(1_234_567))
                .withExtension("second", (Number) new Reading("1234567890.123456789"))
                .withExtension("held", (Number) Long.valueOf(7_654_321))
                .build());

        assertAll(
                restored(held.event(), "first", Long.valueOf(1_234_567)),
                restored(held.event(), "second", new BigDecimal("1234567890.123456789")),
                restored(held.event(), "held", Long.valueOf(7_654_321)),
                () -> assertThat(held.warnings()).containsExactlyInAnyOrder(
                        warning("first", AtomicLong.class, Long.class),
                        warning("second", Reading.class, BigDecimal.class))
        );
    }

    @Test
    void an_extension_whose_value_changes_on_every_read_is_held_and_released_as_one_value_without_falling_back_to_the_data_as_bytes() {
        Ticking ticking = new Ticking(1_000);
        Held held = heldAndReadWarned(baseEvent().withExtension("number", (Number) ticking).build());

        Object released = held.event().getExtension("number");
        Document record = requireNonNull(template.getCollection(SKIPPED_INPUTS).find().first());
        assertAll(
                () -> assertThat(released).as("the released extension").isExactlyInstanceOf(Long.class),
                () -> assertThat(ticking.reads()).as("the values the number wrote when it was read").contains((Long) released),
                () -> assertThat(storedExtension("number")).as("the stored extension").isEqualTo(new Document("type", "number").append("class", Ticking.class.getName()).append("value", released)),
                () -> assertThat(held.warnings()).as("the warnings").containsExactly(warning("number", Ticking.class, Long.class)),
                () -> assertThat(record.containsKey("dataClass")).as("the record holds the data as its bytes").isFalse()
        );
    }

    @Test
    void a_byte_array_extension_that_the_producer_changes_after_the_store_reads_it_is_held_and_released_as_the_bytes_first_read_without_falling_back_to_the_data_as_bytes() {
        byte[] live = {1, 2, 3};
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        CloudEvent delivered = baseEvent().withExtension("raw", live).withDataContentType("application/json")
                .withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();

        Held held = heldAndReadWarned(new ChangesTheArrayAfterItIsRead(delivered, "raw", live));

        Document record = requireNonNull(template.getCollection(SKIPPED_INPUTS).find().first());
        assertAll(
                () -> assertThat(held.event().getExtension("raw")).as("the released extension").isEqualTo(new byte[]{1, 2, 3}),
                () -> assertThat(record.containsKey("dataClass")).as("the record holds the data as its bytes").isFalse(),
                () -> assertThat(held.warnings()).as("the warnings").isEmpty()
        );
    }

    @Test
    void no_warning_is_logged_for_an_event_the_store_refuses_because_an_extension_is_a_UUID() {
        CloudEvent event = baseEvent().withExtension("number", (Number) new AtomicLong(1_234_567)).withExtension(new UuidExtension("correlation", UUID.randomUUID())).build();
        StoredSaga instance = anInstance();

        List<String> warnings = warningsLoggedBy(() -> assertThatThrownBy(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)))
                .isInstanceOf(IllegalStateException.class));

        assertAll(
                () -> assertThat(warnings).as("the warnings").isEmpty(),
                () -> assertNothingWritten()
        );
    }

    @Test
    void no_warning_is_logged_when_the_compare_and_set_loses_because_the_expected_version_is_wrong() {
        CloudEvent event = baseEvent().withExtension("number", (Number) new AtomicLong(1_234_567)).build();
        StoredSaga instance = anInstance();
        AtomicReference<Boolean> saved = new AtomicReference<>();

        List<String> warnings = warningsLoggedBy(() -> saved.set(stateStore.compareAndSaveSkipped(instance, 7, QUARANTINE_ID, new SkippedInput(1, KEY, event))));

        assertAll(
                () -> assertThat(saved.get()).as("saved").isFalse(),
                () -> assertThat(warnings).as("the warnings").isEmpty(),
                () -> assertNothingWritten()
        );
    }

    @Test
    void the_warning_for_a_number_is_logged_once_when_the_same_input_is_delivered_again_after_it_was_recorded() {
        CloudEvent event = baseEvent().withExtension("number", (Number) new AtomicLong(1_234_567)).build();

        List<String> warnings = warningsLoggedBy(() -> {
            heldAndRead(event);
            skipAgain(event);
        });

        assertAll(
                () -> assertThat(template.getCollection(SKIPPED_INPUTS).countDocuments()).as("the records").isEqualTo(1),
                () -> assertThat(warnings).as("the warnings").containsExactly(warning("number", AtomicLong.class, Long.class))
        );
    }

    @Test
    void the_warning_for_a_number_is_logged_before_the_callers_transaction_ends_and_again_when_the_input_is_delivered_again_after_that_transaction_rolled_back() {
        CloudEvent event = baseEvent().withExtension("number", (Number) new AtomicLong(1_234_567)).build();
        StoredSaga instance = anInstance();
        SkippedInput input = new SkippedInput(1, KEY, event);
        AtomicReference<List<String>> warningsInTransaction = new AtomicReference<>();
        AtomicReference<Boolean> savedInTransaction = new AtomicReference<>();

        new TransactionTemplate(new MongoTransactionManager(template.getMongoDatabaseFactory())).executeWithoutResult(status -> {
            warningsInTransaction.set(warningsLoggedBy(() -> savedInTransaction.set(stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, input))));
            status.setRollbackOnly();
        });
        long recordsAfterRollback = template.getCollection(SKIPPED_INPUTS).countDocuments();
        AtomicReference<Boolean> savedAgain = new AtomicReference<>();
        // A rollback discards the record, not the log line, so the redelivery logs the warning a second time
        List<String> warningsOfRedelivery = warningsLoggedBy(() -> savedAgain.set(stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, input)));

        assertAll(
                () -> assertThat(savedInTransaction.get()).as("saved in the callers transaction").isTrue(),
                () -> assertThat(warningsInTransaction.get()).as("the warnings logged before the callers transaction ended").containsExactly(warning("number", AtomicLong.class, Long.class)),
                () -> assertThat(recordsAfterRollback).as("the records after the rollback").isZero(),
                () -> assertThat(savedAgain.get()).as("saved again after the rollback").isTrue(),
                () -> assertThat(template.getCollection(SKIPPED_INPUTS).countDocuments()).as("the records after the redelivery").isEqualTo(1),
                () -> assertThat(stateStore.findSkippedInputs(ORDER, QUARANTINE_ID, 10)).as("the recorded inputs").extracting(SkippedInput::key).containsExactly(KEY),
                () -> assertThat(warningsOfRedelivery).as("the warnings of the redelivery").containsExactly(warning("number", AtomicLong.class, Long.class))
        );
    }

    @Test
    void the_warning_for_data_held_as_its_bytes_is_logged_before_the_callers_transaction_ends_and_nothing_the_write_did_is_kept_after_that_transaction_rolled_back() {
        CloudEvent event = eventWithDataHeldAsBytes();
        StoredSaga instance = anInstance();
        AtomicReference<List<String>> warningsInTransaction = new AtomicReference<>();
        AtomicReference<Boolean> savedInTransaction = new AtomicReference<>();

        new TransactionTemplate(new MongoTransactionManager(template.getMongoDatabaseFactory())).executeWithoutResult(status -> {
            warningsInTransaction.set(warningsLoggedBy(() -> savedInTransaction.set(stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)))));
            status.setRollbackOnly();
        });

        assertAll(
                () -> assertThat(savedInTransaction.get()).as("saved in the callers transaction").isTrue(),
                () -> assertThat(warningsInTransaction.get()).as("the warnings logged before the callers transaction ended").singleElement().asString().contains("can hold the data of the input '" + KEY + "' it skips only as its bytes"),
                () -> assertNothingWritten()
        );
    }

    @Test
    void no_warning_for_data_held_as_its_bytes_is_logged_when_the_compare_and_set_loses_because_the_expected_version_is_wrong() {
        CloudEvent event = eventWithDataHeldAsBytes();
        StoredSaga instance = anInstance();
        AtomicReference<Boolean> saved = new AtomicReference<>();

        List<String> warnings = warningsLoggedBy(() -> saved.set(stateStore.compareAndSaveSkipped(instance, 7, QUARANTINE_ID, new SkippedInput(1, KEY, event))));

        assertAll(
                () -> assertThat(saved.get()).as("saved").isFalse(),
                () -> assertThat(warnings).as("the warnings").isEmpty(),
                () -> assertNothingWritten()
        );
    }

    @Test
    void the_warning_for_data_held_as_its_bytes_is_logged_once_when_the_same_input_is_delivered_again_after_it_was_recorded() {
        CloudEvent event = eventWithDataHeldAsBytes();

        List<String> warnings = warningsLoggedBy(() -> {
            heldAndRead(event);
            skipAgain(event);
        });

        assertAll(
                () -> assertThat(template.getCollection(SKIPPED_INPUTS).countDocuments()).as("the records").isEqualTo(1),
                () -> assertThat(warnings).as("the warnings").singleElement().asString().contains("can hold the data of the input '" + KEY + "' it skips only as its bytes")
        );
    }

    @Test
    void a_BigDecimal_and_a_BigInteger_extension_that_are_subclasses_are_warned_about_by_the_subclass_and_the_superclass() {
        Held held = heldAndReadWarned(baseEvent()
                .withExtension("decimal", (Number) new SubclassedBigDecimal("10.00"))
                .withExtension("integer", (Number) new SubclassedBigInteger("123456789012345678901234567890"))
                .build());

        assertAll(
                restored(held.event(), "decimal", new BigDecimal("10.00")),
                () -> assertThat(((BigDecimal) requireNonNull(held.event().getExtension("decimal"))).scale()).as("the scale of the released value").isEqualTo(2),
                restored(held.event(), "integer", new BigInteger("123456789012345678901234567890")),
                () -> assertThat(held.warnings()).containsExactlyInAnyOrder(
                        warning("decimal", SubclassedBigDecimal.class, BigDecimal.class),
                        warning("integer", SubclassedBigInteger.class, BigInteger.class))
        );
    }

    @Test
    void a_Decimal128_extension_is_held_and_released_as_the_same_Decimal128_without_a_warning() {
        Decimal128 value = new Decimal128(new BigDecimal("1234567890.123456789"));
        Held held = heldAndReadWarned(baseEvent().withExtension("decimal128ext", (Number) value).build());

        assertAll(
                restored(held.event(), "decimal128ext", value),
                () -> assertThat(held.warnings()).isEmpty()
        );
    }

    @Test
    void a_quarantined_instance_that_skips_an_event_with_an_AtomicLong_extension_does_not_stop_the_subscription_for_another_instance() {
        assertAnotherInstanceIsFreeWhenAQuarantinedInstanceSkips(event -> CloudEventBuilder.v1(event).withExtension("counter", (Number) new AtomicLong(7)).build());
    }

    @Test
    void a_quarantined_instance_that_skips_an_event_with_an_unpaired_surrogate_extension_does_not_stop_the_subscription_for_another_instance() {
        assertAnotherInstanceIsFreeWhenAQuarantinedInstanceSkips(event -> CloudEventBuilder.v1(event).withExtension("surrogateext", "a\uD800b").build());
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
    void json_data_the_cloudevents_json_format_deserialized_is_held_and_read_back_as_JsonCloudEventData_with_equal_bytes() {
        CloudEvent event = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE).deserialize(("{\"specversion\":\"1.0\",\"id\":\"json-1\",\"source\":\"urn:test\","
                + "\"type\":\"test\",\"subject\":\"" + ORDER + "\",\"datacontenttype\":\"application/json\",\"data\":{\"orderId\":\"" + ORDER + "\",\"value\":42}}").getBytes(UTF_8));
        assertThat(requireNonNull(event.getData())).as("the event the store is handed carries JsonCloudEventData").isInstanceOf(JsonCloudEventData.class);

        CloudEvent held = heldAndRead(event);

        assertAll(
                () -> assertThat(held.getData()).isExactlyInstanceOf(JsonCloudEventData.class),
                () -> assertThat(requireNonNull(held.getData()).toBytes()).isEqualTo(requireNonNull(event.getData()).toBytes()),
                () -> assertThat(((JsonCloudEventData) requireNonNull(held.getData())).getNode()).isEqualTo(((JsonCloudEventData) requireNonNull(event.getData())).getNode())
        );
    }

    @Test
    void json_data_holding_a_binary_value_that_reads_back_as_text_is_refused_and_nothing_is_written() {
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.put("v", new byte[]{1, 2, 3});
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(JsonCloudEventData.wrap(node)).build();
        StoredSaga instance = anInstance();

        Throwable thrown = catchThrowable(() -> stateStore.compareAndSaveSkipped(instance, 1, QUARANTINE_ID, new SkippedInput(1, KEY, event)));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class),
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
    void map_data_holding_two_equal_keys_is_held_as_the_bytes_of_the_data_and_read_back_as_bytes() {
        assertHeldAsBytes(identityMapWithTwoEqualKeys());
    }

    @Test
    void map_data_holding_two_equal_keys_in_a_nested_map_is_held_as_the_bytes_of_the_data_and_read_back_as_bytes() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("inner", identityMapWithTwoEqualKeys());
        assertHeldAsBytes(data);
    }

    @Test
    void map_data_holding_two_equal_keys_in_a_map_inside_a_list_is_held_as_the_bytes_of_the_data_and_read_back_as_bytes() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("parts", List.of(identityMapWithTwoEqualKeys()));
        assertHeldAsBytes(data);
    }

    @Test
    void map_data_that_is_a_comparator_ordered_map_holding_two_equal_keys_is_held_as_the_bytes_of_the_data_and_read_back_as_bytes() {
        Map<Object, Object> data = new TreeMap<>((a, b) -> -1);
        data.put("item", "first");
        data.put("item", "second");
        assertThat(data).hasSize(2);
        assertHeldAsBytes(data);
    }

    private void assertHeldAsBytes(Map<?, ?> data) {
        CloudEvent event = baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();

        CloudEvent held = heldAndRead(event);

        assertAll(
                () -> assertThat(held.getData()).isExactlyInstanceOf(BytesCloudEventData.class),
                () -> assertThat(requireNonNull(held.getData()).toBytes()).isEqualTo("{}".getBytes(UTF_8))
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

    private static final class SubclassedBigDecimal extends BigDecimal {
        private SubclassedBigDecimal(String value) {
            super(value);
        }
    }

    private static final class SubclassedBigInteger extends BigInteger {
        private SubclassedBigInteger(String value) {
            super(value);
        }
    }

    // A Number whose value is the next integer every time it is read, and which remembers every value it wrote
    private static final class Ticking extends Number {
        private final AtomicLong next;
        private final List<Long> reads = new CopyOnWriteArrayList<>();

        private Ticking(long first) {
            this.next = new AtomicLong(first);
        }

        private long read() {
            long value = next.getAndIncrement();
            reads.add(value);
            return value;
        }

        List<Long> reads() {
            return reads;
        }

        @Override
        public int intValue() {
            return (int) read();
        }

        @Override
        public long longValue() {
            return read();
        }

        @Override
        public float floatValue() {
            return read();
        }

        @Override
        public double doubleValue() {
            return read();
        }

        @Override
        public String toString() {
            return Long.toString(read());
        }
    }

    // A Number of a class the record can't hold as that class, whose toString is the decimal it writes
    private static final class Reading extends Number {
        private final String text;

        private Reading(String text) {
            this.text = text;
        }

        @Override
        public int intValue() {
            return (int) doubleValue();
        }

        @Override
        public long longValue() {
            return (long) doubleValue();
        }

        @Override
        public float floatValue() {
            return (float) doubleValue();
        }

        @Override
        public double doubleValue() {
            return Double.parseDouble(text);
        }

        @Override
        public String toString() {
            return text;
        }
    }

    // An event whose array extension is the live array the producer holds, and which changes that array on every access
    // after the first read of the extension, as a producer that keeps writing to it would
    private static final class ChangesTheArrayAfterItIsRead implements CloudEvent {
        private final CloudEvent delegate;
        private final String extension;
        private final byte[] live;
        private boolean read;

        private ChangesTheArrayAfterItIsRead(CloudEvent delegate, String extension, byte[] live) {
            this.delegate = delegate;
            this.extension = extension;
            this.live = live;
        }

        private void touch() {
            if (read) {
                live[0]++;
            }
        }

        @Override
        public io.cloudevents.SpecVersion getSpecVersion() {
            touch();
            return delegate.getSpecVersion();
        }

        @Override
        public String getId() {
            touch();
            return delegate.getId();
        }

        @Override
        public String getType() {
            touch();
            return delegate.getType();
        }

        @Override
        public URI getSource() {
            touch();
            return delegate.getSource();
        }

        @Override
        public String getDataContentType() {
            touch();
            return delegate.getDataContentType();
        }

        @Override
        public URI getDataSchema() {
            touch();
            return delegate.getDataSchema();
        }

        @Override
        public String getSubject() {
            touch();
            return delegate.getSubject();
        }

        @Override
        public OffsetDateTime getTime() {
            touch();
            return delegate.getTime();
        }

        @Override
        public Object getAttribute(String attributeName) {
            touch();
            return delegate.getAttribute(attributeName);
        }

        @Override
        public Object getExtension(String extensionName) {
            touch();
            Object value = extensionName.equals(extension) ? live : delegate.getExtension(extensionName);
            if (extensionName.equals(extension)) {
                read = true;
            }
            return value;
        }

        @Override
        public Set<String> getExtensionNames() {
            touch();
            return delegate.getExtensionNames();
        }

        @Override
        public CloudEventData getData() {
            touch();
            return delegate.getData();
        }
    }

    private CloudEvent eventWithDataHeldAsBytes() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", ORDER);
        data.put("price", new BigDecimal("9876543210.987654321"));
        return baseEvent().withDataContentType("application/json").withData(PojoCloudEventData.wrap(data, value -> "{}".getBytes(UTF_8))).build();
    }

    // Delivers event for the key again to the instance as it is stored now
    private void skipAgain(CloudEvent event) {
        StoredSaga current = stateStore.find(ORDER).orElseThrow();
        stateStore.compareAndSaveSkipped(current, current.version(), QUARANTINE_ID, new SkippedInput(2, KEY, event));
    }

    private static List<String> warningsLoggedBy(Runnable action) {
        Logger storeLog = (Logger) LoggerFactory.getLogger(SpringMongoSagaStateStore.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        storeLog.addAppender(appender);
        try {
            action.run();
            return appender.list.stream().filter(logged -> logged.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
        } finally {
            storeLog.detachAppender(appender);
            appender.stop();
        }
    }

    private record Held(CloudEvent event, List<String> warnings) {
    }

    private Held heldAndReadWarned(CloudEvent event) {
        Logger storeLog = (Logger) LoggerFactory.getLogger(SpringMongoSagaStateStore.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        storeLog.addAppender(appender);
        try {
            CloudEvent released = heldAndRead(event);
            return new Held(released, appender.list.stream().filter(logged -> logged.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList());
        } finally {
            storeLog.detachAppender(appender);
            appender.stop();
        }
    }

    private static String warning(String extension, Class<?> delivered, Class<?> released) {
        return "Saga instance '" + ORDER + "' can't hold the extension '" + extension + "' of the input '" + KEY + "' it skips as a " + delivered.getName()
               + ", so a release hands it back as a " + released.getName() + ".";
    }

    private Object assertReleasedAs(Number delivered, Object expected) {
        Held held = heldAndReadWarned(baseEvent().withExtension("number", delivered).build());

        assertAll(
                restored(held.event(), "number", expected),
                () -> assertThat(held.warnings()).containsExactly(warning("number", delivered.getClass(), expected.getClass())),
                () -> assertThat(String.join("\n", held.warnings())).as("the warning does not hold the value").doesNotContain(delivered.toString()),
                () -> assertThat(storedExtension("number")).as("the stored extension").containsEntry("type", "number").containsEntry("class", delivered.getClass().getName()),
                () -> assertThat(storedExtension("number").get("value")).as("the stored value").isEqualTo(expected instanceof BigDecimal decimal ? decimal.toString() : expected)
        );
        return requireNonNull(held.event().getExtension("number"));
    }

    private Document storedExtension(String name) {
        Document record = requireNonNull(template.getCollection(SKIPPED_INPUTS).find().first());
        return requireNonNull(record.get("extensions", Document.class)).get(name, Document.class);
    }

    private <T> void assertAttributeHeldAndReleased(CloudEventBuilder builder, Function<CloudEvent, T> attribute, T expected) {
        CloudEvent event = builder.build();
        assertThat(attribute.apply(event)).as("the event the store is handed carries the attribute").isEqualTo(expected);

        CloudEvent held = heldAndRead(event);

        assertAll(
                () -> assertThat(attribute.apply(held)).isEqualTo(expected),
                () -> assertThat(String.valueOf(attribute.apply(held)).toCharArray()).as("the lone surrogate is still there").isEqualTo(String.valueOf(expected).toCharArray()),
                () -> assertThat(held).isEqualTo(event)
        );
    }

    private void assertAnotherInstanceIsFreeWhenAQuarantinedInstanceSkips(UnaryOperator<CloudEvent> withExtension) {
        String other = "order-2";
        List<IllegalStateException> refused = new CopyOnWriteArrayList<>();
        @SuppressWarnings("unchecked")
        SpringMongoSagaStateStore<String> observed = Mockito.mock(SpringMongoSagaStateStore.class, Mockito.withSettings().spiedInstance(stateStore).defaultAnswer(invocation -> {
            try {
                return invocation.callRealMethod();
            } catch (Throwable e) {
                if ("compareAndSaveSkipped".equals(invocation.getMethod().getName()) && e instanceof IllegalStateException refusal) {
                    refused.add(refusal);
                }
                throw e;
            }
        }));
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), observed, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        eventStore.write(STREAM, converter.toCloudEvents(List.of(new OrderPlaced(ORDER), new OrderPlaced(other))));
        failing.add("priced");
        eventStore.write(STREAM, converter.toCloudEvents(List.of(new ItemAdded(ORDER, "priced", 5))));
        await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));

        eventStore.write(STREAM, List.of(withExtension.apply(converter.toCloudEvent(new ItemAdded(ORDER, "pen", 5)))));
        Throwable heldTimeout = catchThrowable(() -> await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(template.getCollection(SKIPPED_INPUTS).countDocuments()).isEqualTo(2)));
        eventStore.write(STREAM, converter.toCloudEvents(List.of(new ItemAdded(other, "ink", 5))));
        Throwable dispatchedTimeout = catchThrowable(() -> await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(dispatched).containsExactly(new Pack("ink"))));

        assertAll(
                () -> assertThat(refused).as("no record is refused, so the subscription is not stopped").isEmpty(),
                () -> assertThat(heldTimeout).as("the event it failed on and the one after it are held").isNull(),
                () -> assertThat(dispatchedTimeout).as("the other instance processes its next event").isNull(),
                () -> assertThat(dispatched).as("only the other instance dispatches").containsExactly(new Pack("ink"))
        );
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
