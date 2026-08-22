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

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitMqCloudEventMapperTest {

    @Test
    void toBasicProperties_writes_every_context_attribute_and_extension_as_a_prefixed_header() {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("com.acme.OrderPlaced")
                .withSubject("subject-1")
                .withTime(OffsetDateTime.parse("2026-08-18T10:00:00Z"))
                .withDataSchema(URI.create("urn:schema"))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .withExtension("streamid", "stream-1")
                .withExtension("streamversion", 3L)
                .withExtension("occurredat", OffsetDateTime.parse("2026-08-18T09:00:00Z"))
                .withExtension("causationsource", URI.create("urn:causation"))
                .withExtension("retrycount", 2)
                .build();

        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());

        assertThat(properties.getContentType()).isEqualTo("application/json");
        assertThat(properties.getHeaders())
                .containsEntry("cloudEvents_id", "id-1")
                .containsEntry("cloudEvents_source", "urn:test")
                .containsEntry("cloudEvents_type", "com.acme.OrderPlaced")
                .containsEntry("cloudEvents_subject", "subject-1")
                .containsEntry("cloudEvents_time", "2026-08-18T10:00Z")
                .containsEntry("cloudEvents_dataschema", "urn:schema")
                .containsEntry("cloudEvents_specversion", "1.0")
                .containsEntry("cloudEvents_streamid", "stream-1")
                .containsEntry("cloudEvents_streamversion", "3")
                // OffsetDateTime, URI and Integer (a Number) extensions, alongside the String and Long ones above,
                // so this test proves toString() carries every extension value type withExtension(...) accepts, not
                // only the two the streamid/streamversion pair happens to cover.
                .containsEntry("cloudEvents_occurredat", "2026-08-18T09:00Z")
                .containsEntry("cloudEvents_causationsource", "urn:causation")
                .containsEntry("cloudEvents_retrycount", "2")
                .doesNotContainKey("cloudEvents_datacontenttype");
    }

    @Test
    void toBasicProperties_base64_encodes_a_binary_extension_instead_of_writing_its_raw_toString() {
        byte[] binaryExtensionValue = {1, 2, 3, 4, 5};
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("t")
                .withExtension("dcbtags", binaryExtensionValue)
                .build();

        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());

        assertThat(properties.getHeaders()).containsEntry("cloudEvents_dcbtags", Base64.getEncoder().encodeToString(binaryExtensionValue));
    }

    @Test
    void toBasicProperties_publishes_persistent_so_a_broker_restart_cannot_discard_a_confirmed_message() {
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(minimalCloudEvent(), Map.of());

        assertThat(properties.getDeliveryMode()).isEqualTo(2);
    }

    @Test
    void toBasicProperties_includes_the_application_headers_alongside_the_cloudEvent_attribute_headers() {
        CloudEvent cloudEvent = minimalCloudEvent();

        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of("tenant", "acme"));

        assertThat(properties.getHeaders()).containsEntry("tenant", "acme").containsKey("cloudEvents_id");
    }

    @Test
    void toBasicProperties_omits_the_contentType_field_when_the_event_has_no_dataContentType() {
        CloudEvent cloudEvent = minimalCloudEvent();

        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());

        assertThat(properties.getContentType()).isNull();
    }

    @Test
    void toBody_returns_the_event_data_unchanged() {
        byte[] data = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        CloudEvent cloudEvent = CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("t").withData(data).build();

        assertThat(RabbitMqCloudEventMapper.toBody(cloudEvent)).isEqualTo(data);
    }

    @Test
    void toBody_returns_an_empty_array_when_the_event_has_no_data() {
        assertThat(RabbitMqCloudEventMapper.toBody(minimalCloudEvent())).isEmpty();
    }

    /**
     * Occurrent's own converters hand back a lazily-serializing {@code PojoCloudEventData}, so calling
     * {@link io.cloudevents.CloudEventData#toBytes()} a second time on the same publish serializes the domain
     * event a second time. {@link RabbitMqCloudEventMapper#toBasicProperties(CloudEvent, Map, byte[])} exists so a
     * caller that already computed the body through {@link RabbitMqCloudEventMapper#toBody(CloudEvent)} can pass
     * those bytes in and have this method reuse them for its empty-data marker check, rather than call
     * {@code toBytes()} again itself.
     */
    @Test
    void toBasicProperties_with_a_precomputed_body_does_not_serialize_the_data_a_second_time() {
        AtomicInteger toBytesCalls = new AtomicInteger();
        byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
        CloudEventData countingData = new CloudEventData() {
            @Override
            public byte[] toBytes() {
                toBytesCalls.incrementAndGet();
                return data;
            }
        };
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("t")
                .withData("application/json", countingData)
                .build();

        byte[] body = RabbitMqCloudEventMapper.toBody(cloudEvent);
        assertThat(toBytesCalls).as("toBody(..) itself calls toBytes() exactly once").hasValue(1);

        RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of(), body);

        assertThat(toBytesCalls).as("toBasicProperties(..., byte[]) must reuse the given body instead of calling "
                        + "CloudEventData.toBytes() again")
                .hasValue(1);
    }

    // This round trip stays in the JVM, so properties.getHeaders() already holds plain java.lang.String values.
    // It does not exercise the LongString branch toCloudEvent's own comment calls out, since that only shows up in
    // a header read back off a real broker delivery. RabbitMqCloudEventBridgeTest and RabbitMqDomainEventBridgeTest
    // cover that conversion end to end, against an actual RabbitMQ container.
    @Test
    void toCloudEvent_rebuilds_every_context_attribute_and_extension_from_a_previously_written_message() {
        CloudEvent original = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("com.acme.OrderPlaced")
                .withSubject("subject-1")
                .withTime(OffsetDateTime.parse("2026-08-18T10:00:00Z"))
                .withDataSchema(URI.create("urn:schema"))
                .withDataContentType("application/json")
                .withData("{\"a\":1}".getBytes(StandardCharsets.UTF_8))
                .withExtension("streamid", "stream-1")
                .withExtension("streamversion", 3L)
                .withExtension("customcount", 7L)
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(original, Map.of());
        byte[] body = RabbitMqCloudEventMapper.toBody(original);

        CloudEvent rebuilt = RabbitMqCloudEventMapper.toCloudEvent(properties, body);

        assertThat(rebuilt.getId()).isEqualTo("id-1");
        assertThat(rebuilt.getSource()).isEqualTo(URI.create("urn:test"));
        assertThat(rebuilt.getType()).isEqualTo("com.acme.OrderPlaced");
        assertThat(rebuilt.getSubject()).isEqualTo("subject-1");
        assertThat(rebuilt.getTime()).isEqualTo(OffsetDateTime.parse("2026-08-18T10:00:00Z"));
        assertThat(rebuilt.getDataSchema()).isEqualTo(URI.create("urn:schema"));
        assertThat(rebuilt.getDataContentType()).isEqualTo("application/json");
        assertThat(rebuilt.getData().toBytes()).isEqualTo("{\"a\":1}".getBytes(StandardCharsets.UTF_8));
        assertThat(rebuilt.getExtension("streamid")).isEqualTo("stream-1");
        // Occurrent owns streamversion and defines it as a Long, so this comes back that type, not a String, per
        // ADR 133's amendment.
        assertThat(rebuilt.getExtension("streamversion")).isEqualTo(3L);
        // An extension Occurrent does not define, unlike streamversion above, still comes back a String, since
        // this mapper has no way to know it was ever a Number.
        assertThat(rebuilt.getExtension("customcount")).isEqualTo("7");
    }

    @Test
    void toCloudEvent_does_not_set_a_data_content_type_when_the_message_carried_none() {
        CloudEvent original = minimalCloudEvent();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(original, Map.of());

        CloudEvent rebuilt = RabbitMqCloudEventMapper.toCloudEvent(properties, RabbitMqCloudEventMapper.toBody(original));

        assertThat(rebuilt.getDataContentType()).isNull();
    }

    @Test
    void toCloudEvent_leaves_data_null_for_an_empty_body() {
        CloudEvent original = minimalCloudEvent();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(original, Map.of());

        CloudEvent rebuilt = RabbitMqCloudEventMapper.toCloudEvent(properties, RabbitMqCloudEventMapper.toBody(original));

        assertThat(rebuilt.getData()).isNull();
    }

    /**
     * Data present but explicitly empty ({@code withData(new byte[0])}) is not the same thing as no data at all,
     * and a handler or a payload filter can tell the two apart. Both encode as the same zero-length AMQP body, so
     * without the {@code cloudEvents_data_present_empty} marker header this rebuilt as {@code null}, the same as
     * {@link #toCloudEvent_leaves_data_null_for_an_empty_body()} above, silently conflating the two on every round
     * trip.
     */
    @Test
    void toCloudEvent_rebuilds_data_present_but_empty_distinctly_from_no_data_at_all() {
        CloudEvent original = CloudEventBuilder.v1()
                .withId("id")
                .withSource(URI.create("urn:test"))
                .withType("t")
                .withData(new byte[0])
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(original, Map.of());

        CloudEvent rebuilt = RabbitMqCloudEventMapper.toCloudEvent(properties, RabbitMqCloudEventMapper.toBody(original));

        assertThat(rebuilt.getData()).isNotNull();
        assertThat(rebuilt.getData().toBytes()).isEmpty();
    }

    @Test
    void toCloudEvent_rebuilds_under_the_spec_version_the_message_was_written_with_rather_than_always_v1() {
        CloudEvent original = CloudEventBuilder.v03()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("t")
                .withSchemaUrl(URI.create("urn:schema-v03"))
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(original, Map.of());

        CloudEvent rebuilt = RabbitMqCloudEventMapper.toCloudEvent(properties, RabbitMqCloudEventMapper.toBody(original));

        assertThat(rebuilt.getSpecVersion()).isEqualTo(SpecVersion.V03);
        assertThat(rebuilt.getDataSchema()).isEqualTo(URI.create("urn:schema-v03"));
    }

    @Test
    void toCloudEvent_ignores_a_header_outside_the_cloudEvents_prefix_namespace() {
        CloudEvent original = minimalCloudEvent();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(original, Map.of("tenant", "acme"));

        CloudEvent rebuilt = RabbitMqCloudEventMapper.toCloudEvent(properties, RabbitMqCloudEventMapper.toBody(original));

        assertThat(rebuilt.getExtensionNames()).doesNotContain("tenant");
        assertThat(rebuilt.getAttributeNames()).doesNotContain("tenant");
    }

    /**
     * {@code streamversion} and {@code position} are restored as a {@code Long}, which means parsing one that is
     * not actually numeric throws rather than silently keeping it a String. A message this mapper never wrote (a
     * corrupted header, or one from a producer that does not follow this mapping) can carry either. The bridges
     * already route any {@link RuntimeException} out of {@code toCloudEvent} to their undecodable-message path
     * rather than acking or crashing on it, so this only has to prove the throw itself, not the bridge behaviour.
     */
    @Test
    void toCloudEvent_throws_on_a_non_numeric_streamversion_or_position_header_rather_than_silently_keeping_it_a_string() {
        BasicProperties streamversionProperties = new BasicProperties.Builder()
                .headers(Map.of(
                        "cloudEvents_id", "id-1",
                        "cloudEvents_source", "urn:test",
                        "cloudEvents_type", "t",
                        "cloudEvents_streamversion", "not-a-number"))
                .build();
        assertThatThrownBy(() -> RabbitMqCloudEventMapper.toCloudEvent(streamversionProperties, new byte[0]))
                .isInstanceOf(NumberFormatException.class);

        BasicProperties positionProperties = new BasicProperties.Builder()
                .headers(Map.of(
                        "cloudEvents_id", "id-1",
                        "cloudEvents_source", "urn:test",
                        "cloudEvents_type", "t",
                        "cloudEvents_position", "not-a-number"))
                .build();
        assertThatThrownBy(() -> RabbitMqCloudEventMapper.toCloudEvent(positionProperties, new byte[0]))
                .isInstanceOf(NumberFormatException.class);
    }

    private static CloudEvent minimalCloudEvent() {
        return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("t").build();
    }
}
