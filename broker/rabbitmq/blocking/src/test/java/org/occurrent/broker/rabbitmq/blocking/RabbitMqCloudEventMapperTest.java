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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
                .build();

        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());

        assertThat(properties.getContentType()).isEqualTo("application/json");
        assertThat(properties.getHeaders())
                .containsEntry("cloudEvents_id", "id-1")
                .containsEntry("cloudEvents_source", "urn:test")
                .containsEntry("cloudEvents_type", "com.acme.OrderPlaced")
                .containsEntry("cloudEvents_subject", "subject-1")
                .containsEntry("cloudEvents_dataschema", "urn:schema")
                .containsEntry("cloudEvents_specversion", "1.0")
                .containsEntry("cloudEvents_streamid", "stream-1")
                .containsEntry("cloudEvents_streamversion", "3")
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

    private static CloudEvent minimalCloudEvent() {
        return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("t").build();
    }
}
