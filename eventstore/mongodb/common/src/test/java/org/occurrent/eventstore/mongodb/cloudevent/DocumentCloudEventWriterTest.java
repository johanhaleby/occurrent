/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.types.Time;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertAll;

class DocumentCloudEventWriterTest {

    @SuppressWarnings("unchecked")
    @Test
    void toDocumentWithBytesData() {
        // Given
        OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

        CloudEvent cloudEvent = new CloudEventBuilder()
                .withSubject("subject")
                .withType("type")
                .withTime(offsetDateTime)
                .withSource(URI.create("urn:name"))
                .withId("id")
                .withData("application/json", "{\"name\" : \"hello\"}".getBytes(UTF_8))
                .build();

        // When
        Document document = DocumentCloudEventWriter.toDocument(cloudEvent);

        // Then
        assertAll(
                () -> assertThat(document).isNotNull(),
                () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                () -> assertThat(document.getString("type")).isEqualTo("type"),
                () -> assertThat(document.getString("time")).isEqualTo(Time.writeTime(offsetDateTime)),
                () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                () -> assertThat(document.getString("id")).isEqualTo("id"),
                () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello"))
        );
    }

    @Test
    void toDocumentWithDocumentData() {
        // Given
        OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

        Document data = new Document().append("name", "hello");

        CloudEvent cloudEvent = new CloudEventBuilder()
                .withSubject("subject")
                .withType("type")
                .withTime(offsetDateTime)
                .withSource(URI.create("urn:name"))
                .withId("id")
                .withData("application/json", PojoCloudEventData.wrap(data, document1 -> document1.toJson().getBytes(UTF_8)))
                .build();

        // When
        Document document = DocumentCloudEventWriter.toDocument(cloudEvent);

        // Then
        assertAll(
                () -> assertThat(document).isNotNull(),
                () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                () -> assertThat(document.getString("type")).isEqualTo("type"),
                () -> assertThat(document.getString("time")).isEqualTo(Time.writeTime(offsetDateTime)),
                () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                () -> assertThat(document.getString("id")).isEqualTo("id"),
                () -> assertThat(document.get("data", Document.class)).isEqualTo(data)
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    void toDocumentWithPojoCloudEventData() {
        // Given
        OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

        Map<String, Object> data = new HashMap<>();
        data.put("name", "hello");

        CloudEvent cloudEvent = new CloudEventBuilder()
                .withSubject("subject")
                .withType("type")
                .withTime(offsetDateTime)
                .withSource(URI.create("urn:name"))
                .withId("id")
                .withData("application/json", PojoCloudEventData.wrap(data, __ -> "!!!shouldn't be serialized!!!".getBytes(UTF_8)))
                .build();

        // When
        Document document = DocumentCloudEventWriter.toDocument(cloudEvent);

        // Then
        assertAll(
                () -> assertThat(document).isNotNull(),
                () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                () -> assertThat(document.getString("type")).isEqualTo("type"),
                () -> assertThat(document.getString("time")).isEqualTo(Time.writeTime(offsetDateTime)),
                () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                () -> assertThat(document.getString("id")).isEqualTo("id"),
                () -> assertThat(document.get("data", Map.class)).isEqualTo(new Document(data))
        );
    }
}