package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.types.Time;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertAll;

class DocumentCloudEventWriterTest {

    @SuppressWarnings("unchecked") @Test
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
                .withData("application/json", new DocumentCloudEventData(data))
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
}