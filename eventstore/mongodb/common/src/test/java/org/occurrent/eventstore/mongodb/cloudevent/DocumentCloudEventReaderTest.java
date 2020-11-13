package org.occurrent.eventstore.mongodb.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.types.Time;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

class DocumentCloudEventReaderTest {

    @Test
    void toEventWithBytesData() {
        // Given
        OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

        Document document = new Document()
                .append("specversion", SpecVersion.V1.toString())
                .append("subject", "subject")
                .append("type", "type")
                .append("time", Time.writeTime(offsetDateTime))
                .append("source", "urn:name")
                .append("id", "id")
                .append("datacontenttype", "application/json")
                .append("data", "{\"name\" : \"hello\"}".getBytes(UTF_8));

        // When
        CloudEvent have = DocumentCloudEventReader.toCloudEvent(document);

        // Then
        CloudEvent want = new CloudEventBuilder()
                .withSubject("subject")
                .withType("type")
                .withTime(offsetDateTime)
                .withSource(URI.create("urn:name"))
                .withId("id")
                .withDataContentType("application/json")
                .withData(new DocumentCloudEventData("{\"name\" : \"hello\"}"))
                .build();

        assertThat(have).isEqualTo(want);
    }

    @Test
    void toEventWithDocumentData() {
        // Given
        OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

        Document data = new Document().append("name", "hello");

        Document document = new Document()
                .append("specversion", SpecVersion.V1.toString())
                .append("subject", "subject")
                .append("type", "type")
                .append("time", Time.writeTime(offsetDateTime))
                .append("source", "urn:name")
                .append("id", "id")
                .append("datacontenttype", "application/json")
                .append("data", data);

        // When
        CloudEvent have = DocumentCloudEventReader.toCloudEvent(document);

        // Then
        CloudEvent want = new CloudEventBuilder()
                .withSubject("subject")
                .withType("type")
                .withTime(offsetDateTime)
                .withSource(URI.create("urn:name"))
                .withId("id")
                .withData("application/json", new DocumentCloudEventData(data))
                .build();

        assertThat(have).isEqualTo(want);
    }
}
