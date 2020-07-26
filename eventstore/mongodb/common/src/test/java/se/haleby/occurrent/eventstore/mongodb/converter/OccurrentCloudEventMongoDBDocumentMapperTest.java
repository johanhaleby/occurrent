package se.haleby.occurrent.eventstore.mongodb.converter;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertAll;

@SuppressWarnings("ConstantConditions")
class OccurrentCloudEventMongoDBDocumentMapperTest {

    private EventFormat eventFormat;

    @BeforeEach
    void initialization() {
        eventFormat = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    @SuppressWarnings("unchecked")
    @Test
    void converts_cloud_event_to_document_with_expected_values() {
        // Given
        ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC);

        CloudEvent cloudEvent = new CloudEventBuilder()
                .withSubject("subject")
                .withType("type")
                .withTime(zonedDateTime)
                .withSource(URI.create("urn:name"))
                .withId("id")
                .withData("application/json", "{\"name\" : \"hello\"}".getBytes(UTF_8))
                .build();

        // When
        Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(eventFormat, "streamId", cloudEvent);

        // Then
        assertAll(
                () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                () -> assertThat(document.getString("type")).isEqualTo("type"),
                () -> assertThat(document.getString("time")).isEqualTo("2020-07-26T09:13:03Z"),
                () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                () -> assertThat(document.getString("id")).isEqualTo("id"),
                () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                () -> assertThat(document.getString("streamId")).isEqualTo("streamId")
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    void converts_document_to_cloud_event_and_remove_mongo_id() {
        // Given
        Document document = new Document(new HashMap<String, Object>() {{
            put("subject", "subject");
            put("type", "type");
            put("time", "2020-07-26T09:13:03Z");
            put("source", "urn:name");
            put("id", "id");
            put("_id", "mongodb");
            put("data", new HashMap<String, Object>() {{
                put("name", "hello");
            }});
            put("datacontenttype", "application/json");
            put("specversion", "1.0");
            put("streamId", "streamId");
        }});

        // When
        CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, document);

        // Then
        CloudEvent expected = new CloudEventBuilder()
                .withSubject("subject")
                .withType("type")
                .withTime(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                .withSource(URI.create("urn:name"))
                .withId("id")
                .withData("application/json", "{\"name\":\"hello\"}".getBytes(UTF_8))
                .withExtension(new OccurrentCloudEventExtension("streamId"))
                .build();

        assertThat(actual).isEqualTo(expected);
    }
}