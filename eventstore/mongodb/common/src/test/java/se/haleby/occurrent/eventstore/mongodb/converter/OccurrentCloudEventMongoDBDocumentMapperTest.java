package se.haleby.occurrent.eventstore.mongodb.converter;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertAll;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.RFC_3339_DATE_TIME_FORMATTER;

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
        ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

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
                () -> assertThat(document.getDate("time")).isEqualTo(toRfc3339Date(zonedDateTime)),
                () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                () -> assertThat(document.getString("id")).isEqualTo("id"),
                () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                () -> assertThat(document.getString("streamId")).isEqualTo("streamId")
        );
    }

    @Test
    void converts_document_to_cloud_event_and_remove_mongo_id() {
        // Given
        Document document = new Document(new HashMap<String, Object>() {{
            put("subject", "subject");
            put("type", "type");
            put("time", toRfc3339Date("2020-07-26T09:13:03Z"));
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

    @Test
    void converts_document_to_cloud_event_when_time_is_specified_with_millis() {
        // Given
        Document document = new Document(new HashMap<String, Object>() {{
            put("subject", "subject");
            put("type", "type");
            put("time", toRfc3339Date("2020-07-26T09:13:03.234Z"));
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
        assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 234_000000), UTC));
    }

    @Test
    void converts_document_to_cloud_event_when_time_is_specified_with_timezone_that_is_plus_utc() {
        // Given
        Document document = new Document(new HashMap<String, Object>() {{
            put("subject", "subject");
            put("type", "type");
            put("time", toRfc3339Date("2020-07-26T09:13:03+02:00"));
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
        assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneId.of("CET")));
    }

    @Test
    void converts_document_to_cloud_event_when_time_is_specified_with_timezone_that_is_minus_utc() {
        // Given
        Document document = new Document(new HashMap<String, Object>() {{
            put("subject", "subject");
            put("type", "type");
            put("time", toRfc3339Date("2020-07-26T09:13:03-02:00"));
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
        assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneOffset.of("-02:00")));
    }


    @ParameterizedTest
    @MethodSource("rfc3339Data")
    void rfc3339_date_time_conversion_test(String dateTimeString, ZonedDateTime expected) {
        // When
        ZonedDateTime actual = ZonedDateTime.from(RFC_3339_DATE_TIME_FORMATTER.parse(dateTimeString));

        // Then
        assertThat(expected).isEqualTo(actual);
    }

    private static Stream<Arguments> rfc3339Data() {
        return Stream.of(
                Arguments.of("2007-05-01T15:43:26+07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26.3+07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452+07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), ZoneOffset.of("+07:00"))),
                Arguments.of("2007-05-01T15:43:26-07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3-07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452-07:00", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), ZoneOffset.of("-07:00"))),
                Arguments.of("2007-05-01T15:43:26.3452Z", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 345_200_000), UTC)),
                Arguments.of("2007-05-01T15:43:26.3Z", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26, 300_000_000), UTC)),
                Arguments.of("2007-05-01T15:43:26Z", ZonedDateTime.of(LocalDateTime.of(2007, 5, 1, 15, 43, 26), UTC))
        );
    }

    private static Date toRfc3339Date(ZonedDateTime zonedDateTime) {
        return Date.from(zonedDateTime.toInstant());
    }

    private static Date toRfc3339Date(String rfc3339FormattedString) {
        return toRfc3339Date(ZonedDateTime.from(RFC_3339_DATE_TIME_FORMATTER.parse(rfc3339FormattedString)));
    }
}