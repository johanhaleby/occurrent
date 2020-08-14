package se.haleby.occurrent.eventstore.mongodb.internal;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static se.haleby.occurrent.eventstore.mongodb.TimeRepresentation.DATE;
import static se.haleby.occurrent.eventstore.mongodb.TimeRepresentation.RFC_3339_STRING;
import static se.haleby.occurrent.eventstore.mongodb.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

class OccurrentCloudEventMongoDBDocumentMapperTest {

    private EventFormat eventFormat;

    @BeforeEach
    void initialization() {
        eventFormat = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    @Nested
    @DisplayName("time representation is rfc 3339 string")
    class TimeRepresentationRfc3339String {

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
            Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(eventFormat, RFC_3339_STRING, "streamId", 2L, cloudEvent);

            // Then
            assertAll(
                    () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                    () -> assertThat(document.getString("type")).isEqualTo("type"),
                    () -> assertThat(document.getString("time")).isEqualTo(RFC_3339_DATE_TIME_FORMATTER.format(zonedDateTime)),
                    () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                    () -> assertThat(document.getString("id")).isEqualTo("id"),
                    () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                    () -> assertThat(document.getString("streamId")).isEqualTo("streamId"),
                    () -> assertThat(document.getLong("streamVersion")).isEqualTo(2L)
            );
        }

        @SuppressWarnings("unchecked")
        @Test
        void converts_cloud_event_with_nanoseconds_in_non_utc_timezone_to_rfc_3339() {
            // Given
            ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 123_456_789), ZoneId.of("Europe/Stockholm"));

            CloudEvent cloudEvent = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(zonedDateTime)
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withData("application/json", "{\"name\" : \"hello\"}".getBytes(UTF_8))
                    .build();

            // When
            Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(eventFormat, RFC_3339_STRING, "streamId", 2L, cloudEvent);

            // Then
            assertAll(
                    () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                    () -> assertThat(document.getString("type")).isEqualTo("type"),
                    () -> assertThat(document.getString("time")).isEqualTo(RFC_3339_DATE_TIME_FORMATTER.format(zonedDateTime)),
                    () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                    () -> assertThat(document.getString("id")).isEqualTo("id"),
                    () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                    () -> assertThat(document.getString("streamId")).isEqualTo("streamId"),
                    () -> assertThat(document.getLong("streamVersion")).isEqualTo(2L)
            );
        }


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
                put("streamVersion", 2L);
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, RFC_3339_STRING, document);

            // Then
            CloudEvent expected = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withData("application/json", "{\"name\":\"hello\"}".getBytes(UTF_8))
                    .withExtension(new OccurrentCloudEventExtension("streamId", 2L))
                    .build();

            assertThat(actual).isEqualTo(expected);
        }

        @Test
        void converts_document_to_cloud_event_when_time_is_specified_with_millis() {
            // Given
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", "2020-07-26T09:13:03.234Z");
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
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, RFC_3339_STRING, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 234_000000), UTC));
        }

        @Test
        void converts_document_to_cloud_event_when_time_is_specified_with_timezone_that_is_plus_utc() {
            // Given
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", "2020-07-26T09:13:03+02:00");
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
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, RFC_3339_STRING, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneId.of("CET")));
        }

        @Test
        void converts_document_to_cloud_event_when_time_is_specified_with_timezone_that_is_minus_utc() {
            // Given
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", "2020-07-26T09:13:03-02:00");
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
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, RFC_3339_STRING, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneOffset.of("-02:00")));
        }
    }

    @Nested
    @DisplayName("time representation is date")
    class TimeRepresentationDate {

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
            Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(eventFormat, DATE, "streamId", 2L, cloudEvent);

            // Then
            assertAll(
                    () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                    () -> assertThat(document.getString("type")).isEqualTo("type"),
                    () -> assertThat(document.getDate("time")).isEqualTo(toDate(zonedDateTime)),
                    () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                    () -> assertThat(document.getString("id")).isEqualTo("id"),
                    () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                    () -> assertThat(document.getString("streamId")).isEqualTo("streamId"),
                    () -> assertThat(document.getLong("streamVersion")).isEqualTo(2L)
            );
        }

        @Test
        void throws_iae_when_time_is_using_with_nanoseconds_and_timezone_is_utc() {
            // Given
            ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 123_456_789), UTC);

            CloudEvent cloudEvent = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(zonedDateTime)
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withData("application/json", "{\"name\" : \"hello\"}".getBytes(UTF_8))
                    .build();

            // When
            Throwable throwable = catchThrowable(() -> OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(eventFormat, DATE, "streamId", 2L, cloudEvent));

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("The ZonedDateTime in the CloudEvent time field contains micro-/nanoseconds. This is is not possible to represent when using TimeRepresentation DATE, either change to TimeRepresentation RFC_3339_STRING or remove micro-/nanoseconds using \"zonedDateTime.truncatedTo(ChronoUnit.MILLIS)\".");
        }

        @Test
        void throws_iae_when_time_another_timezone_than_utc() {
            // Given
            ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 123_000_000), ZoneId.of("Europe/Stockholm")).truncatedTo(MILLIS);

            CloudEvent cloudEvent = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(zonedDateTime)
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withData("application/json", "{\"name\" : \"hello\"}".getBytes(UTF_8))
                    .build();

            // When
            Throwable throwable = catchThrowable(() -> OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(eventFormat, DATE, "streamId", 2L, cloudEvent));

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("The ZonedDateTime in the CloudEvent time field is not defined using UTC. TimeRepresentation DATE require UTC as timezone to not loose precision. Either change to TimeRepresentation RFC_3339_STRING or convert the ZonedDateTime to UTC using e.g. \"zonedDateTime.withZoneSameInstant(ZoneOffset.UTC)\".");
        }

        @Test
        void converts_document_to_cloud_event_and_remove_mongo_id() {
            // Given
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", toDate("2020-07-26T09:13:03Z"));
                put("source", "urn:name");
                put("id", "id");
                put("_id", "mongodb");
                put("data", new HashMap<String, Object>() {{
                    put("name", "hello");
                }});
                put("datacontenttype", "application/json");
                put("specversion", "1.0");
                put("streamId", "streamId");
                put("streamVersion", 2L);
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, DATE, document);

            // Then
            CloudEvent expected = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withData("application/json", "{\"name\":\"hello\"}".getBytes(UTF_8))
                    .withExtension(new OccurrentCloudEventExtension("streamId", 2L))
                    .build();

            assertThat(actual).isEqualTo(expected);
        }

        @Test
        void converts_document_to_cloud_event_when_time_is_specified_with_millis() {
            // Given
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", toDate("2020-07-26T09:13:03.234Z"));
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
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, DATE, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 234_000000), UTC));
        }

        @Test
        void converts_document_to_cloud_event_when_time_is_specified_with_timezone_that_is_plus_utc() {
            // Given
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", toDate("2020-07-26T09:13:03+02:00"));
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
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, DATE, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneId.of("CET")));
        }

        @Test
        void converts_document_to_cloud_event_when_time_is_specified_with_timezone_that_is_minus_utc() {
            // Given
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", toDate("2020-07-26T09:13:03-02:00"));
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
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, DATE, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(ZonedDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneOffset.of("-02:00")));
        }

        private Date toDate(ZonedDateTime zonedDateTime) {
            return Date.from(zonedDateTime.toInstant());
        }

        private Date toDate(String rfc3339FormattedString) {
            return toDate(ZonedDateTime.from(RFC_3339_DATE_TIME_FORMATTER.parse(rfc3339FormattedString)));
        }
    }
}