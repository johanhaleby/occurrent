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

package org.occurrent.eventstore.mongodb.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.jackson.JsonCloudEventData;
import io.cloudevents.types.Time;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.net.URI;
import java.time.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.DATE;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

class OccurrentCloudEventMongoDBDocumentMapperTest {

    @Nested
    @DisplayName("converts cloud event to document")
    class ConvertsCloudEventToDocument {

        @Nested
        @DisplayName("when content-type is json")
        class WhenContentTypeIsJson {

            @SuppressWarnings("unchecked")
            @Test
            void and_data_is_byte_array_that_is_representable_as_map() {
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
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertAll(
                        () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                        () -> assertThat(document.getString("type")).isEqualTo("type"),
                        () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                        () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                        () -> assertThat(document.getString("id")).isEqualTo("id"),
                        () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                        () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                        () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
                );
            }

            @Test
            void and_data_is_byte_array_with_json_string() {
                // Given
                OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

                CloudEvent cloudEvent = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(offsetDateTime)
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withData("application/json", "name".getBytes(UTF_8))
                        .build();

                // When
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertThat(document.getString("data")).isEqualTo("name");
            }

            @SuppressWarnings("unchecked")
            @Test
            void and_data_is_pojo_cloud_event_data_with_document() {
                // Given
                OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

                CloudEvent cloudEvent = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(offsetDateTime)
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("application/json")
                        .withData(PojoCloudEventData.wrap(Document.parse("{\"name\" : \"hello\"}"), OccurrentCloudEventMongoDBDocumentMapperTest::convertDocumentToBytes))
                        .build();

                // When
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertAll(
                        () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                        () -> assertThat(document.getString("type")).isEqualTo("type"),
                        () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                        () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                        () -> assertThat(document.getString("id")).isEqualTo("id"),
                        () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                        () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                        () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
                );
            }

            @SuppressWarnings("unchecked")
            @Test
            void and_data_is_pojo_cloud_event_data_with_map() {
                // Given
                OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

                Map<String, Object> map = new HashMap<>();
                map.put("name", "hello");

                CloudEvent cloudEvent = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(offsetDateTime)
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("application/json")
                        .withData(PojoCloudEventData.wrap(map, __ -> "!!!invalid!!!".getBytes(UTF_8)))
                        .build();

                // When
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertAll(
                        () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                        () -> assertThat(document.getString("type")).isEqualTo("type"),
                        () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                        () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                        () -> assertThat(document.getString("id")).isEqualTo("id"),
                        () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                        () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                        () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
                );
            }

            @SuppressWarnings("unchecked")
            @Test
            void and_data_is_pojo_cloud_event_data_with_string() {
                // Given
                OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);


                CloudEvent cloudEvent = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(offsetDateTime)
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("application/json")
                        .withData(PojoCloudEventData.wrap("{\"name\" : \"hello\"}", __ -> "!!!invalid!!!".getBytes(UTF_8)))
                        .build();

                // When
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertAll(
                        () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                        () -> assertThat(document.getString("type")).isEqualTo("type"),
                        () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                        () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                        () -> assertThat(document.getString("id")).isEqualTo("id"),
                        () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                        () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                        () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
                );
            }

            @SuppressWarnings("unchecked")
            @Test
            void and_data_is_json_cloud_event_data() throws JsonProcessingException {
                // Given
                OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

                ObjectMapper objectMapper = new ObjectMapper();
                CloudEvent cloudEvent = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(offsetDateTime)
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("application/json")
                        .withData(JsonCloudEventData.wrap(objectMapper.readTree("{\"name\" : \"hello\"}")))
                        .build();

                // When
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertAll(
                        () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                        () -> assertThat(document.getString("type")).isEqualTo("type"),
                        () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                        () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                        () -> assertThat(document.getString("id")).isEqualTo("id"),
                        () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                        () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                        () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
                );
            }
        }

        @Nested
        @DisplayName("when content-type is text")
        class WhenContentTypeIsText {

            @Test
            void and_data_is_byte_array() {
                // Given
                OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

                CloudEvent cloudEvent = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(offsetDateTime)
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withData("text/plain", "text".getBytes(UTF_8))
                        .build();

                // When
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertAll(
                        () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                        () -> assertThat(document.getString("type")).isEqualTo("type"),
                        () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                        () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                        () -> assertThat(document.getString("id")).isEqualTo("id"),
                        () -> assertThat(document.getString("data")).isEqualTo("text"),
                        () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                        () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
                );
            }
        }

        @Nested
        @DisplayName("when content-type is not json or text")
        class WhenContentTypeIsNotJson {

            @Test
            void and_data_is_byte_array() {
                // Given
                OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

                CloudEvent cloudEvent = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(offsetDateTime)
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("application/octet-stream")
                        .withData("hello".getBytes(UTF_8))
                        .build();

                // When
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertAll(
                        () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                        () -> assertThat(document.getString("datacontenttype")).isEqualTo("application/octet-stream"),
                        () -> assertThat(document.getString("type")).isEqualTo("type"),
                        () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                        () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                        () -> assertThat(document.getString("id")).isEqualTo("id"),
                        () -> assertThat(document.get("data", byte[].class)).isEqualTo("hello".getBytes(UTF_8)),
                        () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                        () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
                );
            }

            @Test
            void and_data_is_bytes_cloud_event_data() {
                // Given
                OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 223_000000), UTC);

                CloudEvent cloudEvent = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(offsetDateTime)
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("application/octet-stream")
                        .withData(BytesCloudEventData.wrap("hello".getBytes(UTF_8)))
                        .build();

                // When
                Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

                // Then
                assertAll(
                        () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                        () -> assertThat(document.getString("type")).isEqualTo("type"),
                        () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                        () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                        () -> assertThat(document.getString("id")).isEqualTo("id"),
                        () -> assertThat(document.get("data", byte[].class)).isEqualTo("hello".getBytes(UTF_8)),
                        () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                        () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
                );
            }
        }
    }

    @Nested
    @DisplayName("converts document to cloud event")
    class ConvertsDocumentToCloudEvent {

        @Nested
        @DisplayName("when content-type is json")
        class WhenContentTypeIsJson {

            @Test
            void and_data_is_hash_map() {
                // Given
                Map<String, Object> data = new HashMap<String, Object>() {{
                    put("name", "hello");
                }};
                Document document = new Document(new HashMap<String, Object>() {{
                    put("subject", "subject");
                    put("type", "type");
                    put("time", Time.parseTime("2020-07-26T09:13:03Z"));
                    put("source", "urn:name");
                    put("id", "id");
                    put("_id", "mongodb");
                    put("data", data);
                    put("datacontenttype", "application/json");
                    put("specversion", "1.0");
                    put("streamid", "streamid");
                    put("streamversion", 2L);
                }});

                // When
                CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(DATE, document);

                // Then
                CloudEvent expected = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("application/json")
                        .withData(PojoCloudEventData.wrap(new Document(data), OccurrentCloudEventMongoDBDocumentMapperTest::convertDocumentToBytes))
                        .withExtension(new OccurrentCloudEventExtension("streamid", 2L))
                        .build();

                assertThat(actual).isEqualTo(expected);
            }

            @Test
            void and_data_is_string() {
                // Given
                String data = "{\"name\" : \"hello\"}";
                Document document = new Document(new HashMap<String, Object>() {{
                    put("subject", "subject");
                    put("type", "type");
                    put("time", Time.parseTime("2020-07-26T09:13:03Z"));
                    put("source", "urn:name");
                    put("id", "id");
                    put("_id", "mongodb");
                    put("data", data);
                    put("datacontenttype", "application/json");
                    put("specversion", "1.0");
                    put("streamid", "streamid");
                    put("streamversion", 2L);
                }});

                // When
                CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(DATE, document);

                // Then
                CloudEvent expected = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("application/json")
                        .withData(PojoCloudEventData.wrap(Document.parse(data), OccurrentCloudEventMongoDBDocumentMapperTest::convertDocumentToBytes))
                        .withExtension(new OccurrentCloudEventExtension("streamid", 2L))
                        .build();

                assertThat(actual).isEqualTo(expected);
            }
        }

        @Nested
        @DisplayName("when content-type is not json")
        class WhenContentTypeIsNotJson {

            @Test
            void and_data_is_string() {
                // Given
                String data = "string";
                Document document = new Document(new HashMap<String, Object>() {{
                    put("subject", "subject");
                    put("type", "type");
                    put("time", Time.parseTime("2020-07-26T09:13:03Z"));
                    put("source", "urn:name");
                    put("id", "id");
                    put("_id", "mongodb");
                    put("data", data);
                    put("datacontenttype", "text/plain");
                    put("specversion", "1.0");
                    put("streamid", "streamid");
                    put("streamversion", 2L);
                }});

                // When
                CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(DATE, document);

                // Then
                CloudEvent expected = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("text/plain")
                        .withData(BytesCloudEventData.wrap(data.getBytes(UTF_8)))
                        .withExtension(new OccurrentCloudEventExtension("streamid", 2L))
                        .build();

                assertThat(actual).isEqualTo(expected);
            }

            @Test
            void and_data_is_byte_array() {
                // Given
                byte[] data = "string".getBytes(UTF_8);
                Document document = new Document(new HashMap<String, Object>() {{
                    put("subject", "subject");
                    put("type", "type");
                    put("time", Time.parseTime("2020-07-26T09:13:03Z"));
                    put("source", "urn:name");
                    put("id", "id");
                    put("_id", "mongodb");
                    put("data", data);
                    put("datacontenttype", "text/plain");
                    put("specversion", "1.0");
                    put("streamid", "streamid");
                    put("streamversion", 2L);
                }});

                // When
                CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(DATE, document);

                // Then
                CloudEvent expected = new CloudEventBuilder()
                        .withSubject("subject")
                        .withType("type")
                        .withTime(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                        .withSource(URI.create("urn:name"))
                        .withId("id")
                        .withDataContentType("text/plain")
                        .withData(BytesCloudEventData.wrap(data))
                        .withExtension(new OccurrentCloudEventExtension("streamid", 2L))
                        .build();

                assertThat(actual).isEqualTo(expected);
            }
        }
    }

    @Nested
    @DisplayName("time representation is rfc 3339 string")
    class TimeRepresentationRfc3339String {

        @SuppressWarnings("unchecked")
        @Test
        void converts_cloud_event_to_document_with_expected_values() {
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
            Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(RFC_3339_STRING, "streamid", 2L, cloudEvent);

            // Then
            assertAll(
                    () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                    () -> assertThat(document.getString("type")).isEqualTo("type"),
                    () -> assertThat(document.getString("time")).isEqualTo(RFC_3339_DATE_TIME_FORMATTER.format(offsetDateTime)),
                    () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                    () -> assertThat(document.getString("id")).isEqualTo("id"),
                    () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                    () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                    () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
            );
        }

        @SuppressWarnings("unchecked")
        @Test
        void converts_cloud_event_with_nanoseconds_in_non_utc_timezone_to_rfc_3339() {
            // Given
            OffsetDateTime offsetDateTime = offsetDateTimeFrom(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 123_456_789), ZoneId.of("Europe/Stockholm"));

            CloudEvent cloudEvent = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(offsetDateTime)
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withData("application/json", "{\"name\" : \"hello\"}".getBytes(UTF_8))
                    .build();

            // When
            Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(RFC_3339_STRING, "streamid", 2L, cloudEvent);

            // Then
            assertAll(
                    () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                    () -> assertThat(document.getString("type")).isEqualTo("type"),
                    () -> assertThat(document.getString("time")).isEqualTo(RFC_3339_DATE_TIME_FORMATTER.format(offsetDateTime)),
                    () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                    () -> assertThat(document.getString("id")).isEqualTo("id"),
                    () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                    () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                    () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
            );
        }


        @Test
        void converts_document_to_cloud_event_and_remove_mongo_id() {
            // Given
            Map<String, Object> data = new HashMap<String, Object>() {{
                put("name", "hello");
            }};
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", "2020-07-26T09:13:03Z");
                put("source", "urn:name");
                put("id", "id");
                put("_id", "mongodb");
                put("data", data);
                put("datacontenttype", "application/json");
                put("specversion", "1.0");
                put("streamid", "streamid");
                put("streamversion", 2L);
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(RFC_3339_STRING, document);

            // Then
            CloudEvent expected = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withDataContentType("application/json")
                    .withData(PojoCloudEventData.wrap(new Document(data), OccurrentCloudEventMongoDBDocumentMapperTest::convertDocumentToBytes))
                    .withExtension(new OccurrentCloudEventExtension("streamid", 2L))
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
                put("streamid", "streamid");
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(RFC_3339_STRING, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 234_000000), UTC));
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
                put("streamid", "streamid");
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(RFC_3339_STRING, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(offsetDateTimeFrom(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneId.of("CET")));
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
                put("streamid", "streamid");
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(RFC_3339_STRING, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneOffset.of("-02:00")));
        }
    }

    @Nested
    @DisplayName("time representation is date")
    class TimeRepresentationDate {

        @SuppressWarnings("unchecked")
        @Test
        void converts_cloud_event_to_document_with_expected_values() {
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
            Document document = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent);

            // Then
            assertAll(
                    () -> assertThat(document.getString("subject")).isEqualTo("subject"),
                    () -> assertThat(document.getString("type")).isEqualTo("type"),
                    () -> assertThat(document.getDate("time")).isEqualTo(toDate(offsetDateTime)),
                    () -> assertThat(document.getString("source")).isEqualTo("urn:name"),
                    () -> assertThat(document.getString("id")).isEqualTo("id"),
                    () -> assertThat(document.get("data", Map.class)).containsOnly(entry("name", "hello")),
                    () -> assertThat(document.getString("streamid")).isEqualTo("streamid"),
                    () -> assertThat(document.getLong("streamversion")).isEqualTo(2L)
            );
        }

        @Test
        void throws_iae_when_time_is_using_with_nanoseconds_and_timezone_is_utc() {
            // Given
            OffsetDateTime offsetDateTime = OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 123_456_789), UTC);

            CloudEvent cloudEvent = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(offsetDateTime)
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withData("application/json", "{\"name\" : \"hello\"}".getBytes(UTF_8))
                    .build();

            // When
            Throwable throwable = catchThrowable(() -> OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent));

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("The OffsetDateTime in the CloudEvent time field contains micro-/nanoseconds. This is is not possible to represent when using TimeRepresentation DATE, either change to TimeRepresentation RFC_3339_STRING or remove micro-/nanoseconds using \"offsetDateTime.truncatedTo(ChronoUnit.MILLIS)\".");
        }

        @Test
        void throws_iae_when_time_another_timezone_than_utc() {
            // Given
            OffsetDateTime offsetDateTime = offsetDateTimeFrom(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 123_000_000), ZoneId.of("Europe/Stockholm")).truncatedTo(MILLIS);

            CloudEvent cloudEvent = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(offsetDateTime)
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withData("application/json", "{\"name\" : \"hello\"}".getBytes(UTF_8))
                    .build();

            // When
            Throwable throwable = catchThrowable(() -> OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(DATE, "streamid", 2L, cloudEvent));

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("The OffsetDateTime in the CloudEvent time field is not defined in UTC. TimeRepresentation DATE require UTC as timezone to not loose precision. Either change to TimeRepresentation RFC_3339_STRING or convert the OffsetDateTime to UTC using e.g. \"offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC)\".");
        }

        @Test
        void converts_document_to_cloud_event_and_remove_mongo_id() {
            // Given
            Map<String, Object> data = new HashMap<String, Object>() {{
                put("name", "hello");
            }};
            Document document = new Document(new HashMap<String, Object>() {{
                put("subject", "subject");
                put("type", "type");
                put("time", toDate("2020-07-26T09:13:03Z"));
                put("source", "urn:name");
                put("id", "id");
                put("_id", "mongodb");
                put("data", data);
                put("datacontenttype", "application/json");
                put("specversion", "1.0");
                put("streamid", "streamid");
                put("streamversion", 2L);
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(DATE, document);

            // Then
            CloudEvent expected = new CloudEventBuilder()
                    .withSubject("subject")
                    .withType("type")
                    .withTime(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), UTC))
                    .withSource(URI.create("urn:name"))
                    .withId("id")
                    .withDataContentType("application/json")
                    .withData(PojoCloudEventData.wrap(new Document(data), OccurrentCloudEventMongoDBDocumentMapperTest::convertDocumentToBytes))
                    .withExtension(new OccurrentCloudEventExtension("streamid", 2L))
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
                put("streamid", "streamid");
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(DATE, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3, 234_000000), UTC));
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
                put("streamid", "streamid");
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(DATE, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(offsetDateTimeFrom(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneId.of("CET")));
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
                put("streamid", "streamid");
            }});

            // When
            CloudEvent actual = OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(DATE, document);

            // Then
            assertThat(actual.getTime()).isEqualTo(OffsetDateTime.of(LocalDateTime.of(2020, 7, 26, 9, 13, 3), ZoneOffset.of("-02:00")));
        }

    }

    private Date toDate(OffsetDateTime offsetDateTime) {
        return Date.from(offsetDateTime.toInstant());
    }

    private Date toDate(String rfc3339FormattedString) {
        return toDate(OffsetDateTime.from(RFC_3339_DATE_TIME_FORMATTER.parse(rfc3339FormattedString)));
    }

    private static OffsetDateTime offsetDateTimeFrom(LocalDateTime ldf, ZoneId zoneId) {
        return ZonedDateTime.of(ldf, zoneId).toOffsetDateTime();
    }

    private static byte[] convertDocumentToBytes(Document document) {
        return document.toJson().getBytes(UTF_8);
    }
}