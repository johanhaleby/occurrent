/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.application.converter.xstream;

import com.thoughtworks.xstream.XStream;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertAll;

public class XStreamCloudEventConverterTest {

    private static final URI CLOUD_EVENT_SOURCE = URI.create("urn:occurrent:domain");

    @SuppressWarnings("ConstantConditions")
    @Test
    void converts_domain_event_to_cloud_event_using_default_configuration() {
        // Given
        XStream xStream = new XStream();
        XStreamCloudEventConverter<DomainEvent> cloudEventConverter = new XStreamCloudEventConverter<>(xStream, CLOUD_EVENT_SOURCE);
        NameDefined domainEvent = new NameDefined(UUID.randomUUID().toString(), new Date(), "name");

        // When
        CloudEvent cloudEvent = cloudEventConverter.toCloudEvent(domainEvent);

        // Then
        assertAll(
                () -> assertThat(cloudEvent.getId()).matches("([a-f0-9]{8}(-[a-f0-9]{4}){4}[a-f0-9]{8})"),
                () -> assertThat(cloudEvent.getType()).isEqualTo(NameDefined.class.getSimpleName()),
                () -> assertThat(cloudEvent.getSource()).isEqualTo(CLOUD_EVENT_SOURCE),
                () -> assertThat(cloudEvent.getSubject()).isNull(),
                () -> assertThat(cloudEvent.getTime()).isCloseToUtcNow(within(3, ChronoUnit.SECONDS)),
                () -> assertThat(cloudEvent.getDataSchema()).isNull(),
                () -> assertThat(cloudEvent.getDataContentType()).isEqualTo("application/xml"),
                () -> assertThat(new String(cloudEvent.getData().toBytes(), UTF_8)).isEqualTo(xStream.toXML(domainEvent))
        );
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void converts_domain_event_to_cloud_event_using_non_default_configuration() {
        // Given
        XStream xStream = new XStream();
        XStreamCloudEventConverter<DomainEvent> cloudEventConverter = new XStreamCloudEventConverter.Builder<DomainEvent>(xStream, CLOUD_EVENT_SOURCE)
                .charset(UTF_16LE)
                .contentType("application/xstream+xml")
                .idMapper(DomainEvent::getEventId)
                .subjectMapper(__ -> "subject")
                .timeMapper(domainEvent -> OffsetDateTime.ofInstant(domainEvent.getTimestamp().toInstant(), UTC))
                .typeGetter(Class::getSimpleName)
                .build();

        NameDefined domainEvent = new NameDefined(UUID.randomUUID().toString(), new Date(), "name");

        // When
        CloudEvent cloudEvent = cloudEventConverter.toCloudEvent(domainEvent);

        // Then
        assertAll(
                () -> assertThat(cloudEvent.getId()).isEqualTo(domainEvent.getEventId()),
                () -> assertThat(cloudEvent.getType()).isEqualTo(NameDefined.class.getSimpleName()),
                () -> assertThat(cloudEvent.getSource()).isEqualTo(CLOUD_EVENT_SOURCE),
                () -> assertThat(cloudEvent.getSubject()).isEqualTo("subject"),
                () -> assertThat(cloudEvent.getTime()).isEqualTo(OffsetDateTime.ofInstant(domainEvent.getTimestamp().toInstant(), UTC)),
                () -> assertThat(cloudEvent.getDataSchema()).isNull(),
                () -> assertThat(cloudEvent.getDataContentType()).isEqualTo("application/xstream+xml"),
                () -> assertThat(new String(cloudEvent.getData().toBytes(), UTF_16LE)).isEqualTo(xStream.toXML(domainEvent))
        );
    }

    @Test
    void converts_cloud_event_to_domain_event() {
        // Given
        XStream xStream = new XStream();
        xStream.allowTypeHierarchy(DomainEvent.class);
        XStreamCloudEventConverter<DomainEvent> cloudEventConverter = new XStreamCloudEventConverter<>(xStream, CLOUD_EVENT_SOURCE);

        String xml = "<org.occurrent.domain.NameDefined>\n" +
                "  <eventId>45a5925b-b1df-41f2-b51b-d5bf6d0fba88</eventId>\n" +
                "  <timestamp>2021-09-24 10:39:03.556 UTC</timestamp>\n" +
                "  <name>name</name>\n" +
                "</org.occurrent.domain.NameDefined>";

        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(CLOUD_EVENT_SOURCE)
                .withType(NameDefined.class.getName())
                .withTime(OffsetDateTime.now())
                .withData(xml.getBytes(UTF_8))
                .build();

        // When
        DomainEvent domainEvent = cloudEventConverter.toDomainEvent(cloudEvent);

        // Then
        assertThat(domainEvent).isExactlyInstanceOf(NameDefined.class);
        NameDefined nameDefined = (NameDefined) domainEvent;
        assertAll(
                () -> assertThat(nameDefined.getName()).isEqualTo("name"),
                () -> assertThat(nameDefined.getEventId()).isEqualTo("45a5925b-b1df-41f2-b51b-d5bf6d0fba88"),
                () -> assertThat(nameDefined.getTimestamp()).isEqualTo(new Date(LocalDateTime.of(2021, 9, 24, 10, 39, 3, 556_000_000).atZone(UTC).toInstant().toEpochMilli()))
        );
    }
}
