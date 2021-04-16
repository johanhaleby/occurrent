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

package org.occurrent.example.springevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.time.TimeConversion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Hooks;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ForwardEventsFromMongoDBToSpringApplication.class)
@Testcontainers
public class MongoSubscriptionToSpringEventTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.8");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @BeforeAll
    static void enableDebug() {
        Hooks.onOperatorDebug();
    }

    @AfterAll
    static void disableDebug() {
        Hooks.resetOnOperatorDebug();
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EventStore mongoEventStore;

    @Autowired
    private EventListenerExample eventListenerExample;

    @Test
    void publishes_events_to_spring_event_publisher() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
        NameWasChanged nameWasChanged = new NameWasChanged(UUID.randomUUID().toString(), now, "another name");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined, nameWasChanged));

        // Then
        await().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
            Assertions.assertThat(eventListenerExample.definedNames).containsExactly(nameDefined);
            Assertions.assertThat(eventListenerExample.changedNames).containsExactly(nameWasChanged);
        });
    }

    private Stream<CloudEvent> serialize(DomainEvent... events) {
        return Stream.of(events)
                .map(e -> CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getSimpleName())
                        .withTime(TimeConversion.toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build());
    }
}