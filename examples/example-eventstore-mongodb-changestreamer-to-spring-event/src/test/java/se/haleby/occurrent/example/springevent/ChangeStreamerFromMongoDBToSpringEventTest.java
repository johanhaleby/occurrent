package se.haleby.occurrent.example.springevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Hooks;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static se.haleby.occurrent.functional.CheckedFunction.unchecked;
import static se.haleby.occurrent.time.TimeConversion.toLocalDateTime;

@SpringBootTest(classes = ChangeStreamerFromMongoDBToSpringEventApplication.class)
@Testcontainers
public class ChangeStreamerFromMongoDBToSpringEventTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.7");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.setPortBindings(ports);
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
            assertThat(eventListenerExample.definedNames).containsExactly(nameDefined);
            assertThat(eventListenerExample.changedNames).containsExactly(nameWasChanged);
        });
    }

    private Stream<CloudEvent> serialize(DomainEvent... events) {
        return Stream.of(events)
                .map(e -> CloudEventBuilder.v1()
                        .withId(UUID.randomUUID().toString())
                        .withSource(URI.create("http://name"))
                        .withType(e.getClass().getSimpleName())
                        .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                        .withSubject(e.getName())
                        .withDataContentType("application/json")
                        .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build());
    }
}