package se.haleby.occurrent.changestreamer.mongodb.spring.reactive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import se.haleby.occurrent.MongoEventStore;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class SpringReactiveChangeStreamerForMongoDBTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.7");
    private MongoEventStore mongoEventStore;
    private SpringReactiveChangeStreamerForMongoDB<DomainEvent> changeStreamer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void create_mongo_event_store() {
        objectMapper = new ObjectMapper();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoEventStore = new MongoEventStore(connectionString);
        ReactiveMongoOperations mongoOperations = new ReactiveMongoTemplate(MongoClients.create(connectionString), Objects.requireNonNull(connectionString.getDatabase()));
        changeStreamer = new SpringReactiveChangeStreamerForMongoDB<>(mongoOperations, "events", "ack");
    }

    @Test
    void calls_listener_for_each_new_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEventImpl<DomainEvent>> state = new CopyOnWriteArrayList<>();
        changeStreamer.subscribe("test", cloudEvent -> Mono.fromRunnable(() -> state.addAll(cloudEvent))).subscribeOn(Schedulers.newSingle("test")).subscribe();
        NameDefined nameDefined1 = new NameDefined(now, "name1");
        NameDefined nameDefined2 = new NameDefined(now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    private Stream<CloudEventImpl<DomainEvent>> serialize(DomainEvent e) {
        return Stream.of(CloudEventBuilder.<DomainEvent>builder()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getSimpleName())
                .withTime(e.getTime().atZone(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(e)
                .build());
    }
}