package se.haleby.occurrent.changestreamer.mongodb.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.domain.DomainEvent;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.reactor.SpringReactorMongoEventStore;
import se.haleby.occurrent.eventstore.mongodb.spring.reactor.StreamConsistencyGuarantee;
import se.haleby.occurrent.testsupport.mongodb.FlushMongoDBExtension;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static se.haleby.occurrent.functional.CheckedFunction.unchecked;
import static se.haleby.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
public class SpringReactiveChangeStreamerForMongoDBTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.7");

    private SpringReactorMongoEventStore mongoEventStore;
    private SpringReactiveChangeStreamerForMongoDB changeStreamer;
    private ObjectMapper objectMapper;
    private CopyOnWriteArrayList<Disposable> disposables;

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));
    private MongoClient mongoClient;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, Objects.requireNonNull(connectionString.getDatabase()));
        changeStreamer = new SpringReactiveChangeStreamerForMongoDB(reactiveMongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        ReactiveTransactionManager reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        mongoEventStore = new SpringReactorMongoEventStore(reactiveMongoTemplate, new EventStoreConfig("events", StreamConsistencyGuarantee.transactional("event-consistency", reactiveMongoTransactionManager), TimeRepresentation.RFC_3339_STRING));
        objectMapper = new ObjectMapper();
        disposables = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
    }

    @Test
    void reactive_spring_change_streamer_calls_listener_for_each_new_event() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        disposeAfterTest(changeStreamer.stream().flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent))).subscribe());
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();

        // Then
        await().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    private Flux<CloudEvent> serialize(DomainEvent e) {
        return Flux.just(CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getSimpleName())
                .withTime(toLocalDateTime(e.getTimestamp()).atZone(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }

    private void disposeAfterTest(Disposable disposable) {
        disposables.add(disposable);
    }
}