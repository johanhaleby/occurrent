/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbReadInTransactionTest {

    // Sending the two reads together failed on about a quarter of the reads, so a broken order can't pass this many by luck
    private static final int READS = 30;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Tag TAG = Tag.parse("name:1");

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final OverlappingCommands overlappingCommands = new OverlappingCommands();
    private MongoClient mongoClient;
    private ReactiveMongoTransactionManager transactionManager;
    private ReactorMongoEventStore eventStore;

    @BeforeEach
    void create_event_store_with_one_event() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbreadintransaction");
        mongoClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(overlappingCommands).build());
        var databaseFactory = new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase()));
        transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(databaseFactory), config);
        eventStore.append(List.of(taggedEvent())).block(TIMEOUT);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @ParameterizedTest(name = "propagation behavior {0}")
    @ValueSource(ints = {TransactionDefinition.PROPAGATION_REQUIRED, TransactionDefinition.PROPAGATION_NOT_SUPPORTED})
    void a_read_with_transaction_synchronization_active_succeeds_and_runs_its_commands_one_at_a_time(int propagationBehavior) {
        TransactionalOperator operator = TransactionalOperator.create(transactionManager, new DefaultTransactionDefinition(propagationBehavior));
        overlappingCommands.reset();
        List<Throwable> failures = new ArrayList<>();

        for (int i = 0; i < READS; i++) {
            operator.transactional(eventStore.read(DcbCriteria.tags(TAG)))
                    .onErrorResume(failure -> {
                        failures.add(failure);
                        return Mono.empty();
                    })
                    .block(TIMEOUT);
        }

        assertThat(failures).isEmpty();
        assertThat(overlappingCommands.count()).isZero();
    }

    @Test
    void a_read_outside_any_transaction_still_runs_its_token_and_position_reads_concurrently() {
        overlappingCommands.reset();

        for (int i = 0; i < READS; i++) {
            eventStore.read(DcbCriteria.tags(TAG)).block(TIMEOUT);
        }

        assertThat(overlappingCommands.count()).isPositive();
    }

    private static CloudEvent taggedEvent() {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:test"))
                .withType("NameDefined")
                .withTime(OffsetDateTime.now())
                .withData("text/plain", "{}".getBytes(UTF_8))
                .build();
        return DcbCloudEvents.withTags(event, List.of(TAG));
    }

    // Counts the commands on the event store's collections that started while another of them was still running. The
    // reads run one at a time, so an overlap can only come from within one read.
    private static final class OverlappingCommands implements CommandListener {
        private final Map<Integer, Boolean> running = new ConcurrentHashMap<>();
        private final AtomicInteger overlapping = new AtomicInteger();

        void reset() {
            overlapping.set(0);
        }

        int count() {
            return overlapping.get();
        }

        @Override
        public void commandStarted(CommandStartedEvent event) {
            BsonValue target = event.getCommand().get(event.getCommandName());
            if (target != null && target.isString() && target.asString().getValue().startsWith("events")) {
                if (!running.isEmpty()) {
                    overlapping.incrementAndGet();
                }
                running.put(event.getRequestId(), true);
            }
        }

        @Override
        public void commandSucceeded(CommandSucceededEvent event) {
            running.remove(event.getRequestId());
        }

        @Override
        public void commandFailed(CommandFailedEvent event) {
            running.remove(event.getRequestId());
        }
    }
}
