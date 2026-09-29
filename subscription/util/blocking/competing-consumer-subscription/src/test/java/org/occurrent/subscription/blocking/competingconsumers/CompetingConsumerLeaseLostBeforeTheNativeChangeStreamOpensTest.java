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

package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A native subscription loses its lease while MongoDB cannot be reached, before its change stream has opened. The
 * competing consumer model pauses it through the native model, so once MongoDB is back the subscription stays paused
 * and this node delivers nothing. The test runs one node, and the lease loss is the listener call a strategy's refresh
 * would make when another node takes the lease, so nothing here waits for a lease to expire.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeaseLostBeforeTheNativeChangeStreamOpensTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final AtomicBoolean unreachable = new AtomicBoolean();
    private final AtomicInteger refusedChangeStreams = new AtomicInteger();

    private MongoClient mongoClient;
    private MongoDatabase database;
    private String eventCollection;
    private MongoEventStore eventStore;
    private CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollection = requireNonNull(connectionString.getCollection());
        eventStore = new MongoEventStore(mongoClient, requireNonNull(connectionString.getDatabase()), eventCollection, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
    }

    @AfterEach
    void shutdown() {
        if (competingConsumerSubscriptionModel != null) {
            competingConsumerSubscriptionModel.shutdown();
        }
        mongoClient.close();
    }

    @Timeout(value = 30, unit = SECONDS)
    @Test
    void a_subscription_that_loses_its_lease_before_its_change_stream_opens_delivers_nothing_once_mongodb_is_back() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollection, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool(),
                NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
        competingConsumerSubscriptionModel = new CompetingConsumerSubscriptionModel(nativeModel, new AlwaysGrantedCompetingConsumerStrategy());
        unreachable.set(true);
        competingConsumerSubscriptionModel.subscribe("A", subscriptionId, null, unreachableWhileToldToForTheNativeModel(), delivered::add);
        await().atMost(10, SECONDS).until(() -> refusedChangeStreams.get() >= 1);

        // When
        competingConsumerSubscriptionModel.onConsumeProhibited(subscriptionId, "A");
        unreachable.set(false);

        // Then
        NameDefined writtenOnceMongoDBIsBack = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name");
        eventStore.write(UUID.randomUUID().toString(), serialize(writtenOnceMongoDBIsBack));
        // Two seconds is twenty of the retry's backoffs, so a change stream still trying to open has opened by then
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4)).untilAsserted(() -> assertThat(delivered).isEmpty());
        assertThat(nativeModel.isPaused(subscriptionId)).isTrue();
    }

    // Resolved by the native model on every attempt to open the change stream, so throwing here fails the attempt
    // the way an unreachable MongoDB would, while the competing consumer model resolves it to the present
    private StartAt unreachableWhileToldToForTheNativeModel() {
        return StartAt.dynamic(context -> {
            if (context.hasSubscriptionModelType(NativeMongoSubscriptionModel.class) && unreachable.get()) {
                refusedChangeStreams.incrementAndGet();
                throw new MongoTimeoutException("MongoDB cannot be reached");
            }
            return StartAt.now();
        });
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(OBJECT_MAPPER::writeValueAsBytes).apply(e))
                .build());
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final class AlwaysGrantedCompetingConsumerStrategy implements CompetingConsumerStrategy {

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
        }
    }
}
