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

package org.occurrent.subscription.redis.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.occurrent.testsupport.previousrelease.PreviousRelease;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.tck.ConformanceEvents.eventAt;

/**
 * A catch-up stores a marked checkpoint in Redis when its replay runs without the live delegate. Occurrent 0.34 must
 * refuse the marked form before it writes anything, and this release must resume from it.
 */
@Timeout(120)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringRedisMarkedCheckpointPreviousReleaseTest {

    private static final Duration AT_MOST = Duration.ofSeconds(30);
    private static final String DATABASE = "redismarkedcheckpoint";
    private static final String EVENTS = "events";
    private static final String SUBSCRIPTION = "sub";
    private static final String VERSION_KEY_PREFIX = "occurrent:checkpoint-version:";
    private static final OffsetDateTime TIME = OffsetDateTime.of(2026, 10, 1, 12, 0, 0, 0, ZoneOffset.UTC);

    // Subscribes the way a 0.34 node resumes a subscription from a Redis checkpoint storage
    private static final String READER = """
            import com.mongodb.client.MongoClient;
            import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
            import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
            import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
            import org.occurrent.subscription.StartAt;
            import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
            import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
            import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
            import org.occurrent.subscription.redis.spring.blocking.SpringRedisCheckpointStorage;
            import org.springframework.data.mongodb.MongoTransactionManager;
            import org.springframework.data.mongodb.core.MongoTemplate;
            import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
            import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
            import org.springframework.data.redis.core.RedisTemplate;

            import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

            public class Reader {
                public static Object run(Object... args) {
                    MongoClient client = (MongoClient) args[0];
                    String database = (String) args[1];
                    boolean writesPosition = (Boolean) args[2];
                    LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory((String) args[3], (Integer) args[4]);
                    connectionFactory.afterPropertiesSet();
                    RedisTemplate<String, String> redis = new RedisTemplate<>();
                    redis.setConnectionFactory(connectionFactory);
                    redis.afterPropertiesSet();
                    MongoTemplate template = new MongoTemplate(client, database);
                    EventStoreConfig.Builder config = new EventStoreConfig.Builder().eventStoreCollectionName("events")
                            .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                            .timeRepresentation(TimeRepresentation.RFC_3339_STRING);
                    SpringMongoEventStore eventStore = new SpringMongoEventStore(template,
                            (writesPosition ? config.withStreamPosition() : config.withoutStreamPosition()).build());
                    StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(
                            new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING), eventStore,
                            new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new SpringRedisCheckpointStorage(redis))));
                    try {
                        catchup.subscribe("sub", StartAt.subscriptionModelDefault(), event -> {
                        }).waitUntilStarted();
                        return null;
                    } finally {
                        catchup.shutdown();
                        connectionFactory.destroy();
                    }
                }
            }
            """;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
    @Container
    private static final GenericContainer<?> redisContainer = new GenericContainer<>("redis:5.0.3-alpine").withExposedPorts(6379);

    @RegisterExtension
    OccurrentMongoFlush flushMongo = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, DATABASE));

    @RegisterExtension
    FlushRedisExtension flushRedis = new FlushRedisExtension(redisContainer.getHost(), redisContainer.getFirstMappedPort());

    private static PreviousRelease.Driver reader;

    private final CountDownLatch crashed = new CountDownLatch(1);
    private final List<StreamCatchupSubscriptionModel> models = new ArrayList<>();
    private MongoClient mongoClient;
    private String databaseName;
    private MongoTemplate mongoTemplate;
    private LettuceConnectionFactory connectionFactory;
    private RedisTemplate<String, String> redis;

    @BeforeAll
    static void compile_the_0_34_reader() {
        reader = PreviousRelease.fromJarsIn(Path.of("target", "occurrent-0.34.0")).compile("Reader", READER);
    }

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        connectionFactory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        connectionFactory.afterPropertiesSet();
        redis = new RedisTemplate<>();
        redis.setConnectionFactory(connectionFactory);
        redis.afterPropertiesSet();
    }

    @AfterEach
    void shutdown() {
        crashed.countDown();
        models.forEach(StreamCatchupSubscriptionModel::shutdown);
        connectionFactory.destroy();
        mongoClient.close();
    }

    @Test
    void a_position_a_replay_without_the_live_delegate_stored_is_refused_by_0_34_and_resumed_by_this_release() {
        // Given
        SpringMongoEventStore eventStore = eventStore(true);
        String stored = storeMarkedCheckpointByStoppingInTheReplay(eventStore, StartAt.checkpoint(GlobalCheckpoint.of(0)));
        assertThat(CatchupCheckpoints.isMarked(new StringBasedCheckpoint(stored)) && !CatchupCheckpoints.isMarkedTime(new StringBasedCheckpoint(stored)))
                .as("the position the replay stored, %s, is the marked form of a position replay", stored).isTrue();

        // When
        // Then
        verifyRefusedBy034AndResumedByThisRelease(eventStore, stored, true);
    }

    @Test
    void a_time_a_replay_without_the_live_delegate_stored_on_a_store_writing_no_positions_is_refused_by_0_34_and_resumed_by_this_release_by_time() {
        // Given
        SpringMongoEventStore eventStore = eventStore(false);
        String stored = storeMarkedCheckpointByStoppingInTheReplay(eventStore, StartAtTime.beginningOfTime());
        assertThat(CatchupCheckpoints.isMarkedTime(new StringBasedCheckpoint(stored)))
                .as("the time the replay stored, %s, is the marked form of a time replay", stored).isTrue();

        // When
        // Then
        verifyRefusedBy034AndResumedByThisRelease(eventStore, stored, false);
    }

    private void verifyRefusedBy034AndResumedByThisRelease(SpringMongoEventStore eventStore, String stored, boolean writesPosition) {
        assertThat(redis.opsForValue().get(SUBSCRIPTION)).as("the whole marked form under the subscription key").isEqualTo(stored);
        Map<String, String> keys = rawRedisKeysAndValues();
        assertThat(keys.keySet()).as("the Redis keys after the replay stored the marked form")
                .contains(SUBSCRIPTION).allSatisfy(key -> assertThat(key).as("a key besides the checkpoint").satisfiesAnyOf(
                        k -> assertThat(k).isEqualTo(SUBSCRIPTION), k -> assertThat(k).startsWith(VERSION_KEY_PREFIX)));

        assertThatThrownBy(() -> reader.run(mongoClient, databaseName, writesPosition, redisContainer.getHost(), redisContainer.getFirstMappedPort()))
                .as("what 0.34 throws when it resumes from the marked form")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not a global checkpoint: " + stored);
        assertThat(rawRedisKeysAndValues()).as("the raw Redis keys and values after 0.34 refused the marked form").isEqualTo(keys);

        List<String> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel restarted = catchup(eventStore);
        restarted.subscribe(SUBSCRIPTION, StartAt.subscriptionModelDefault(), event -> received.add(event.getId())).waitUntilStarted();
        eventStore.write("stream-d", List.of(eventAt("D", "Happened", TIME.plusMinutes(3))));
        await().atMost(AT_MOST).untilAsserted(() -> assertThat(received).as("what the restarted subscription received").contains("D"));
        assertThat(received).as("the events after the stored checkpoint, replayed from it").contains("B", "C");
    }

    // Stores the marked form the way a crash leaves it, a replay without the live delegate that stored a checkpoint
    // for A and stopped while delivering B
    private String storeMarkedCheckpointByStoppingInTheReplay(SpringMongoEventStore eventStore, StartAt origin) {
        eventStore.write("stream-a", List.of(eventAt("A", "Happened", TIME)));
        eventStore.write("stream-b", List.of(eventAt("B", "Happened", TIME.plusMinutes(1))));
        eventStore.write("stream-c", List.of(eventAt("C", "Happened", TIME.plusMinutes(2))));
        StreamCatchupSubscriptionModel first = catchup(eventStore);
        CountDownLatch deliveringB = new CountDownLatch(1);
        StartAt withoutTheLiveDelegate = StartAt.dynamic(context -> context.hasSubscriptionModelType(SpringMongoSubscriptionModel.class) ? null : origin);

        first.subscribe(SUBSCRIPTION, withoutTheLiveDelegate, (CloudEvent event) -> {
            if (event.getId().equals("B")) {
                deliveringB.countDown();
                awaitLatch(crashed);
            }
        });

        awaitLatch(deliveringB);
        first.shutdown();
        return requireNonNull(checkpointStorage().read(SUBSCRIPTION), "a checkpoint stored for A").asString();
    }

    // Every key, decoded, with its value as the raw bytes Redis holds, so a rewrite in any encoding shows
    private Map<String, String> rawRedisKeysAndValues() {
        return requireNonNull(redis.execute((RedisCallback<Map<String, String>>) connection -> {
            Map<String, String> keysAndValues = new TreeMap<>();
            for (byte[] key : requireNonNull(connection.keyCommands().keys("*".getBytes(StandardCharsets.UTF_8)))) {
                keysAndValues.put(String.valueOf(redis.getKeySerializer().deserialize(key)),
                        Base64.getEncoder().encodeToString(requireNonNull(connection.stringCommands().get(key))));
            }
            return keysAndValues;
        }));
    }

    private SpringMongoEventStore eventStore(boolean writesPosition) {
        EventStoreConfig.Builder config = new EventStoreConfig.Builder().eventStoreCollectionName(EVENTS)
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).eventStoreCapabilities(STREAM);
        return new SpringMongoEventStore(mongoTemplate, (writesPosition ? config.withStreamPosition() : config.withoutStreamPosition()).build());
    }

    private CheckpointStorage checkpointStorage() {
        return new SpringRedisCheckpointStorage(redis);
    }

    private StreamCatchupSubscriptionModel catchup(SpringMongoEventStore eventStore) {
        SpringMongoSubscriptionModel live = new SpringMongoSubscriptionModel(mongoTemplate, EVENTS, TimeRepresentation.RFC_3339_STRING);
        StreamCatchupSubscriptionModel model = new StreamCatchupSubscriptionModel(live, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(checkpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        models.add(model);
        return model;
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(60, TimeUnit.SECONDS)).as("the latch was counted down in time").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
