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

package org.occurrent.subscription.blocking.durable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A subscribe with the model default for an id that is already running or paused is refused with the
 * {@link DuplicateSubscriptionIdException} the wrapped model throws, and stores no start position for the id. The
 * wrapped model is {@link NativeMongoSubscriptionModel} rather than a test double, so the exception is the one a
 * caller gets from it, and the storage can't delete anything, so a test passes only if nothing was stored.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelDuplicateSubscribeTest {

    private static final String DATABASE = "durablesubscriptionduplicatesubscribe";
    private static final Duration STARTED_TIMEOUT = Duration.ofSeconds(10);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private final Consumer<CloudEvent> action = cloudEvent -> {
    };

    private ExecutorService executor;
    private MongoCollection<Document> events;
    private MongoEventStore eventStore;
    private NativeMongoSubscriptionModel wrapped;
    private CheckpointStorage storage;
    private DurableSubscriptionModel durable;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void createModel() {
        executor = Executors.newCachedThreadPool();
        RetryStrategy retryStrategy = RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500), 2.0f);
        events = database.getCollection("events-" + UUID.randomUUID());
        eventStore = new MongoEventStore(mongoClient, database, events, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
        wrapped = new NativeMongoSubscriptionModel(database, events, TimeRepresentation.RFC_3339_STRING, executor, retryStrategy);
        storage = new NeverDeletes();
        durable = new DurableSubscriptionModel(wrapped, storage);
    }

    @AfterEach
    void shutDown() {
        durable.shutdown();
        executor.shutdownNow();
    }

    @Test
    void a_refused_duplicate_of_a_running_subscription_stores_no_start_position() {
        String id = UUID.randomUUID().toString();
        boolean started = durable.subscribe(id, StartAt.now(), action).waitUntilStarted(STARTED_TIMEOUT);
        assertThat(started).isTrue();
        assertThat(storage.exists(id)).as("precondition: nothing stored for the running subscription").isFalse();

        DuplicateSubscriptionIdException exception = catchThrowableOfType(DuplicateSubscriptionIdException.class, () -> durable.subscribe(id, action));

        assertThat(exception).hasMessage("Subscription " + id + " is already defined.");
        assertThat(storage.read(id)).as("the position a refused subscribe would have stored").isNull();
        assertThat(durable.isRunning(id)).isTrue();
    }

    @Test
    void a_refused_duplicate_of_a_paused_subscription_stores_no_start_position() {
        String id = UUID.randomUUID().toString();
        durable.subscribePaused(id, null, StartAt.now(), action);
        assertThat(durable.isPaused(id)).isTrue();
        assertThat(storage.exists(id)).as("precondition: nothing stored for the paused subscription").isFalse();

        DuplicateSubscriptionIdException exception = catchThrowableOfType(DuplicateSubscriptionIdException.class, () -> durable.subscribe(id, action));

        assertThat(exception).hasMessage("Subscription " + id + " is already defined.");
        assertThat(storage.read(id)).as("the position a refused subscribe would have stored").isNull();
        assertThat(durable.isPaused(id)).isTrue();
    }

    /**
     * The wrapped model here answers {@code false} from {@code isRunning(id)} and {@code isPaused(id)} for the paused
     * id, so only its subscribe refuses the duplicate. A position stored before that refusal is where the resume below
     * starts, after {@code e1}.
     */
    @Test
    void a_paused_subscription_resumes_from_where_it_was_paused_after_a_subscribe_of_its_id_is_refused() throws Exception {
        ForwardsToTheNativeModel forwarding = new ForwardsToTheNativeModel(wrapped);
        DurableSubscriptionModel durableOverForwarding = new DurableSubscriptionModel(forwarding, storage);
        List<String> received = new CopyOnWriteArrayList<>();
        String id = UUID.randomUUID().toString();
        assertThat(durableOverForwarding.subscribe(id, null, StartAt.now(), e -> received.add(e.getId())).waitUntilStarted(STARTED_TIMEOUT)).isTrue();
        durableOverForwarding.pauseSubscription(id);
        eventStore.write("stream", 0L, List.of(event("e1")));
        forwarding.missTheId = true;
        assertThatThrownBy(() -> durableOverForwarding.subscribe(id, action)).isInstanceOf(DuplicateSubscriptionIdException.class);
        forwarding.missTheId = false;
        assertThat(storage.read(id)).as("the first position the refused subscribe stored").isNull();
        assertThat(received).as("what the paused subscription received before the resume").isEmpty();

        assertThat(durableOverForwarding.resumeSubscription(id).waitUntilStarted(STARTED_TIMEOUT)).isTrue();
        eventStore.write("stream", 1L, List.of(event("e2")));

        awaitReceived(received, "e2");
        assertThat(received)
                .as("the resume starts where the subscription was paused, so the event written while it was paused comes first")
                .containsExactly("e1", "e2");
    }

    /**
     * A pause or a resume moves the id between running and paused, and a stop and a start move every id. A subscribe
     * of the id is tried again and again meanwhile, through a wrapped model that does not list its subscriptions, and
     * must leave nothing stored, since the running subscription started from {@code StartAt.now()} and handles no
     * event that would store one.
     */
    @Test
    @Timeout(60)
    void a_refused_duplicate_racing_pauses_resumes_stops_and_starts_never_leaves_a_start_position_behind() throws Exception {
        DurableSubscriptionModel durableOverForwarding = new DurableSubscriptionModel(new ForwardsToTheNativeModel(wrapped), storage);
        String id = UUID.randomUUID().toString();
        assertThat(durableOverForwarding.subscribe(id, null, StartAt.now(), action).waitUntilStarted(STARTED_TIMEOUT)).isTrue();
        AtomicBoolean racing = new AtomicBoolean(true);
        AtomicInteger moves = new AtomicInteger();
        Thread mover = Thread.ofPlatform().start(() -> {
            for (int round = 0; racing.get(); round++) {
                try {
                    if (round % 10 == 9) {
                        durableOverForwarding.stop();
                        durableOverForwarding.start(true);
                    } else {
                        wrapped.pauseSubscription(id);
                        wrapped.resumeSubscription(id);
                    }
                    moves.incrementAndGet();
                } catch (RuntimeException e) {
                    // A stop or a start can run into a pause or a resume of the same id, which is not what this tests
                }
            }
        });
        AtomicInteger refused = new AtomicInteger();
        try {
            long until = System.nanoTime() + Duration.ofSeconds(4).toNanos();
            while (System.nanoTime() < until) {
                assertThatThrownBy(() -> durableOverForwarding.subscribe(id, action)).isInstanceOf(DuplicateSubscriptionIdException.class);
                refused.incrementAndGet();
                assertThat(storage.read(id))
                        .as("the position a refused subscribe stored, after %s refusals", refused.get())
                        .isNull();
            }
        } finally {
            racing.set(false);
            mover.join(Duration.ofSeconds(30));
        }
        assertThat(mover.isAlive()).as("the thread pausing, resuming, stopping and starting the id").isFalse();
        assertThat(moves.get()).as("the pauses and resumes, and the stops and starts, that completed").isPositive();
    }

    @Test
    void a_pause_while_the_first_position_is_recorded_holds_the_subscription_paused_at_that_position() throws Exception {
        ForwardsToTheNativeModel forwarding = new ForwardsToTheNativeModel(wrapped);
        DurableSubscriptionModel durableOverForwarding = new DurableSubscriptionModel(forwarding, storage);
        List<String> received = new CopyOnWriteArrayList<>();
        String id = UUID.randomUUID().toString();
        forwarding.whileAskedForTheGlobalCheckpoint = () -> durableOverForwarding.pauseSubscription(id);

        durableOverForwarding.subscribe(id, e -> received.add(e.getId()));
        eventStore.write("stream", 0L, List.of(event("e1")));
        Thread.sleep(500);

        assertThat(durableOverForwarding.isPaused(id)).as("the subscription paused while its first position was recorded").isTrue();
        assertThat(received).as("what the paused subscription received").isEmpty();
        assertThat(storage.read(id)).as("the first position").isNotNull();
        assertThat(durableOverForwarding.resumeSubscription(id).waitUntilStarted(STARTED_TIMEOUT)).isTrue();
        awaitReceived(received, "e1");
        assertThat(received).as("the events from the first position on").containsExactly("e1");
    }

    @Test
    void a_stop_while_the_first_position_is_recorded_delivers_nothing_until_the_model_starts() throws Exception {
        ForwardsToTheNativeModel forwarding = new ForwardsToTheNativeModel(wrapped);
        DurableSubscriptionModel durableOverForwarding = new DurableSubscriptionModel(forwarding, storage);
        List<String> received = new CopyOnWriteArrayList<>();
        String id = UUID.randomUUID().toString();
        forwarding.whileAskedForTheGlobalCheckpoint = durableOverForwarding::stop;

        durableOverForwarding.subscribe(id, e -> received.add(e.getId()));
        eventStore.write("stream", 0L, List.of(event("e1")));
        Thread.sleep(500);

        assertThat(durableOverForwarding.isRunning()).as("the model stopped while the first position was recorded").isFalse();
        assertThat(received).as("what the subscription received while the model was stopped").isEmpty();
        durableOverForwarding.start(true);
        awaitReceived(received, "e1");
        assertThat(received).as("the events from the first position on").containsExactly("e1");
    }

    private static void awaitReceived(List<String> received, String eventId) throws InterruptedException {
        long until = System.nanoTime() + STARTED_TIMEOUT.toNanos();
        while (!received.contains(eventId) && System.nanoTime() < until) {
            Thread.sleep(20);
        }
        assertThat(received).contains(eventId);
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created")
                .withTime(OffsetDateTime.now()).withData("{}".getBytes()).withDataContentType("application/json").build();
    }

    /**
     * Stores and reads like {@link InMemoryCheckpointStorage}, but throws from {@code delete}, so nothing a subscribe
     * stored can be taken back.
     */
    private static final class NeverDeletes implements CheckpointStorage {
        private final InMemoryCheckpointStorage stored = new InMemoryCheckpointStorage();

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return stored.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return stored.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return stored.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            throw new UnsupportedOperationException("This storage never deletes");
        }

        @Override
        public boolean exists(String subscriptionId) {
            return stored.exists(subscriptionId);
        }
    }

    /**
     * Forwards to the native model, without listing its subscriptions. It answers {@code false} from
     * {@code isRunning(id)} and {@code isPaused(id)} while {@code missTheId} is set, and runs
     * {@code whileAskedForTheGlobalCheckpoint} once, when it is next asked for the global checkpoint.
     */
    private static final class ForwardsToTheNativeModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        private final NativeMongoSubscriptionModel delegate;
        volatile boolean missTheId;
        volatile @Nullable Runnable whileAskedForTheGlobalCheckpoint;

        ForwardsToTheNativeModel(NativeMongoSubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            Runnable hook = whileAskedForTheGlobalCheckpoint;
            whileAskedForTheGlobalCheckpoint = null;
            if (hook != null) {
                hook.run();
            }
            return delegate.globalCheckpoint();
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return delegate.resumeSubscription(subscriptionId, startAt);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return !missTheId && delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return !missTheId && delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
