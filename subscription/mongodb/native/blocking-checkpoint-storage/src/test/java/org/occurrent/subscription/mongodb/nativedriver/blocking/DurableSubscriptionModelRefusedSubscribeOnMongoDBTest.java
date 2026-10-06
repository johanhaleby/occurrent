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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

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
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A subscribe of an id that {@code DurableSubscriptionModel} cannot hold, because the wrapped model refuses it, must
 * not take away the start position of the subscription of that id that does run. On an idle replica set two
 * subscribes read the same operation time, so the position a refused subscribe would record is often the very one
 * the running subscription recorded.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelRefusedSubscribeOnMongoDBTest {

    private static final String DATABASE = "durablesubscriptionrefusedsubscribe";
    private static final Duration STARTED_TIMEOUT = Duration.ofSeconds(10);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private final List<ExecutorService> executors = new ArrayList<>();
    private final List<DurableSubscriptionModel> models = new ArrayList<>();
    private MongoCollection<Document> events;
    private MongoCollection<Document> checkpoints;
    private MongoEventStore eventStore;

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
    void createCollections() {
        events = database.getCollection("events-" + UUID.randomUUID());
        checkpoints = database.getCollection("checkpoints-" + UUID.randomUUID());
        eventStore = new MongoEventStore(mongoClient, database, events, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
    }

    @AfterEach
    void shutDown() {
        models.forEach(model -> {
            try {
                model.shutdown();
            } catch (RuntimeException ignored) {
                // Already shut down by the test
            }
        });
        executors.forEach(ExecutorService::shutdownNow);
    }

    /**
     * Node Y holds the id paused, with nothing stored for it, and its wrapped model's answers miss the id. Node X
     * subscribes the id while Y's subscribe of it is under way, or right after it was refused. X then crashes before
     * it has handled anything, and restarts. An event written while X was down comes after X's start position.
     */
    @Test
    void a_subscribe_refused_on_another_node_leaves_the_start_position_the_running_subscription_restarts_from() throws Exception {
        NativeMongoSubscriptionModel nativeY = nativeModel();
        HookedNativeModel wrappedY = new HookedNativeModel(nativeY);
        DurableSubscriptionModel durableY = durable(wrappedY);
        DurableSubscriptionModel durableX = durable(nativeModel());
        String id = UUID.randomUUID().toString();
        assertThat(durableY.subscribe(id, null, StartAt.now(), ignore()).waitUntilStarted(STARTED_TIMEOUT)).isTrue();
        durableY.pauseSubscription(id);
        Runnable xSubscribes = () -> assertThat(durableX.subscribe(id, ignore()).waitUntilStarted(STARTED_TIMEOUT)).isTrue();
        wrappedY.missTheId = true;
        wrappedY.whileAskedForTheGlobalCheckpoint = xSubscribes;

        assertThatThrownBy(() -> durableY.subscribe(id, ignore())).isInstanceOf(DuplicateSubscriptionIdException.class);

        wrappedY.missTheId = false;
        if (wrappedY.whileAskedForTheGlobalCheckpoint != null) {
            wrappedY.whileAskedForTheGlobalCheckpoint = null;
            xSubscribes.run();
        }
        assertThat(durableX.isRunning(id)).as("precondition: the subscription of node X runs").isTrue();
        assertThat(new NativeMongoCheckpointStorage(checkpoints).read(id))
                .as("the start position of the subscription node X runs, after node Y's subscribe was refused")
                .isNotNull();

        durableX.shutdown();
        eventStore.write("stream", 0L, List.of(event("e1")));
        DurableSubscriptionModel restartedX = durable(nativeModel());
        List<String> received = new CopyOnWriteArrayList<>();
        assertThat(restartedX.subscribe(id, e -> received.add(e.getId())).waitUntilStarted(STARTED_TIMEOUT)).isTrue();
        eventStore.write("stream", 1L, List.of(event("e2")));

        awaitReceived(received, "e2");
        assertThat(received)
                .as("node X started before e1 was written, so e1, written while it was down, comes first")
                .containsExactly("e1", "e2");
    }

    /**
     * Two durable models over one native model. Each one subscribes the same id, one while the other records its
     * start position, so one of the two is refused by the native model.
     */
    @Test
    void a_subscribe_refused_by_the_wrapped_model_another_durable_model_shares_leaves_the_start_position_of_the_one_that_runs() {
        NativeMongoSubscriptionModel shared = nativeModel();
        HookedNativeModel hooked = new HookedNativeModel(shared);
        DurableSubscriptionModel first = durable(hooked);
        DurableSubscriptionModel second = durable(shared);
        String id = UUID.randomUUID().toString();
        List<RuntimeException> refusals = new CopyOnWriteArrayList<>();
        hooked.whileAskedForTheGlobalCheckpoint = () -> subscribeOrCollectTheRefusal(second, id, refusals);

        subscribeOrCollectTheRefusal(first, id, refusals);

        assertThat(refusals).as("the refused subscribe").singleElement().isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(shared.isRunning(id)).as("precondition: the subscription that was accepted runs").isTrue();
        assertThat(new NativeMongoCheckpointStorage(checkpoints).read(id))
                .as("the start position of the subscription that runs, after the other model's subscribe was refused")
                .isNotNull();
    }

    private static void subscribeOrCollectTheRefusal(DurableSubscriptionModel model, String id, List<RuntimeException> refusals) {
        try {
            assertThat(model.subscribe(id, ignore()).waitUntilStarted(STARTED_TIMEOUT)).isTrue();
        } catch (RuntimeException e) {
            refusals.add(e);
        }
    }

    private NativeMongoSubscriptionModel nativeModel() {
        ExecutorService executor = Executors.newCachedThreadPool();
        executors.add(executor);
        return new NativeMongoSubscriptionModel(database, events, TimeRepresentation.RFC_3339_STRING, executor, retryStrategy());
    }

    private DurableSubscriptionModel durable(CheckpointAwareSubscriptionModel wrapped) {
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new NativeMongoCheckpointStorage(checkpoints, retryStrategy()));
        models.add(durable);
        return durable;
    }

    private static RetryStrategy retryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500), 2.0f);
    }

    private static Consumer<CloudEvent> ignore() {
        return cloudEvent -> {
        };
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
     * Forwards to a native model. It answers {@code false} from {@code isRunning(id)} and {@code isPaused(id)}, and
     * omits the id from {@code subscriptionIds()}, while {@code missTheId} is set, and runs
     * {@code whileAskedForTheGlobalCheckpoint} once, when it is next asked for the global checkpoint, and then answers
     * what the native model answered before it ran.
     */
    private static final class HookedNativeModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions, IntrospectableSubscriptions {
        private final NativeMongoSubscriptionModel delegate;
        volatile boolean missTheId;
        volatile @Nullable Runnable whileAskedForTheGlobalCheckpoint;

        HookedNativeModel(NativeMongoSubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Set<String> subscriptionIds() {
            return missTheId ? Set.of() : delegate.subscriptionIds();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            Checkpoint checkpoint = delegate.globalCheckpoint();
            Runnable hook = whileAskedForTheGlobalCheckpoint;
            whileAskedForTheGlobalCheckpoint = null;
            if (hook != null) {
                hook.run();
            }
            return checkpoint;
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
