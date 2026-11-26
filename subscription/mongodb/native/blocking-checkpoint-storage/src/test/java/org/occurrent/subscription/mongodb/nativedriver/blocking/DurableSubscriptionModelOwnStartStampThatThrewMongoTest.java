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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscription that replays from a checkpoint of its own, before the checkpoint another node stored, owes the events
 * between its position and the stored one. A plain resume whose write stored this node's write version with that
 * checkpoint in {@link NativeMongoCheckpointStorage} and then threw, as a lost acknowledgement does, is followed by a
 * plain resume that finds the checkpoint with this node's own version. Only this node holds that version, so it goes on
 * from the wrapped model's own position and the replay is delivered. The wrapped model is a replaying fake, since what
 * is under test is the version and checkpoint the real storage keeps.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelOwnStartStampThatThrewMongoTest {

    private static final String DATABASE = "durableownstartstampthatthrew";
    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final List<String> LOG = List.of("p1", "p2", "p3", "p4", "p5", "p6");
    private static final Checkpoint OWN_START = new StringBasedCheckpoint("p0");
    private static final Checkpoint STORED_BY_ANOTHER_NODE = new StringBasedCheckpoint("p4");
    private static final long VERSION_OF_ANOTHER_NODE = 1;

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    @BeforeAll
    static void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Test
    void a_plain_resume_goes_on_from_the_replay_and_delivers_every_event_after_an_earlier_resume_stored_this_nodes_version_with_the_stored_checkpoint_in_mongodb_and_threw() {
        ReplayingModel wrapped = new ReplayingModel();
        NativeMongoCheckpointStorage mongoStorage = new NativeMongoCheckpointStorage(database.getCollection("checkpoints-" + UUID.randomUUID()));
        StampThatThrowsStorage storage = new StampThatThrowsStorage(mongoStorage);
        List<String> delivered = new CopyOnWriteArrayList<>();
        storage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE, CheckpointWriteCondition.notOlderThan(VERSION_OF_ANOTHER_NODE));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(__ -> false), id -> OptionalLong.of(2));
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(OWN_START), cloudEvent -> delivered.add(cloudEvent.getId()));
        wrapped.deliverNext(SUBSCRIPTION_ID);
        wrapped.deliverNext(SUBSCRIPTION_ID);
        durable.pauseSubscription(SUBSCRIPTION_ID);
        storage.throwAfterTheNextStamp = true;

        Throwable firstResume = catchThrowable(() -> durable.resumeSubscription(SUBSCRIPTION_ID));
        Checkpoint storedByTheFirstResume = mongoStorage.read(SUBSCRIPTION_ID);
        OptionalLong versionStoredByTheFirstResume = mongoStorage.writeVersion(SUBSCRIPTION_ID);
        boolean pausedAfterTheFirstResume = wrapped.isPaused(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID);
        wrapped.deliverTheRest(SUBSCRIPTION_ID);

        // A resume that repositions to the stored checkpoint would deliver p5 and p6 only, since the replay up to p4 would be
        // skipped. Going on from the wrapped model's own position can deliver an event twice at most, never skip one
        assertAll(
                () -> assertThat(firstResume).as("thrown by the first resume").isSameAs(storage.failure),
                () -> assertThat(storedByTheFirstResume.asString()).as("checkpoint stored by the first resume").isEqualTo(STORED_BY_ANOTHER_NODE.asString()),
                () -> assertThat(versionStoredByTheFirstResume).as("version stored by the first resume").hasValue(2L),
                () -> assertThat(pausedAfterTheFirstResume).as("subscription paused after the first resume").isTrue(),
                () -> assertThat(delivered).as("events delivered").containsExactly("p1", "p2", "p3", "p4", "p5", "p6"),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty(),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(1),
                () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("subscription paused").isFalse()
        );
    }

    /**
     * Delegates to {@link NativeMongoCheckpointStorage} and throws once after the three-argument
     * {@code resolveFirstCheckpointRace} has stored, as a lost acknowledgement does.
     */
    private static final class StampThatThrowsStorage implements CheckpointStorage {
        final NativeMongoCheckpointStorage delegate;
        final RuntimeException failure = new IllegalStateException("acknowledgement lost");
        volatile boolean throwAfterTheNextStamp;

        StampThatThrowsStorage(NativeMongoCheckpointStorage delegate) {
            this.delegate = delegate;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return delegate.evaluatesWriteConditionsFor(subscriptionId);
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            Optional<Checkpoint> resolved = delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
            if (throwAfterTheNextStamp) {
                throwAfterTheNextStamp = false;
                throw failure;
            }
            return resolved;
        }
    }

    /**
     * Replays {@link #LOG} from the position it is subscribed or resumed at, one event at a time when told to. It
     * restarts at any checkpoint, and records the plain resumes and the resumes at a position it saw.
     */
    private static final class ReplayingModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        final List<Checkpoint> resumedAt = new CopyOnWriteArrayList<>();
        final AtomicInteger plainResumes = new AtomicInteger();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private final ConcurrentHashMap<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicInteger> positions = new ConcurrentHashMap<>();

        void deliverNext(String subscriptionId) {
            int next = positions.get(subscriptionId).getAndIncrement();
            String position = LOG.get(next);
            CloudEvent event = CloudEventBuilder.v1().withId(position).withSource(URI.create("urn:test")).withType("type").build();
            actions.get(subscriptionId).accept(new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint(position)));
        }

        void deliverTheRest(String subscriptionId) {
            while (!paused.contains(subscriptionId) && positions.get(subscriptionId).get() < LOG.size()) {
                deliverNext(subscriptionId);
            }
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            positions.put(subscriptionId, new AtomicInteger(indexAfter(startAt)));
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            paused.remove(subscriptionId);
            plainResumes.incrementAndGet();
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            paused.remove(subscriptionId);
            resumedAt.add(((StartAt.StartAtCheckpoint) startAt).checkpoint);
            positions.get(subscriptionId).set(indexAfter(startAt));
            return subscription(subscriptionId);
        }

        // A start at p0 is before the log, one at pN is after the Nth event
        private static int indexAfter(StartAt startAt) {
            StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(ReplayingModel.class));
            Checkpoint checkpoint = ((StartAt.StartAtCheckpoint) resolved).checkpoint;
            return Integer.parseInt(checkpoint.asString().substring(1));
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            actions.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return actions.containsKey(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("p" + LOG.size());
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        private static Subscription subscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }
}
