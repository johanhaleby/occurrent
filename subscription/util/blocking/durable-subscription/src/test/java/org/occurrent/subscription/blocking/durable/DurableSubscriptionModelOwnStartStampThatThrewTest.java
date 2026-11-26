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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
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
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscription that replays from a checkpoint of its own, before the checkpoint another node stored, owes the events
 * between its position and the stored one. A plain resume that goes on from the wrapped model's own position delivers
 * them. When an earlier resume stored this node's write version with that checkpoint, or the put-back of a failed
 * resume at a position did, and then threw, the later plain resume finds the stored checkpoint with a version that
 * isn't the one it saw at subscribe. Only this node holds that version, so the checkpoint is no change by another
 * holder, and repositioning the wrapped model to it would skip the replay. Over {@link InMemoryCheckpointStorage}
 * and a wrapped model that replays from a position by hand.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelOwnStartStampThatThrewTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final List<String> LOG = List.of("p1", "p2", "p3", "p4", "p5", "p6");
    private static final Checkpoint OWN_START = new StringBasedCheckpoint("p0");
    private static final Checkpoint STORED_BY_ANOTHER_NODE = new StringBasedCheckpoint("p4");
    private static final Checkpoint REPOSITIONED_TO = new StringBasedCheckpoint("p1");
    private static final long VERSION_OF_ANOTHER_NODE = 1;

    private final ReplayingModel wrapped = new ReplayingModel();
    private final StampThatThrowsStorage storage = new StampThatThrowsStorage();
    private final AtomicLong thisNodesVersion = new AtomicLong(2);
    private final List<String> delivered = new CopyOnWriteArrayList<>();

    @Test
    void a_plain_resume_goes_on_from_the_replay_and_delivers_every_event_after_an_earlier_resume_stored_this_nodes_version_with_the_stored_checkpoint_and_threw() {
        DurableSubscriptionModel durable = pausedHalfWayThroughTheReplay();
        storage.throwAfterTheNextStamp = true;

        Throwable firstResume = catchThrowable(() -> durable.resumeSubscription(SUBSCRIPTION_ID));
        Checkpoint storedByTheFirstResume = storage.read(SUBSCRIPTION_ID);
        OptionalLong versionStoredByTheFirstResume = storage.writeVersion(SUBSCRIPTION_ID);
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

    @Test
    void a_plain_resume_goes_on_from_the_replay_and_delivers_every_event_when_this_nodes_version_has_moved_on_since_the_resume_that_stored_the_older_one_and_threw() {
        DurableSubscriptionModel durable = pausedHalfWayThroughTheReplay();
        storage.throwAfterTheNextStamp = true;
        catchThrowable(() -> durable.resumeSubscription(SUBSCRIPTION_ID));
        thisNodesVersion.set(3);

        durable.resumeSubscription(SUBSCRIPTION_ID);
        wrapped.deliverTheRest(SUBSCRIPTION_ID);

        assertAll(
                () -> assertThat(delivered).as("events delivered").containsExactly("p1", "p2", "p3", "p4", "p5", "p6"),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty(),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(1),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void a_plain_resume_goes_on_from_the_replay_when_the_version_stored_is_one_of_several_this_node_has_offered_and_the_latest_never_reached_the_storage() {
        DurableSubscriptionModel durable = pausedHalfWayThroughTheReplay();
        storage.throwAfterTheNextStamp = true;
        catchThrowable(() -> durable.resumeSubscription(SUBSCRIPTION_ID));
        thisNodesVersion.set(3);
        storage.throwBeforeTheNextStamp = true;
        Throwable secondResume = catchThrowable(() -> durable.resumeSubscription(SUBSCRIPTION_ID));
        OptionalLong versionStoredAfterTheSecondResume = storage.writeVersion(SUBSCRIPTION_ID);

        durable.resumeSubscription(SUBSCRIPTION_ID);
        wrapped.deliverTheRest(SUBSCRIPTION_ID);

        assertAll(
                () -> assertThat(secondResume).as("thrown by the second resume").isSameAs(storage.failure),
                () -> assertThat(versionStoredAfterTheSecondResume).as("version stored after the second resume").hasValue(2L),
                () -> assertThat(delivered).as("events delivered").containsExactly("p1", "p2", "p3", "p4", "p5", "p6"),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty(),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(1)
        );
    }

    @Test
    void a_plain_resume_goes_on_from_the_replay_and_delivers_every_event_after_a_resume_at_a_position_that_failed_and_put_the_stored_checkpoint_back_with_this_nodes_version() {
        DurableSubscriptionModel durable = pausedHalfWayThroughTheReplay();
        wrapped.failNextResumeAtPosition = true;

        Throwable resumeAtPosition = catchThrowable(() -> RepositionableSubscriptions.findIn(durable).orElseThrow()
                .resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(REPOSITIONED_TO)));
        Checkpoint storedAfterThePutBack = storage.read(SUBSCRIPTION_ID);
        OptionalLong versionStoredAfterThePutBack = storage.writeVersion(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID);
        wrapped.deliverTheRest(SUBSCRIPTION_ID);

        // A failed resume at a position leaves the stored position as it was, so the replay it interrupted is still owed
        assertAll(
                () -> assertThat(resumeAtPosition).as("thrown by the resume at a position").isSameAs(wrapped.resumeFailure),
                () -> assertThat(storedAfterThePutBack.asString()).as("checkpoint stored after the put-back").isEqualTo(STORED_BY_ANOTHER_NODE.asString()),
                () -> assertThat(versionStoredAfterThePutBack).as("version stored after the put-back").hasValue(2L),
                () -> assertThat(delivered).as("events delivered").containsExactly("p1", "p2", "p3", "p4", "p5", "p6"),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty(),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(1)
        );
    }

    // Another node has stored p4 with its version while this node replays from p0, and p1 and p2 have been delivered
    private DurableSubscriptionModel pausedHalfWayThroughTheReplay() {
        storage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE, CheckpointWriteCondition.notOlderThan(VERSION_OF_ANOTHER_NODE));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(__ -> false), id -> OptionalLong.of(thisNodesVersion.get()));
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(OWN_START), cloudEvent -> delivered.add(cloudEvent.getId()));
        wrapped.deliverNext(SUBSCRIPTION_ID);
        wrapped.deliverNext(SUBSCRIPTION_ID);
        durable.pauseSubscription(SUBSCRIPTION_ID);
        assertAll(
                () -> assertThat(delivered).as("test setup, events delivered before the pause").containsExactly("p1", "p2"),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("test setup, stored checkpoint").isEqualTo(STORED_BY_ANOTHER_NODE.asString()),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("test setup, stored version").hasValue(VERSION_OF_ANOTHER_NODE),
                () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("test setup, subscription paused").isTrue()
        );
        return durable;
    }

    /**
     * Fails the three-argument {@code resolveFirstCheckpointRace} once, after it stored, as a lost acknowledgement does,
     * or before it stored.
     */
    private static final class StampThatThrowsStorage extends InMemoryCheckpointStorage {
        final RuntimeException failure = new IllegalStateException("acknowledgement lost");
        volatile boolean throwAfterTheNextStamp;
        volatile boolean throwBeforeTheNextStamp;

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            if (throwBeforeTheNextStamp) {
                throwBeforeTheNextStamp = false;
                throw failure;
            }
            Optional<Checkpoint> resolved = super.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
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
        final RuntimeException resumeFailure = new IllegalStateException("resume failed");
        volatile boolean failNextResumeAtPosition;
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
            if (failNextResumeAtPosition) {
                failNextResumeAtPosition = false;
                throw resumeFailure;
            }
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
