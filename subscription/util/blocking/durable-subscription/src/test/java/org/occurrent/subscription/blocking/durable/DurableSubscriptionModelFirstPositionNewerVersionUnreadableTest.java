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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A storage that refuses the first position because it holds a newer write version, and then can't name what it
 * holds, stands for a node whose lease moved on while the checkpoint can't be read. Starting from the position this
 * node read could skip the events up to what the newer node stored, so the start is refused, and nothing is started
 * from or saved.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionNewerVersionUnreadableTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final String POSITION_READ = "position-this-node-read";

    @Test
    void a_newer_version_stored_with_a_checkpoint_that_cannot_be_read_back_refuses_the_subscribe_with_the_failure_as_cause() {
        IllegalStateException unreachable = new IllegalStateException("Checkpoint storage cannot be reached");
        NewerVersionStoredStorage storage = new NewerVersionStoredStorage(unreachable);
        Feed feed = new Feed(false);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, refusal -> {
                    assertThat(refusal.positionRead.asString()).as("position read").isEqualTo(POSITION_READ);
                    assertThat(refusal.positionStored).as("position stored").isEmpty();
                    assertThat(refusal.getCause()).as("cause").isSameAs(unreachable);
                    assertThat(refusal).hasMessageContaining("newer write version for it than 2").hasMessageContaining("failed");
                }),
                () -> assertThat(storage.saved).as("saved").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void a_newer_version_stored_with_a_checkpoint_that_reads_back_as_nothing_refuses_the_subscribe_with_no_cause() {
        NewerVersionStoredStorage storage = new NewerVersionStoredStorage(null);
        Feed feed = new Feed(false);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, refusal -> {
                    assertThat(refusal.positionRead.asString()).as("position read").isEqualTo(POSITION_READ);
                    assertThat(refusal.positionStored).as("position stored").isEmpty();
                    assertThat(refusal.getCause()).as("cause").isNull();
                    assertThat(refusal).hasMessageContaining("newer write version for it than 2").hasMessageContaining("found nothing");
                }),
                () -> assertThat(storage.saved).as("saved").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void an_evaluation_that_records_the_first_position_and_finds_a_newer_version_it_cannot_read_back_is_refused_with_the_failure_as_cause() {
        IllegalStateException unreachable = new IllegalStateException("Checkpoint storage cannot be reached");
        NewerVersionStoredStorage storage = new NewerVersionStoredStorage(unreachable);
        Feed feed = new Feed(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true), id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(feed.startPositionFailures).as("start position failures").singleElement()
                        .isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, refusal -> {
                            assertThat(refusal.positionRead.asString()).as("position read").isEqualTo(POSITION_READ);
                            assertThat(refusal.positionStored).as("position stored").isEmpty();
                            assertThat(refusal.getCause()).as("cause").isSameAs(unreachable);
                            assertThat(refusal).hasMessageContaining("newer write version for it than 2");
                            assertThat(refusal.getMessage()).doesNotContain("stored in between");
                        }),
                () -> assertThat(storage.saved).as("saved").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void an_evaluation_that_records_the_first_position_and_finds_a_newer_version_that_reads_back_as_nothing_is_refused_with_no_cause() {
        NewerVersionStoredStorage storage = new NewerVersionStoredStorage(null);
        Feed feed = new Feed(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true), id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(feed.startPositionFailures).as("start position failures").singleElement()
                        .isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, refusal -> {
                            assertThat(refusal.positionStored).as("position stored").isEmpty();
                            assertThat(refusal.getCause()).as("cause").isNull();
                            assertThat(refusal).hasMessageContaining("newer write version for it than 2").hasMessageContaining("found nothing");
                            assertThat(refusal.getMessage()).doesNotContain("stored in between");
                        }),
                () -> assertThat(storage.saved).as("saved").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    /**
     * Refuses the three-argument resolver because a newer version is stored. Reads find nothing before that, and after it
     * they fail or keep finding nothing.
     */
    private static final class NewerVersionStoredStorage implements CheckpointStorage {
        final List<Checkpoint> saved = new ArrayList<>();
        private final @Nullable RuntimeException readFailure;
        private boolean refusedTheResolver = false;

        NewerVersionStoredStorage(@Nullable RuntimeException readFailure) {
            this.readFailure = readFailure;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (refusedTheResolver && readFailure != null) {
                throw readFailure;
            }
            return null;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            saved.add(checkpoint);
            return checkpoint;
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            refusedTheResolver = true;
            throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(3), CheckpointWriteCondition.notOlderThan(writeVersion));
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
        }

        @Override
        public boolean exists(String subscriptionId) {
            return false;
        }
    }

    /**
     * Answers {@link #POSITION_READ} as its position, and null the first time when {@code firstAnswerIsNull}, so the
     * first position is recorded by the start position evaluation. Evaluates a dynamic start position on subscribe and
     * keeps going when it throws, as a model that evaluates it on a thread of its own does.
     */
    private static final class Feed implements CheckpointAwareSubscriptionModel {
        final List<String> positionsStartedFrom = new ArrayList<>();
        final List<Throwable> startPositionFailures = new ArrayList<>();
        private final boolean firstAnswerIsNull;
        private int globalCheckpointCalls = 0;

        Feed(boolean firstAnswerIsNull) {
            this.firstAnswerIsNull = firstAnswerIsNull;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startAt.isDynamic()) {
                try {
                    if (startAt.get(new SubscriptionModelContext(Feed.class)) instanceof StartAt.StartAtCheckpoint checkpoint) {
                        positionsStartedFrom.add(checkpoint.checkpoint.asString());
                    }
                } catch (RuntimeException e) {
                    if (!firstAnswerIsNull) {
                        throw e;
                    }
                    startPositionFailures.add(e);
                }
            }
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

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            globalCheckpointCalls++;
            return firstAnswerIsNull && globalCheckpointCalls == 1 ? null : new StringBasedCheckpoint(POSITION_READ);
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }
}
