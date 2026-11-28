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
 * A storage that refuses the first position because it holds a newer write version stands for a node whose lease moved
 * on, since the node that stored the newer version holds a newer lease. The model lets the storage's
 * {@link CheckpointWriteConditionNotFulfilledException} reach the caller, on subscribe and in a start position
 * evaluation, and writes nothing. It doesn't read the stored checkpoint back, so the outcome doesn't depend on whether
 * that read would have worked.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionNewerVersionRefusedTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final String POSITION_READ = "position-this-node-read";

    @Test
    void a_newer_version_stored_refuses_the_subscribe_with_the_storages_exception_when_the_stored_checkpoint_cannot_be_read() {
        NewerVersionStoredStorage storage = new NewerVersionStoredStorage(new IllegalStateException("Checkpoint storage cannot be reached"));
        Feed feed = new Feed(false);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isSameAs(storage.refusal),
                () -> assertThat(storage.readsAfterTheRefusal).as("reads after the refusal").isZero(),
                () -> assertThat(storage.saved).as("saved").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void a_newer_version_stored_refuses_the_subscribe_with_the_storages_exception_when_the_stored_checkpoint_reads_back_as_nothing() {
        NewerVersionStoredStorage storage = new NewerVersionStoredStorage(null);
        Feed feed = new Feed(false);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isSameAs(storage.refusal),
                () -> assertThat(storage.readsAfterTheRefusal).as("reads after the refusal").isZero(),
                () -> assertThat(storage.saved).as("saved").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void an_evaluation_that_records_the_first_position_and_finds_a_newer_version_throws_the_storages_exception_when_the_stored_checkpoint_cannot_be_read() {
        NewerVersionStoredStorage storage = new NewerVersionStoredStorage(new IllegalStateException("Checkpoint storage cannot be reached"));
        Feed feed = new Feed(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true), id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(feed.startPositionFailures).as("start position failures").singleElement().isSameAs(storage.refusal),
                () -> assertThat(storage.readsAfterTheRefusal).as("reads after the refusal").isZero(),
                () -> assertThat(storage.saved).as("saved").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void an_evaluation_that_records_the_first_position_and_finds_a_newer_version_throws_the_storages_exception_when_the_stored_checkpoint_reads_back_as_nothing() {
        NewerVersionStoredStorage storage = new NewerVersionStoredStorage(null);
        Feed feed = new Feed(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true), id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(feed.startPositionFailures).as("start position failures").singleElement().isSameAs(storage.refusal),
                () -> assertThat(storage.readsAfterTheRefusal).as("reads after the refusal").isZero(),
                () -> assertThat(storage.saved).as("saved").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    /**
     * Refuses the three-argument resolver because a newer version is stored. Reads find nothing before that, and after it
     * they fail or keep finding nothing, and are counted, since the model must not read the stored checkpoint back.
     */
    private static final class NewerVersionStoredStorage implements CheckpointStorage {
        final List<Checkpoint> saved = new ArrayList<>();
        final CheckpointWriteConditionNotFulfilledException refusal =
                new CheckpointWriteConditionNotFulfilledException(SUBSCRIPTION_ID, OptionalLong.of(3), CheckpointWriteCondition.notOlderThan(2));
        int readsAfterTheRefusal = 0;
        private final @Nullable RuntimeException readFailure;
        private boolean refusedTheResolver = false;

        NewerVersionStoredStorage(@Nullable RuntimeException readFailure) {
            this.readFailure = readFailure;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (refusedTheResolver) {
                readsAfterTheRefusal++;
                if (readFailure != null) {
                    throw readFailure;
                }
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
            throw refusal;
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
