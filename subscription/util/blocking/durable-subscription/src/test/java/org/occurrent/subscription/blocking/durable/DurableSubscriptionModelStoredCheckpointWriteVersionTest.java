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
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscribe that finds a checkpoint stored, such as the start boundary the Spring Boot starter stores, stores this
 * node's write version with it before it starts from it, so a node that has lost its lease can't store the checkpoint
 * of a later event over it. Over {@link InMemoryCheckpointStorage}, which evaluates write conditions for real.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStoredCheckpointWriteVersionTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Checkpoint STORED = new StringBasedCheckpoint("stored-checkpoint");
    private static final Checkpoint OTHER = new StringBasedCheckpoint("other-nodes-checkpoint");

    @Test
    void a_subscribe_from_a_stored_checkpoint_stores_this_nodes_version_with_it_so_a_save_with_an_older_version_is_refused() {
        HookedStorage storage = new HookedStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(false), storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        Throwable thrown = catchThrowable(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)));
        assertAll(
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(thrown).as("save with an older version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED.asString())
        );
    }

    @Test
    void a_newer_version_stored_with_the_checkpoint_refuses_the_subscribe_on_the_subscribing_thread_and_nothing_is_written() {
        HookedStorage storage = new HookedStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED, CheckpointWriteCondition.notOlderThan(3));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(false), storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class,
                        e -> assertThat(e.storedVersion).as("stored version reported").hasValue(3L)),
                () -> assertThat(storage.conditionsSaved).as("conditions saved with").isEmpty(),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void another_checkpoint_stored_between_the_read_and_the_stamp_is_kept_and_gets_this_nodes_version() {
        HookedStorage storage = new HookedStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        storage.beforeTheFirstWriteVersionStore = () -> storage.delegate.save(SUBSCRIPTION_ID, OTHER);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(false), storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        Throwable thrown = catchThrowable(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)));
        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(OTHER.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(thrown).as("save with an older version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class)
        );
    }

    @Test
    void a_storage_that_answers_empty_for_the_very_checkpoint_it_holds_is_started_from_without_a_version() {
        StorageWithoutResolvers storage = new StorageWithoutResolvers();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        Feed feed = new Feed(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isNotEmpty().containsOnly(STORED.asString()),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty()
        );
    }

    @Test
    void a_version_source_that_throws_starts_from_the_stored_checkpoint_without_a_version() {
        HookedStorage storage = new HookedStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        AtomicInteger asked = new AtomicInteger();
        Feed feed = new Feed(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> {
            asked.incrementAndGet();
            throw new IllegalStateException("expected");
        });

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(asked).as("asks of the version source").hasValueGreaterThan(0),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isNotEmpty().containsOnly(STORED.asString()),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty()
        );
    }

    @Test
    void the_evaluation_of_the_start_position_stores_this_nodes_version_with_the_checkpoint_it_reads_as_well() {
        HookedStorage storage = new HookedStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        // A checkpoint stored with no version, after the subscribe settled and before the evaluation reads
        storage.afterTheFirstWriteVersionStore = () -> {
            storage.delegate.delete(SUBSCRIPTION_ID);
            storage.delegate.save(SUBSCRIPTION_ID, OTHER);
        };
        Feed feed = new Feed(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        Throwable thrown = catchThrowable(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)));
        assertAll(
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isNotEmpty().containsOnly(OTHER.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(thrown).as("save with an older version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class)
        );
    }

    /**
     * Over {@link InMemoryCheckpointStorage}, records what is saved through it and runs a hook once, right before or
     * right after the first call of {@code storeWriteVersion}.
     */
    private static class HookedStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        final List<CheckpointWriteCondition> conditionsSaved = new ArrayList<>();
        @Nullable Runnable beforeTheFirstWriteVersionStore;
        @Nullable Runnable afterTheFirstWriteVersionStore;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            conditionsSaved.add(condition);
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
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
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }

        @Override
        public Optional<Checkpoint> storeWriteVersion(String subscriptionId, long writeVersion) {
            Runnable before = beforeTheFirstWriteVersionStore;
            beforeTheFirstWriteVersionStore = null;
            if (before != null) {
                before.run();
            }
            Optional<Checkpoint> answer;
            try {
                answer = delegate.storeWriteVersion(subscriptionId, writeVersion);
            } finally {
                Runnable after = afterTheFirstWriteVersionStore;
                afterTheFirstWriteVersionStore = null;
                if (after != null) {
                    after.run();
                }
            }
            return answer;
        }
    }

    /**
     * Keeps the default {@code storeWriteVersion} and {@code resolveFirstCheckpointRace}, which answer empty whatever is stored.
     */
    private static final class StorageWithoutResolvers implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();

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
    }

    /**
     * Evaluates the dynamic start position before {@code subscribe} returns when {@code evaluatesOnSubscribe}, as a
     * model that evaluates it on a thread of its own does while the subscribe is still running. Otherwise it never
     * evaluates it, so only what the subscribe itself does with the stored checkpoint shows.
     */
    private static final class Feed implements CheckpointAwareSubscriptionModel {
        final List<String> positionsStartedFrom = new ArrayList<>();
        private final boolean evaluatesOnSubscribe;

        Feed(boolean evaluatesOnSubscribe) {
            this.evaluatesOnSubscribe = evaluatesOnSubscribe;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (evaluatesOnSubscribe && startAt.isDynamic()) {
                StartAt resolved = startAt.get(new SubscriptionModelContext(Feed.class));
                if (resolved instanceof StartAt.StartAtCheckpoint checkpoint) {
                    positionsStartedFrom.add(checkpoint.checkpoint.asString());
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
            return new StringBasedCheckpoint("global-checkpoint");
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
