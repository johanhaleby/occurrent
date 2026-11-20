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
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An {@code ifAbsent()} write stores no write version, and {@code notOlderThan(v)} accepts any write while none is
 * stored. So a first position this node recorded is saved again with {@code notOlderThan(v)} when a
 * {@link org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource} answers {@code v}, otherwise a node whose
 * lease has moved on could overwrite it. Over {@link InMemoryCheckpointStorage}, which evaluates write conditions for
 * real, so the stored version shows what the model stamped.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionWriteVersionTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final String FIRST_POSITION = "first-position";

    @Test
    void a_first_position_recorded_with_a_version_source_answering_a_version_stores_that_version() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(), storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(storage.writeVersion(SUBSCRIPTION_ID)).hasValue(2L);
    }

    @Test
    void a_node_with_an_older_version_cannot_overwrite_the_first_position_this_node_recorded() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(), storage, id -> OptionalLong.of(2));
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThatThrownBy(() -> storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)))
                .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);

        assertThat(storage.read(SUBSCRIPTION_ID).asString()).isEqualTo(FIRST_POSITION);
    }

    @Test
    void a_first_position_adopted_from_another_node_is_neither_overwritten_nor_stamped_with_this_nodes_version() {
        RecordingStorage storage = new RecordingStorage();
        storage.beforeTheIfAbsentWrite = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("other-nodes-position"));
        storage.answersTheWinnerOfALostRace = true;
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("other-nodes-position"),
                () -> assertThat(storage.conditionsSaved).as("conditions saved with").noneMatch(CheckpointWriteCondition.NotOlderThan.class::isInstance),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly("other-nodes-position")
        );
    }

    @Test
    void a_lost_race_that_resolves_to_this_nodes_own_position_stores_this_nodes_version() {
        RecordingStorage storage = new RecordingStorage();
        storage.beforeTheIfAbsentWrite = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("later-position-of-another-node"));
        storage.replacesTheWinnerWithTheCandidate = true;
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(FIRST_POSITION),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION),
                () -> assertThatThrownBy(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)))
                        .as("a node with an older version writing over it")
                        .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class)
        );
    }

    @Test
    void a_lost_race_whose_stored_position_read_back_is_the_one_this_node_read_stores_this_nodes_version() {
        RecordingStorage storage = new RecordingStorage();
        storage.beforeTheIfAbsentWrite = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint(FIRST_POSITION));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(FIRST_POSITION),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION),
                () -> assertThatThrownBy(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)))
                        .as("a node with an older version writing over it")
                        .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class)
        );
    }

    @Test
    void a_stamp_refused_because_a_newer_lease_holder_wrote_since_still_starts_the_subscription_from_the_recorded_position() {
        RecordingStorage storage = new RecordingStorage();
        storage.afterTheIfAbsentWrite = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.conditionsSaved).as("conditions saved with")
                        .containsExactly(CheckpointWriteCondition.ifAbsent(), CheckpointWriteCondition.notOlderThan(2)),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(3L),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("newer-nodes-position")
        );
    }

    @Test
    void a_version_source_that_throws_records_the_first_position_without_a_version_and_does_not_refuse_the_subscription() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> {
            throw new IllegalStateException("the lease cannot be read");
        });

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(FIRST_POSITION),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION)
        );
    }

    @Test
    void no_version_source_saves_the_first_position_once_with_if_absent() {
        RecordingStorage storage = new RecordingStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(), storage);

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(storage.conditionsSaved).singleElement().isInstanceOf(CheckpointWriteCondition.IfAbsent.class);
    }

    /**
     * Delegates to a real {@link InMemoryCheckpointStorage} and records the condition of every save, with hooks that
     * run around the {@code ifAbsent()} write to stand in for another node writing at that moment.
     */
    private static final class RecordingStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        final List<CheckpointWriteCondition> conditionsSaved = new ArrayList<>();
        @Nullable Runnable beforeTheIfAbsentWrite;
        @Nullable Runnable afterTheIfAbsentWrite;
        boolean answersTheWinnerOfALostRace = false;
        boolean replacesTheWinnerWithTheCandidate = false;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            conditionsSaved.add(condition);
            boolean ifAbsent = condition instanceof CheckpointWriteCondition.IfAbsent;
            if (ifAbsent && beforeTheIfAbsentWrite != null) {
                beforeTheIfAbsentWrite.run();
            }
            Checkpoint saved = delegate.save(subscriptionId, checkpoint, condition);
            if (ifAbsent && afterTheIfAbsentWrite != null) {
                afterTheIfAbsentWrite.run();
            }
            return saved;
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
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            if (replacesTheWinnerWithTheCandidate) {
                return Optional.of(delegate.save(subscriptionId, candidate));
            }
            return answersTheWinnerOfALostRace ? Optional.ofNullable(delegate.read(subscriptionId)) : Optional.empty();
        }
    }

    /**
     * A feed whose model default is {@link #FIRST_POSITION}, and which evaluates a dynamic start position on subscribe,
     * recording the checkpoint it starts from.
     */
    private static final class Feed implements CheckpointAwareSubscriptionModel {
        final List<String> positionsStartedFrom = new ArrayList<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startAt.isDynamic()) {
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
            return new StringBasedCheckpoint(FIRST_POSITION);
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
