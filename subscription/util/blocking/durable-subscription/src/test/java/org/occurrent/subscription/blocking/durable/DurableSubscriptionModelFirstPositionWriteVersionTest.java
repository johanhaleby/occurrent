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
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * With a write version, the model asks the three-argument {@code resolveFirstCheckpointRace} to settle the first
 * position and store the version in one write. A storage that answers that empty gets the {@code ifAbsent()} write
 * and no version, and nothing is saved a second time, since a second save would write over, or move later, whatever
 * another node stored in between. Over {@link InMemoryCheckpointStorage}, which evaluates write conditions for real,
 * so the stored version shows what the model wrote.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionWriteVersionTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final String FIRST_POSITION = "first-position";
    private static final String EARLIER_FIRST_POSITION = "earlier-first-position";

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
    void a_lost_race_that_the_two_argument_resolver_settles_on_this_nodes_position_keeps_that_position_and_stores_no_version() {
        RecordingStorage storage = new RecordingStorageWithAThreeArgumentResolver();
        storage.beforeTheIfAbsentWrite = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("later-position-of-another-node"));
        storage.replacesTheWinnerWithTheCandidate = true;
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(FIRST_POSITION),
                () -> assertThat(storage.conditionsSaved).as("conditions saved with").containsExactly(CheckpointWriteCondition.ifAbsent()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION)
        );
    }

    @Test
    void a_lost_race_whose_stored_position_read_back_is_the_one_this_node_read_starts_from_it_and_stores_no_version() {
        RecordingStorage storage = new RecordingStorageWithAThreeArgumentResolver();
        storage.beforeTheIfAbsentWrite = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint(FIRST_POSITION));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(FIRST_POSITION),
                () -> assertThat(storage.conditionsSaved).as("conditions saved with").containsExactly(CheckpointWriteCondition.ifAbsent()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION)
        );
    }

    @Test
    void an_earlier_first_position_another_node_resolves_after_this_nodes_if_absent_write_is_not_moved_later_by_a_second_save() {
        RecordingStorage storage = new RecordingStorageWithAThreeArgumentResolver();
        storage.replacesTheWinnerWithTheCandidate = true;
        storage.afterTheIfAbsentWrite = () -> storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, new StringBasedCheckpoint(EARLIER_FIRST_POSITION));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(), storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(EARLIER_FIRST_POSITION),
                () -> assertThat(storage.conditionsSaved).as("conditions saved with").containsExactly(CheckpointWriteCondition.ifAbsent()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty()
        );
    }

    @Test
    void a_newer_version_stored_just_before_the_three_argument_resolver_writes_writes_nothing_and_starts_from_the_position_stored() {
        StorageWithAHookInTheThreeArgumentResolver storage = new StorageWithAHookInTheThreeArgumentResolver();
        storage.beforeTheThreeArgumentResolver = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(storage.conditionsSaved).as("conditions saved with").isEmpty(),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("newer-nodes-position"),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(3L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly("newer-nodes-position")
        );
    }

    @Test
    void a_different_first_position_another_node_stored_with_an_older_version_just_before_the_three_argument_resolver_refuses_the_subscribe_and_is_left_as_it_was() {
        StorageWithAHookInTheThreeArgumentResolver storage = new StorageWithAHookInTheThreeArgumentResolver();
        storage.beforeTheThreeArgumentResolver = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-nodes-position"), CheckpointWriteCondition.notOlderThan(1));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, e -> {
                    assertThat(e.positionRead.asString()).as("position read").isEqualTo(FIRST_POSITION);
                    assertThat(e.positionStored.map(Checkpoint::asString)).as("position stored").hasValue("stale-nodes-position");
                }),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("stale-nodes-position"),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(1L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void a_different_first_position_another_node_stored_with_no_version_just_before_the_three_argument_resolver_refuses_the_subscribe_and_is_left_as_it_was() {
        StorageWithAHookInTheThreeArgumentResolver storage = new StorageWithAHookInTheThreeArgumentResolver();
        storage.beforeTheThreeArgumentResolver = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("other-nodes-position"));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class,
                        e -> assertThat(e.positionStored.map(Checkpoint::asString)).as("position stored").hasValue("other-nodes-position")),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo("other-nodes-position"),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void the_very_position_this_node_read_stored_by_another_node_with_an_older_version_just_before_the_three_argument_resolver_is_started_from_and_gets_this_nodes_version() {
        StorageWithAHookInTheThreeArgumentResolver storage = new StorageWithAHookInTheThreeArgumentResolver();
        storage.beforeTheThreeArgumentResolver = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint(FIRST_POSITION), CheckpointWriteCondition.notOlderThan(1));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(FIRST_POSITION),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION)
        );
    }

    @Test
    void a_three_argument_resolver_that_fails_with_anything_but_a_refused_write_condition_fails_the_recording_and_writes_nothing() {
        StorageWithAHookInTheThreeArgumentResolver storage = new StorageWithAHookInTheThreeArgumentResolver();
        storage.beforeTheThreeArgumentResolver = () -> {
            throw new IllegalStateException("the storage is unavailable");
        };
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        assertAll(
                () -> assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
                })).as("subscribe").isInstanceOf(IllegalStateException.class).hasMessage("the storage is unavailable"),
                () -> assertThat(storage.conditionsSaved).as("conditions saved with").isEmpty(),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("stored position").isNull(),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void a_storage_that_overrides_only_the_two_argument_resolver_keeps_the_earlier_first_position_another_node_stored_between_the_two_writes() {
        RecordingStorage storage = new RecordingStorage();
        storage.replacesTheWinnerWithTheCandidate = true;
        storage.afterTheIfAbsentWrite = () -> storage.resolveFirstCheckpointRace(SUBSCRIPTION_ID, new StringBasedCheckpoint(EARLIER_FIRST_POSITION));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(EARLIER_FIRST_POSITION),
                () -> assertThat(storage.conditionsSaved).as("conditions saved with").containsExactly(CheckpointWriteCondition.ifAbsent()),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION)
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
    private static class RecordingStorage implements CheckpointStorage {
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
     * Overrides the three-argument resolver too, answering that it settled nothing, so the model goes on to the
     * {@code ifAbsent()} write.
     */
    private static final class RecordingStorageWithAThreeArgumentResolver extends RecordingStorage {
        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            return Optional.empty();
        }
    }

    /**
     * Delegates the three-argument resolver to the real {@link InMemoryCheckpointStorage} after a hook has run, which
     * stands in for another node writing just before the resolver does.
     */
    private static final class StorageWithAHookInTheThreeArgumentResolver extends RecordingStorage {
        @Nullable Runnable beforeTheThreeArgumentResolver;

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            if (beforeTheThreeArgumentResolver != null) {
                beforeTheThreeArgumentResolver.run();
            }
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
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
