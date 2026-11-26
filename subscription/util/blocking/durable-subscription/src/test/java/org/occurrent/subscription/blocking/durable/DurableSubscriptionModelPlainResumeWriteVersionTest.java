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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A plain resume of a paused subscription to a checkpoint another node stored while it was paused stores this node's
 * write version with that checkpoint before the wrapped model resumes there, so a node that has lost its lease can't
 * store the checkpoint of a later event over it. Over {@link InMemoryCheckpointStorage}, which evaluates write
 * conditions for real.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelPlainResumeWriteVersionTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint STORED_BEFORE_THE_OWN_START = new StringBasedCheckpoint("stored-before-the-own-start");
    private static final Checkpoint STORED_BY_ANOTHER_NODE = new StringBasedCheckpoint("stored-by-another-node");
    private static final Checkpoint STORED_BETWEEN_THE_READ_AND_THE_STAMP = new StringBasedCheckpoint("stored-between-the-read-and-the-stamp");

    private final RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
    private final HookedStorage storage = new HookedStorage();

    @Test
    void a_plain_resume_to_a_checkpoint_another_node_stored_with_an_older_version_stores_this_nodes_version_with_it_before_the_wrapped_model_resumes_there() {
        DurableSubscriptionModel durable = pausedAfterSubscribingAtStart();
        anotherNodeStoresWhilePaused(STORED_BY_ANOTHER_NODE, OptionalLong.of(1));

        durable.resumeSubscription(SUBSCRIPTION_ID);

        Throwable thrown = catchThrowable(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)));
        assertAll(
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED_BY_ANOTHER_NODE.asString()),
                () -> assertThat(thrown).as("save with an older version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BY_ANOTHER_NODE),
                () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("subscription paused").isFalse()
        );
    }

    @Test
    void a_plain_resume_to_a_checkpoint_another_node_stored_with_no_version_stores_this_nodes_version_with_it() {
        DurableSubscriptionModel durable = pausedAfterSubscribingAtStart();
        anotherNodeStoresWhilePaused(STORED_BY_ANOTHER_NODE, OptionalLong.empty());

        durable.resumeSubscription(SUBSCRIPTION_ID);

        Throwable thrown = catchThrowable(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)));
        assertAll(
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(thrown).as("save with an older version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BY_ANOTHER_NODE)
        );
    }

    @Test
    void a_newer_version_stored_refuses_a_plain_resume_that_leaves_the_subscription_paused_and_writes_nothing() {
        DurableSubscriptionModel durable = pausedAfterSubscribingAtStart();
        anotherNodeStoresWhilePaused(STORED_BY_ANOTHER_NODE, OptionalLong.of(3));
        int savesBefore = storage.conditionsSaved.size();

        Throwable thrown = catchThrowable(() -> durable.resumeSubscription(SUBSCRIPTION_ID));

        assertAll(
                () -> assertThat(thrown).as("thrown by resumeSubscription").isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class,
                        e -> assertThat(e.storedVersion).as("stored version reported").hasValue(3L)),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty(),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(0),
                () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("subscription paused").isTrue(),
                () -> assertThat(storage.conditionsSaved).as("saves after the resume").hasSize(savesBefore),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED_BY_ANOTHER_NODE.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(3L)
        );
    }

    @Test
    void another_checkpoint_stored_between_the_read_and_the_stamp_is_read_again_and_the_wrapped_model_resumes_there_with_this_nodes_version() {
        DurableSubscriptionModel durable = pausedAfterSubscribingAtStart();
        anotherNodeStoresWhilePaused(STORED_BY_ANOTHER_NODE, OptionalLong.empty());
        storage.beforeTheFirstThreeArgumentResolver = () -> storage.delegate.save(SUBSCRIPTION_ID, STORED_BETWEEN_THE_READ_AND_THE_STAMP);

        durable.resumeSubscription(SUBSCRIPTION_ID);

        Throwable thrown = catchThrowable(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)));
        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED_BETWEEN_THE_READ_AND_THE_STAMP.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(thrown).as("save with an older version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BETWEEN_THE_READ_AND_THE_STAMP)
        );
    }

    @Test
    void a_storage_that_answers_empty_for_the_very_checkpoint_it_holds_resumes_there_without_a_version() {
        StorageWithoutResolvers withoutResolvers = new StorageWithoutResolvers();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, withoutResolvers, id -> OptionalLong.of(2));
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        durable.pauseSubscription(SUBSCRIPTION_ID);
        withoutResolvers.delegate.delete(SUBSCRIPTION_ID);
        withoutResolvers.delegate.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE);

        Throwable thrown = catchThrowable(() -> durable.resumeSubscription(SUBSCRIPTION_ID));

        assertAll(
                () -> assertThat(thrown).as("thrown by resumeSubscription").isNull(),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BY_ANOTHER_NODE),
                () -> assertThat(withoutResolvers.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED_BY_ANOTHER_NODE.asString()),
                () -> assertThat(withoutResolvers.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty()
        );
    }

    @Test
    void a_plain_resume_when_the_stored_checkpoint_is_still_the_one_stored_before_the_subscriptions_own_start_stores_this_nodes_version_with_it_and_leaves_the_resume_to_the_wrapped_model() {
        storage.delegate.save(SUBSCRIPTION_ID, STORED_BEFORE_THE_OWN_START);
        DurableSubscriptionModel durable = pausedAfterSubscribingAtStart();
        int resolversBefore = storage.threeArgumentResolverCalls.get();
        int savesBefore = storage.conditionsSaved.size();

        durable.resumeSubscription(SUBSCRIPTION_ID);

        assertAll(
                () -> assertThat(storage.threeArgumentResolverCalls).as("calls of the three-argument resolver by the resume").hasValue(resolversBefore + 1),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(storage.conditionsSaved).as("saves by the resume").hasSize(savesBefore),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED_BEFORE_THE_OWN_START.asString()),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(1),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty()
        );
    }

    @Test
    void a_save_by_the_node_that_stored_the_checkpoint_before_the_subscriptions_own_start_with_an_older_version_is_refused_after_a_plain_resume_that_leaves_the_resume_to_the_wrapped_model() {
        storage.delegate.save(SUBSCRIPTION_ID, STORED_BEFORE_THE_OWN_START, CheckpointWriteCondition.notOlderThan(1));
        DurableSubscriptionModel durable = pausedAfterSubscribingAtStart();

        durable.resumeSubscription(SUBSCRIPTION_ID);

        Throwable thrown = catchThrowable(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)));
        assertAll(
                () -> assertThat(thrown).as("save with the older version").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED_BEFORE_THE_OWN_START.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(1),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty()
        );
    }

    @Test
    void a_second_plain_resume_after_a_pause_still_leaves_the_resume_to_the_wrapped_model_when_the_first_stored_this_nodes_version_with_the_checkpoint_stored_before_the_subscriptions_own_start() {
        storage.delegate.save(SUBSCRIPTION_ID, STORED_BEFORE_THE_OWN_START, CheckpointWriteCondition.notOlderThan(1));
        DurableSubscriptionModel durable = pausedAfterSubscribingAtStart();
        durable.resumeSubscription(SUBSCRIPTION_ID);
        durable.pauseSubscription(SUBSCRIPTION_ID);

        durable.resumeSubscription(SUBSCRIPTION_ID);

        assertAll(
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(2),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").isEmpty(),
                () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("subscription paused").isFalse(),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED_BEFORE_THE_OWN_START.asString()),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    private DurableSubscriptionModel pausedAfterSubscribingAtStart() {
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(2));
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        durable.pauseSubscription(SUBSCRIPTION_ID);
        assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("subscription paused").isTrue();
        return durable;
    }

    // Stored behind the model's back, as another node does, with the version it holds or none. The delete is how the
    // version of this node's subscribe goes, since a save can't lower a stored version
    private void anotherNodeStoresWhilePaused(Checkpoint checkpoint, OptionalLong version) {
        storage.delegate.delete(SUBSCRIPTION_ID);
        if (version.isPresent()) {
            storage.delegate.save(SUBSCRIPTION_ID, checkpoint, CheckpointWriteCondition.notOlderThan(version.getAsLong()));
        } else {
            storage.delegate.save(SUBSCRIPTION_ID, checkpoint);
        }
    }

    /**
     * Over {@link InMemoryCheckpointStorage}, records what is saved through it, counts the calls of the three-argument
     * {@code resolveFirstCheckpointRace} and runs a hook once, right before the first of them.
     */
    private static final class HookedStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        final List<CheckpointWriteCondition> conditionsSaved = new ArrayList<>();
        final AtomicInteger threeArgumentResolverCalls = new AtomicInteger();
        @Nullable Runnable beforeTheFirstThreeArgumentResolver;

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
            threeArgumentResolverCalls.incrementAndGet();
            Runnable before = beforeTheFirstThreeArgumentResolver;
            beforeTheFirstThreeArgumentResolver = null;
            if (before != null) {
                before.run();
            }
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }
    }

    /**
     * Keeps the default {@code resolveFirstCheckpointRace}, which answers empty whatever is stored.
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
}
