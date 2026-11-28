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
import org.occurrent.subscription.DuplicateSubscriptionIdException;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscribe that starts from a checkpoint already stored asks the storage once to store this node's write version
 * with it, through {@code storeWriteVersion}. The storage leaves the checkpoint as it is and writes nothing when none is
 * stored, so a checkpoint another node deleted or saved in the meantime is neither stored again nor replaced.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStoreWriteVersionTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Checkpoint STORED = new StringBasedCheckpoint("stored-checkpoint");

    @Test
    void a_checkpoint_deleted_between_the_read_and_the_version_write_is_not_stored_again_by_the_subscribe() {
        InterceptingStorage storage = new InterceptingStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        storage.beforeEachVersionWrite = () -> storage.delegate.delete(SUBSCRIPTION_ID);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(), storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.versionWrites).as("calls that write a version").hasValue(1),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("stored checkpoint").isNull(),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty()
        );
    }

    @Test
    void a_stale_node_saving_a_checkpoint_before_every_version_write_does_not_make_the_subscribe_write_the_version_again() {
        InterceptingStorage storage = new InterceptingStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        AtomicInteger staleSaves = new AtomicInteger();
        storage.beforeEachVersionWrite = () -> {
            if (staleSaves.get() < 5) {
                storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-nodes-save-" + staleSaves.incrementAndGet()));
            }
        };
        DurableSubscriptionModel durable = new DurableSubscriptionModel(new Feed(), storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertAll(
                () -> assertThat(storage.versionWrites).as("calls that write a version").hasValue(1),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo("stale-nodes-save-1"),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    @Test
    void an_evaluation_of_a_subscription_the_wrapped_model_may_still_hold_after_a_later_subscribe_of_its_id_is_refused_starts_from_the_stored_checkpoint_and_writes_nothing() {
        InterceptingStorage storage = new InterceptingStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        AtomicLong version = new AtomicLong(2);
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(version.get()));
        feed.subscribeFailure = new IllegalStateException("expected");
        feed.evaluatesBeforeFailing = true;
        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).as("the first subscribe, which the wrapped model may still hold").isSameAs(feed.subscribeFailure);
        StartAt held = feed.startAts.getFirst();
        feed.evaluatesBeforeFailing = false;
        feed.subscribeFailure = new DuplicateSubscriptionIdException(SUBSCRIPTION_ID);
        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).as("the later subscribe").isSameAs(feed.subscribeFailure);
        version.set(5);
        OptionalLong versionBefore = storage.delegate.writeVersion(SUBSCRIPTION_ID);

        StartAt evaluated = held.get(new SubscriptionModelContext(Feed.class));

        assertAll(
                () -> assertThat(evaluated).as("start position evaluated").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo(STORED.asString())),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(STORED.asString()),
                () -> assertThat(versionBefore).as("test setup, the two subscribes stored this node's version").hasValue(2L),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
        );
    }

    /**
     * Over {@link InMemoryCheckpointStorage}, runs a hook right before every call that writes a version, the
     * {@code storeWriteVersion} and the three-argument {@code resolveFirstCheckpointRace}, and counts those calls.
     */
    private static final class InterceptingStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        final AtomicInteger versionWrites = new AtomicInteger();
        volatile @Nullable Runnable beforeEachVersionWrite;

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

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            beforeAVersionWrite();
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }

        @Override
        public Optional<Checkpoint> storeWriteVersion(String subscriptionId, long writeVersion) {
            beforeAVersionWrite();
            return delegate.storeWriteVersion(subscriptionId, writeVersion);
        }

        private void beforeAVersionWrite() {
            versionWrites.incrementAndGet();
            Runnable before = beforeEachVersionWrite;
            if (before != null) {
                before.run();
            }
        }
    }

    /**
     * Keeps every start position it is given and never evaluates one, so every evaluation is the test's own. Throws
     * {@code subscribeFailure} from a subscribe once it is set, after evaluating the start position first when
     * {@code evaluatesBeforeFailing}, as a model that fails once it has opened its stream does.
     */
    private static final class Feed implements CheckpointAwareSubscriptionModel {
        final List<StartAt> startAts = new ArrayList<>();
        volatile @Nullable RuntimeException subscribeFailure;
        volatile boolean evaluatesBeforeFailing;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            startAts.add(startAt);
            RuntimeException failure = subscribeFailure;
            if (failure != null) {
                if (evaluatesBeforeFailing) {
                    startAt.get(new SubscriptionModelContext(Feed.class));
                }
                throw failure;
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
