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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Issue #1253. A resume at a position found through {@link RepositionableSubscriptions#findIn} on a
 * {@link DurableSubscriptionModel} stores that position before the wrapped model resumes there, so a restart before
 * the next event starts from it and not from the checkpoint stored before. Written against the lookup only, so the
 * same file compiles on a build where the durable model doesn't resume at a position itself.
 */
@DisplayName("DurableSubscriptionModelResumeAtPositionStores")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelResumeAtPositionStoresTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint OLDER = new StringBasedCheckpoint("older");
    private static final Checkpoint NEWER = new StringBasedCheckpoint("newer");
    private static final Checkpoint GLOBAL = new StringBasedCheckpoint("global");

    @Nested
    @DisplayName("when the subscription is checkpointed and paused")
    class When_the_subscription_is_checkpointed_and_paused {

        @Test
        void a_resume_at_a_checkpoint_stores_that_checkpoint() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(NEWER));

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(NEWER);
        }

        @Test
        void a_resume_at_a_checkpoint_resumes_the_wrapped_model_there() {
            // Given
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storageHolding(OLDER));
            subscribeAndPause(model);

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(NEWER));

            // Then
            assertAll(
                    () -> assertThat(wrapped.checkpointsResumedAt).containsExactly(NEWER),
                    () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("paused").isFalse(),
                    () -> assertThat(wrapped.isRunning(SUBSCRIPTION_ID)).as("running").isTrue()
            );
        }

        @Test
        void a_resume_at_now_stores_the_wrapped_models_global_checkpoint_and_resumes_there() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            wrapped.globalCheckpoint = GLOBAL;

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.now());

            // Then
            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(GLOBAL),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(GLOBAL)
            );
        }

        @Test
        void throws_IllegalStateException_when_a_resume_at_now_finds_no_global_checkpoint() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            wrapped.globalCheckpoint = null;

            // When
            Throwable thrown = catchThrowable(() -> repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.now()));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").isEmpty(),
                    () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("still paused").isTrue()
            );
        }

        @Test
        void a_resume_at_the_subscription_model_default_resumes_from_the_stored_checkpoint_and_stores_nothing_else() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            wrapped.globalCheckpoint = GLOBAL;
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.subscriptionModelDefault());

            // Then
            assertAll(
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(OLDER),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER)
            );
        }

        @Test
        void a_resume_at_a_dynamic_start_at_resolves_it_once_and_stores_what_it_answered() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            AtomicInteger resolutions = new AtomicInteger();
            StartAt dynamic = StartAt.dynamic(context -> {
                resolutions.incrementAndGet();
                return StartAt.checkpoint(NEWER);
            });

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, dynamic);

            // Then
            assertAll(
                    () -> assertThat(resolutions).as("resolutions").hasValue(1),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(NEWER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(NEWER)
            );
        }

        @Test
        void a_resume_at_a_dynamic_start_at_that_answers_now_stores_the_wrapped_models_global_checkpoint() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            wrapped.globalCheckpoint = GLOBAL;

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.dynamic(context -> StartAt.now()));

            // Then
            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(GLOBAL),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(GLOBAL)
            );
        }

        @Test
        void throws_IllegalArgumentException_when_a_dynamic_start_at_answers_null() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);

            // When
            Throwable thrown = catchThrowable(() -> repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.dynamic(context -> null)));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalArgumentException.class),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").isEmpty(),
                    () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("still paused").isTrue()
            );
        }

        @Test
        void throws_CheckpointWriteConditionNotFulfilledException_when_the_storage_refuses_the_write() {
            // Given a newer write version stored by another node, and a model that writes with an older one
            InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
            storage.save(SUBSCRIPTION_ID, OLDER, CheckpointWriteCondition.notOlderThan(5));
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(1));
            subscribeAndPause(model);

            // When
            Throwable thrown = catchThrowable(() -> repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(NEWER)));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").isEmpty(),
                    () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("still paused").isTrue()
            );
        }

        @Test
        void throws_what_the_wrapped_model_throws_after_the_checkpoint_is_stored_and_puts_back_the_checkpoint_stored_before() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            IllegalStateException wrappedFailure = new IllegalStateException("the wrapped model could not reopen its feed");
            wrapped.failResumeAtPositionWith = wrappedFailure;

            // When
            Throwable thrown = catchThrowable(() -> repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(NEWER)));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(wrappedFailure),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER)
            );
        }

        @Test
        void throws_what_the_wrapped_model_throws_and_keeps_the_new_checkpoint_when_none_was_stored_before() {
            // Given nothing stored for the subscription when the resume at a position runs
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            storage.delete(SUBSCRIPTION_ID);
            IllegalStateException wrappedFailure = new IllegalStateException("the wrapped model could not reopen its feed");
            wrapped.failResumeAtPositionWith = wrappedFailure;

            // When
            Throwable thrown = catchThrowable(() -> repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(NEWER)));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(wrappedFailure),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(NEWER)
            );
        }

        @Test
        void throws_what_the_wrapped_model_throws_with_the_failure_to_put_the_checkpoint_back_as_suppressed() {
            // Given a storage that takes the new position and then fails the save that puts the old one back
            PutBackFailingStorage storage = new PutBackFailingStorage();
            storage.save(SUBSCRIPTION_ID, OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            IllegalStateException wrappedFailure = new IllegalStateException("the wrapped model could not reopen its feed");
            wrapped.failResumeAtPositionWith = wrappedFailure;
            IllegalStateException putBackFailure = new IllegalStateException("the storage lost its connection");
            storage.failSaveNumber(2, putBackFailure);

            // When
            Throwable thrown = catchThrowable(() -> repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(NEWER)));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(wrappedFailure),
                    () -> assertThat(thrown.getSuppressed()).as("suppressed").containsExactly(putBackFailure)
            );
        }

        @Test
        void deletes_nothing_when_the_wrapped_model_throws_and_none_was_stored_before() {
            // Given nothing stored for the subscription, and a storage that records every delete and fails it
            PutBackFailingStorage storage = new PutBackFailingStorage();
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            storage.delete(SUBSCRIPTION_ID);
            storage.failDeletes(new IllegalStateException("the storage lost its connection"));
            IllegalStateException wrappedFailure = new IllegalStateException("the wrapped model could not reopen its feed");
            wrapped.failResumeAtPositionWith = wrappedFailure;

            // When
            Throwable thrown = catchThrowable(() -> repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(NEWER)));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(wrappedFailure),
                    () -> assertThat(thrown.getSuppressed()).as("suppressed").isEmpty(),
                    () -> assertThat(storage.deletesAttempted()).as("deletes attempted after the resume started").isZero()
            );
        }

        @Test
        void a_resume_at_a_GlobalCheckpoint_stores_nothing_and_resumes_the_wrapped_model_there() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            Checkpoint position = GlobalCheckpoint.of(42);

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(position));

            // Then
            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(position),
                    () -> assertThat(wrapped.isRunning(SUBSCRIPTION_ID)).as("running").isTrue()
            );
        }

        @Test
        void a_resume_at_a_GlobalCheckpoint_leaves_the_storage_empty_when_nothing_was_stored_before() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            storage.delete(SUBSCRIPTION_ID);
            Checkpoint position = GlobalCheckpoint.of(42);

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(position));

            // Then
            assertAll(
                    () -> assertThat(storage.exists(SUBSCRIPTION_ID)).as("anything stored").isFalse(),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(position)
            );
        }

        @Test
        void a_resume_at_a_GlobalCheckpoint_that_storage_read_back_as_a_string_stores_nothing_and_resumes_the_wrapped_model_there() {
            // Given the string form of a GlobalCheckpoint, as a checkpoint storage reads it back
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            Checkpoint position = new StringBasedCheckpoint(GlobalCheckpoint.of(42).asString());

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(position));

            // Then
            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(position)
            );
        }

        @Test
        void a_resume_at_a_CatchupTimeCheckpoint_stores_nothing_and_resumes_the_wrapped_model_there() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            Checkpoint position = catchupTimeCheckpoint();

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(position));

            // Then
            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(position),
                    () -> assertThat(wrapped.isRunning(SUBSCRIPTION_ID)).as("running").isTrue()
            );
        }

        @Test
        void a_resume_at_a_CatchupTimeCheckpoint_that_storage_read_back_as_a_string_stores_nothing_and_resumes_the_wrapped_model_there() {
            // Given the string form of a CatchupTimeCheckpoint, as a checkpoint storage reads it back
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            Checkpoint position = new StringBasedCheckpoint(catchupTimeCheckpoint().asString());

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(position));

            // Then
            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(position)
            );
        }

        @Test
        void a_resume_at_now_stores_nothing_when_the_wrapped_models_global_checkpoint_is_a_GlobalCheckpoint() {
            // Given a wrapped model whose present position is a GlobalCheckpoint
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeAndPause(model);
            wrapped.globalCheckpoint = GlobalCheckpoint.of(7);

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.now());

            // Then
            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER),
                    () -> assertThat(wrapped.checkpointsResumedAt).as("resumed at").containsExactly(GlobalCheckpoint.of(7))
            );
        }
    }

    @Nested
    @DisplayName("when the subscription is not checkpointed")
    class When_the_subscription_is_not_checkpointed {

        @Test
        void a_resume_at_a_checkpoint_is_passed_on_as_it_is_and_nothing_is_stored() {
            // Given a subscription that opted out of the durable model's checkpointing
            InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            StartAt optOut = StartAt.dynamic(context -> context.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
            model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            });
            model.pauseSubscription(SUBSCRIPTION_ID);
            StartAt startAt = StartAt.checkpoint(NEWER);

            // When
            repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, startAt);

            // Then
            assertAll(
                    () -> assertThat(wrapped.startAtsResumedAt).as("resumed at").containsExactly(startAt),
                    () -> assertThat(storage.exists(SUBSCRIPTION_ID)).as("anything stored").isFalse()
            );
        }
    }

    @Nested
    @DisplayName("when the subscription is running")
    class When_the_subscription_is_running {

        @Test
        void throws_what_the_wrapped_model_throws_and_stores_nothing() {
            // Given
            InMemoryCheckpointStorage storage = storageHolding(OLDER);
            RepositionableFake wrapped = new RepositionableFake();
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), event -> {
            });

            // When
            Throwable thrown = catchThrowable(() -> repositionableIn(model).resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(NEWER)));

            // Then
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("is not paused"),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(OLDER)
            );
        }
    }

    @Test
    void the_lookup_finds_the_durable_model_itself_when_the_wrapped_model_is_repositionable() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(new RepositionableFake(), new InMemoryCheckpointStorage());

        // When
        RepositionableSubscriptions found = repositionableIn(model);

        // Then
        assertThat(found).isSameAs(model);
    }

    private static CatchupTimeCheckpoint catchupTimeCheckpoint() {
        return CatchupTimeCheckpoint.of("2026-01-01T00:00:05Z", new StringBasedCheckpoint("live"), "2026-01-01T00:00:00Z");
    }

    private static InMemoryCheckpointStorage storageHolding(Checkpoint checkpoint) {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, checkpoint);
        return storage;
    }

    private static void subscribeAndPause(DurableSubscriptionModel model) {
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), event -> {
        });
        model.pauseSubscription(SUBSCRIPTION_ID);
    }

    private static RepositionableSubscriptions repositionableIn(DurableSubscriptionModel model) {
        return RepositionableSubscriptions.findIn(model).orElseThrow();
    }

    /**
     * A storage that fails the nth save counted from {@link #failSaveNumber}, and every delete after {@link #failDeletes}
     * while counting the deletes it is asked for.
     */
    private static class PutBackFailingStorage extends InMemoryCheckpointStorage {
        private int savesLeftBeforeFailure = Integer.MAX_VALUE;
        @Nullable private RuntimeException saveFailure = null;
        @Nullable private RuntimeException deleteFailure = null;
        private final AtomicInteger deletesAttempted = new AtomicInteger();

        void failSaveNumber(int number, RuntimeException failure) {
            savesLeftBeforeFailure = number - 1;
            saveFailure = failure;
        }

        void failDeletes(RuntimeException failure) {
            deleteFailure = failure;
        }

        int deletesAttempted() {
            return deletesAttempted.get();
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (saveFailure != null && savesLeftBeforeFailure-- == 0) {
                throw saveFailure;
            }
            return super.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public void delete(String subscriptionId) {
            if (deleteFailure != null) {
                deletesAttempted.incrementAndGet();
                throw deleteFailure;
            }
            super.delete(subscriptionId);
        }
    }

    /**
     * A wrapped model that tracks running and paused subscriptions and refuses a resume of one that isn't paused, as a
     * live model does, and records the position it was resumed at.
     */
    private static class RepositionableFake implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        @Nullable volatile Checkpoint globalCheckpoint = new StringBasedCheckpoint("global-at-subscribe");
        @Nullable volatile RuntimeException failResumeAtPositionWith = null;
        final List<StartAt> startAtsResumedAt = new CopyOnWriteArrayList<>();
        final List<Checkpoint> checkpointsResumedAt = new CopyOnWriteArrayList<>();
        private final Set<String> running = ConcurrentHashMap.newKeySet();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            running.add(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            if (!paused.contains(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            RuntimeException failure = failResumeAtPositionWith;
            if (failure != null) {
                throw failure;
            }
            startAtsResumedAt.add(startAt);
            if (startAt instanceof StartAt.StartAtCheckpoint at) {
                checkpointsResumedAt.add(at.checkpoint);
            }
            paused.remove(subscriptionId);
            running.add(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException("a plain resume is not expected");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (!running.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not running");
            }
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running.contains(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return globalCheckpoint;
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
