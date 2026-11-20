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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A first position is recorded for a subscription started from the model default with nothing stored. A storage that
 * answers {@code false} from {@code evaluatesWriteConditionsFor(id)} can only write it unconditionally, so two nodes
 * recording it at the same moment can overwrite each other's and lose the events between the two positions. The model
 * refuses such a subscription before anything is written, unless {@code startWhenFirstPositionCanBeOverwritten} is set.
 * A subscription that records no first position is never refused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionCanBeOverwrittenTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final String OTHER_SUBSCRIPTION_ID = "otherSubscription";
    private static final String WRITE_ONLY_UNCONDITIONALLY = "startWhenFirstPositionCanBeOverwritten";

    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(DurableSubscriptionModel.class);

    @BeforeEach
    void captureTheLog() {
        logged.start();
        modelLog.addAppender(logged);
    }

    @AfterEach
    void stopCapturingTheLog() {
        modelLog.detachAppender(logged);
    }

    @Test
    void a_default_start_is_refused_before_anything_is_written_when_the_storage_cannot_evaluate_write_conditions() {
        HoldingFeed wrapped = new HoldingFeed("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY)
                .hasMessageContaining(SUBSCRIPTION_ID);

        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
        assertThat(wrapped.subscriptions).as("the subscriptions the wrapped model holds").isEmpty();
    }

    @Test
    void a_default_start_evaluated_by_the_wrapped_model_on_its_own_thread_is_refused_by_that_evaluation() {
        EvaluatesOnItsOwnThread wrapped = new EvaluatesOnItsOwnThread("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY);

        assertThat(wrapped.evaluated).as("the start position the wrapped model evaluated")
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .havingCause()
                .isInstanceOf(IllegalStateException.class)
                .withMessageContaining(WRITE_ONLY_UNCONDITIONALLY);
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
        assertThat(wrapped.subscriptions).as("the subscriptions the wrapped model holds").isEmpty();
    }

    @Test
    void a_dynamic_start_that_answers_the_model_default_is_refused_the_same_way() {
        HoldingFeed wrapped = new HoldingFeed("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, StartAt.dynamic(StartAt::subscriptionModelDefault), __ -> {
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY)
                .hasMessageContaining(SUBSCRIPTION_ID);

        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
        assertThat(wrapped.subscriptions).as("the subscriptions the wrapped model holds").isEmpty();
    }

    @Test
    void a_default_start_is_refused_on_subscribe_when_the_wrapped_model_has_no_position_yet_and_starting_without_one_is_allowed() {
        PositionAvailableLater wrapped = new PositionAvailableLater("position-available-later");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));
        wrapped.hasAPosition = true;

        assertThat(thrown)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY)
                .hasMessageContaining(SUBSCRIPTION_ID);
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void only_the_subscription_id_the_storage_cannot_evaluate_write_conditions_for_is_refused() {
        HoldingFeed wrapped = new HoldingFeed("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining(WRITE_ONLY_UNCONDITIONALLY);
        durable.subscribe(OTHER_SUBSCRIPTION_ID, __ -> {
        });

        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
        assertThat(storage.read(OTHER_SUBSCRIPTION_ID).asString()).isEqualTo("position-at-subscribe");
        assertThat(storage.saves).extracting(save -> save.subscriptionId).containsExactly(OTHER_SUBSCRIPTION_ID);
        assertThat(storage.saves).extracting(save -> save.condition).containsExactly(CheckpointWriteCondition.ifAbsent());
        assertThat(wrapped.subscriptions).containsExactly(OTHER_SUBSCRIPTION_ID);
    }

    @Test
    void a_start_position_of_the_callers_own_is_not_refused_since_no_first_position_is_recorded() {
        HoldingFeed wrapped = new HoldingFeed("position-at-subscribe");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        durable.subscribe(SUBSCRIPTION_ID, StartAt.checkpoint(new StringBasedCheckpoint("replay-from-here")), __ -> {
        });

        assertThat(wrapped.subscriptions).containsExactly(SUBSCRIPTION_ID);
        assertThat(wrapped.startedAt).hasSize(1);
        assertThat(wrapped.startedAt.getFirst()).isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("replay-from-here"));
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_default_start_with_a_checkpoint_already_stored_starts_from_it_whatever_the_storage_evaluates() {
        HoldingFeed wrapped = new HoldingFeed("position-at-subscribe");
        wrapped.evaluatesStartAtWhenSubscribed = true;
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        storage.writeWithoutRecording(SUBSCRIPTION_ID, new StringBasedCheckpoint("from-a-previous-run"));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(wrapped.subscriptions).containsExactly(SUBSCRIPTION_ID);
        assertThat(wrapped.startedAt).hasSize(1);
        assertThat(wrapped.startedAt.getFirst()).isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("from-a-previous-run"));
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
    }

    @Test
    void an_evaluation_that_finds_the_checkpoint_deleted_since_subscribe_starts_from_the_one_read_then_on_a_storage_that_cannot_evaluate_write_conditions() {
        HoldingFeed wrapped = new HoldingFeed("position-after-the-delete");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        storage.writeWithoutRecording(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-at-subscribe"));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });
        storage.delete(SUBSCRIPTION_ID);

        StartAt held = wrapped.startedAt.getFirst();
        List<StartAt> evaluated = new CopyOnWriteArrayList<>();
        Throwable thrown = catchThrowable(() -> evaluated.add(held.get(new SubscriptionModelContext(HoldingFeed.class))));

        assertThat(thrown).as("what the evaluation threw").isNull();
        assertThat(evaluated.getFirst()).isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("stored-at-subscribe"));
        assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void an_evaluation_that_finds_the_checkpoint_deleted_since_subscribe_records_the_position_of_the_wrapped_model_when_the_first_position_can_be_overwritten_by_opt_in() {
        HoldingFeed wrapped = new HoldingFeed("position-after-the-delete");
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        storage.writeWithoutRecording(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-at-subscribe"));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(1).startWhenFirstPositionCanBeOverwritten(true));
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });
        storage.delete(SUBSCRIPTION_ID);

        StartAt held = wrapped.startedAt.getFirst();
        List<StartAt> evaluated = new CopyOnWriteArrayList<>();
        Throwable thrown = catchThrowable(() -> evaluated.add(held.get(new SubscriptionModelContext(HoldingFeed.class))));

        assertThat(thrown).as("what the evaluation threw").isNull();
        // The opt-in accepts an unconditional write, so the evaluation records a first position as it would on a storage
        // that evaluates ifAbsent, and the fallback to the checkpoint read at subscribe, there only to avoid that write,
        // is not taken
        assertThat(evaluated.getFirst()).isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("position-after-the-delete"));
        assertThat(storage.saves).as("the saves the storage saw").hasSize(1);
        assertThat(storage.saves.getFirst().subscriptionId).isEqualTo(SUBSCRIPTION_ID);
        assertThat(storage.saves.getFirst().position).isEqualTo("position-after-the-delete");
        assertThat(storage.saves.getFirst().condition).isEqualTo(CheckpointWriteCondition.any());
        assertThat(storage.read(SUBSCRIPTION_ID)).isNotNull().extracting(Checkpoint::asString).isEqualTo("position-after-the-delete");
    }

    @Test
    void the_opt_in_writes_the_first_position_unconditionally_and_warns_as_before_the_refusal_existed() {
        HoldingFeed wrapped = new HoldingFeed("position-at-subscribe");
        wrapped.evaluatesStartAtWhenSubscribed = true;
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage(Set.of(SUBSCRIPTION_ID));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(1).startWhenFirstPositionCanBeOverwritten(true));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(storage.saves).hasSize(1);
        assertThat(storage.saves.getFirst().subscriptionId).isEqualTo(SUBSCRIPTION_ID);
        assertThat(storage.saves.getFirst().position).isEqualTo("position-at-subscribe");
        assertThat(storage.saves.getFirst().condition).isEqualTo(CheckpointWriteCondition.any());
        assertThat(wrapped.subscriptions).containsExactly(SUBSCRIPTION_ID);
        assertThat(wrapped.startedAt).hasSize(1);
        assertThat(wrapped.startedAt.getFirst()).isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("position-at-subscribe"));
        assertThat(warnings()).hasSize(1).first().asString()
                .contains("does not evaluate write conditions for subscription " + SUBSCRIPTION_ID)
                .contains("written unconditionally");
    }

    @Test
    void the_opt_in_survives_every_copy_of_the_config() {
        DurableSubscriptionModelConfig optedIn = new DurableSubscriptionModelConfig(1).startWhenFirstPositionCanBeOverwritten(true);

        assertThat(optedIn.startWhenNoStartPositionCanBeRecorded(true).startWhenFirstPositionCanBeOverwritten).as("after startWhenNoStartPositionCanBeRecorded").isTrue();
        assertThat(optedIn.saveQuietPositionEvery(Duration.ofSeconds(7)).startWhenFirstPositionCanBeOverwritten).as("after saveQuietPositionEvery").isTrue();
        assertThat(optedIn.neverSaveQuietPosition().startWhenFirstPositionCanBeOverwritten).as("after neverSaveQuietPosition").isTrue();
        assertThat(optedIn.startWhenFirstPositionCanBeOverwritten(false).startWhenFirstPositionCanBeOverwritten).as("after turning it off again").isFalse();
        assertThat(new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true).saveQuietPositionEvery(Duration.ofSeconds(7)).neverSaveQuietPosition().startWhenFirstPositionCanBeOverwritten)
                .as("a config that never opted in").isFalse();
    }

    private List<String> warnings() {
        return logged.list.stream().filter(event -> event.getLevel().equals(Level.WARN)).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private record Save(String subscriptionId, String position, CheckpointWriteCondition condition) {
    }

    /**
     * Answers {@code false} from {@code evaluatesWriteConditionsFor} for the ids it is given, and {@code true} for every
     * other, as a storage that reserves a shape of id it cannot evaluate a condition for does. Records every save.
     */
    private static final class RecordingCheckpointStorage extends InMemoryCheckpointStorage {

        final List<Save> saves = new CopyOnWriteArrayList<>();
        private final Set<String> idsWithoutConditions;

        RecordingCheckpointStorage(Set<String> idsWithoutConditions) {
            this.idsWithoutConditions = idsWithoutConditions;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            saves.add(new Save(subscriptionId, checkpoint.asString(), condition));
            return super.save(subscriptionId, checkpoint, condition);
        }

        void writeWithoutRecording(String subscriptionId, Checkpoint checkpoint) {
            super.save(subscriptionId, checkpoint, CheckpointWriteCondition.any());
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return !idsWithoutConditions.contains(subscriptionId);
        }
    }

    /**
     * Holds the subscription and leaves the start position to the subscribing model, unless it is told to evaluate it
     * on the subscribing thread once it holds the subscription.
     */
    private static class HoldingFeed implements CheckpointAwareSubscriptionModel {
        final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
        final List<StartAt> startedAt = new CopyOnWriteArrayList<>();
        boolean evaluatesStartAtWhenSubscribed = false;
        private final Checkpoint globalCheckpoint;

        HoldingFeed(String globalCheckpoint) {
            this.globalCheckpoint = new StringBasedCheckpoint(globalCheckpoint);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscriptions.add(subscriptionId);
            startedAt.add(evaluatesStartAtWhenSubscribed && startAt.isDynamic() ? startAt.get(new SubscriptionModelContext(HoldingFeed.class)) : startAt);
            return subscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
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
            return subscriptions.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return subscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
        }

        static Subscription subscription(String subscriptionId) {
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

    /**
     * Has no global checkpoint until it is told it has one, as a model that has not yet seen anything in its feed
     * when the subscription is registered.
     */
    private static final class PositionAvailableLater extends HoldingFeed {
        volatile boolean hasAPosition = false;

        PositionAvailableLater(String globalCheckpoint) {
            super(globalCheckpoint);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return hasAPosition ? super.globalCheckpoint() : null;
        }
    }

    /**
     * Evaluates the start position of the subscription on a thread of its own once the subscribing thread asks for the
     * global checkpoint, and holds the subscribing thread there until that evaluation has ended, so the evaluation is
     * what the model refuses first. Both threads are answered with the same global checkpoint.
     */
    private static final class EvaluatesOnItsOwnThread extends HoldingFeed {
        final CompletableFuture<@Nullable StartAt> evaluated = new CompletableFuture<>();
        private final CountDownLatch askedForTheGlobalCheckpoint = new CountDownLatch(1);
        private volatile @Nullable Thread evaluator;

        EvaluatesOnItsOwnThread(String globalCheckpoint) {
            super(globalCheckpoint);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscriptions.add(subscriptionId);
            evaluator = Thread.ofPlatform().start(() -> {
                try {
                    askedForTheGlobalCheckpoint.await();
                    evaluated.complete(startAt.get(new SubscriptionModelContext(EvaluatesOnItsOwnThread.class)));
                } catch (Throwable t) {
                    evaluated.completeExceptionally(t);
                }
            });
            return subscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            if (Thread.currentThread() != evaluator) {
                askedForTheGlobalCheckpoint.countDown();
                try {
                    evaluated.get(5, java.util.concurrent.TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // The evaluation refused, which is what the test asserts on
                }
            }
            return super.globalCheckpoint();
        }
    }
}
