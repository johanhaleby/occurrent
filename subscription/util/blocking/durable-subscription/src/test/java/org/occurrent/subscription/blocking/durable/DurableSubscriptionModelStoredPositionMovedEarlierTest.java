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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A checkpoint storage refuses the save of a delivered event with a stored version that is not newer than this node's
 * once another node's start position moved the stored position earlier, since this node may have delivered events
 * after that position. The model tells its start refusal listeners, or logs an error when it has none, and throws the
 * refusal on, and it does not resume the subscription itself. A resume at a position stores this node's version with
 * the stored checkpoint first, which takes the refusal away, and writes once more.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DurableSubscriptionModelStoredPositionMovedEarlierTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final long VERSION = 2;
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint LATER_POSITION = new StringBasedCheckpoint("later-position");
    private static final Checkpoint EARLIER_POSITION = new StringBasedCheckpoint("earlier-position");
    private static final Checkpoint POSITION_OF_A_DELIVERED_EVENT = new StringBasedCheckpoint("position-of-a-delivered-event");
    private static final Checkpoint RESUMED_AT = new StringBasedCheckpoint("resumed-at");

    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(DurableSubscriptionModel.class);
    private @Nullable Level levelBefore;

    private final RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
    private final StorageWithAMovedEarlierMark storage = new StorageWithAMovedEarlierMark();
    private final List<CheckpointWriteConditionNotFulfilledException> reported = new CopyOnWriteArrayList<>();

    @BeforeEach
    void captureTheLog() {
        levelBefore = modelLog.getLevel();
        modelLog.setLevel(Level.ERROR);
        logged.start();
        modelLog.addAppender(logged);
    }

    @AfterEach
    void stopCapturingTheLog() {
        modelLog.detachAppender(logged);
        modelLog.setLevel(levelBefore);
    }

    @Test
    void a_refused_save_of_a_delivered_event_is_told_to_the_listeners_and_thrown_on_and_the_subscription_is_not_resumed() {
        // Given a node that stored a later position with its version and delivers from it, and another node's start position that moved it earlier
        DurableSubscriptionModel durable = subscribedWithAListener();
        storage.aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier();

        // When
        Throwable thrown = catchThrowable(() -> wrapped.deliver(SUBSCRIPTION_ID, POSITION_OF_A_DELIVERED_EVENT));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown to the wrapped model").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(reported).as("refusals told to the listener").containsExactly((CheckpointWriteConditionNotFulfilledException) thrown),
                () -> assertThat(reported.getFirst().storedVersion).as("stored version of the refusal").hasValue(VERSION),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(0),
                () -> assertThat(wrapped.resumedAt).as("resumes of the wrapped model at a position").isEmpty(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(EARLIER_POSITION.asString()),
                () -> assertThat(errors()).as("errors logged when a listener is told").isEmpty()
        );
        durable.shutdown();
    }

    @Test
    void a_refused_save_of_a_delivered_event_with_no_listener_added_logs_an_error_and_throws_the_refusal_on_and_the_subscription_is_not_resumed() {
        // Given
        DurableSubscriptionModel durable = subscribed();
        storage.aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier();

        // When
        Throwable thrown = catchThrowable(() -> wrapped.deliver(SUBSCRIPTION_ID, POSITION_OF_A_DELIVERED_EVENT));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown to the wrapped model").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(errors()).as("errors logged").singleElement().satisfies(error -> {
                    assertThat(error.getFormattedMessage()).contains(SUBSCRIPTION_ID).contains("moved its stored position earlier");
                    assertThat(error.getThrowableProxy()).as("exception logged").isNotNull();
                }),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(0),
                () -> assertThat(wrapped.resumedAt).as("resumes of the wrapped model at a position").isEmpty(),
                () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("paused in the wrapped model").isFalse()
        );
        durable.shutdown();
    }

    @Test
    void a_refused_save_of_a_delivered_event_for_a_newer_version_stored_is_not_told_to_the_listeners_and_not_logged_as_a_moved_position() {
        // Given another node's lease took over and stored a newer version
        DurableSubscriptionModel durable = subscribedWithAListener();
        storage.delegate.save(SUBSCRIPTION_ID, LATER_POSITION, CheckpointWriteCondition.notOlderThan(VERSION + 3));

        // When
        Throwable thrown = catchThrowable(() -> wrapped.deliver(SUBSCRIPTION_ID, POSITION_OF_A_DELIVERED_EVENT));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown to the wrapped model").isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class,
                        refusal -> assertThat(refusal.storedVersion).as("stored version of the refusal").hasValue(VERSION + 3)),
                () -> assertThat(reported).as("refusals told to the listener").isEmpty(),
                () -> assertThat(errors()).as("errors logged").isEmpty()
        );
        durable.shutdown();
    }

    @Test
    void a_resume_at_a_position_after_a_start_position_moved_the_stored_position_earlier_stores_this_nodes_version_with_the_stored_checkpoint_and_then_the_position() {
        // Given
        DurableSubscriptionModel durable = subscribedWithAListener();
        storage.aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier();
        durable.pauseSubscription(SUBSCRIPTION_ID);

        // When
        Throwable thrown = catchThrowable(() -> durable.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(RESUMED_AT)));

        // Then
        Throwable saveOfAnOlderVersion = catchThrowable(() -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale"), CheckpointWriteCondition.notOlderThan(VERSION - 1)));
        assertAll(
                () -> assertThat(thrown).as("thrown by the resume").isNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(RESUMED_AT.asString()),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(VERSION),
                () -> assertThat(storage.movedEarlier).as("stored position marked as moved earlier").isFalse(),
                () -> assertThat(wrapped.resumedAt).as("positions the wrapped model resumed at").containsExactly(RESUMED_AT),
                () -> assertThat(saveOfAnOlderVersion).as("save with an older version after the resume").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(reported).as("refusals told to the listener").isEmpty()
        );
        durable.shutdown();
    }

    @Test
    void a_plain_resume_after_a_start_position_moved_the_stored_position_earlier_stores_this_nodes_version_and_resumes_the_wrapped_model_from_the_stored_position() {
        // Given
        DurableSubscriptionModel durable = subscribedWithAListener();
        storage.aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier();
        durable.pauseSubscription(SUBSCRIPTION_ID);

        // When
        durable.resumeSubscription(SUBSCRIPTION_ID);
        Throwable saveOfThisNode = catchThrowable(() -> wrapped.deliver(SUBSCRIPTION_ID, POSITION_OF_A_DELIVERED_EVENT));

        // Then
        assertAll(
                () -> assertThat(saveOfThisNode).as("thrown by the save of an event delivered after the resume").isNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(POSITION_OF_A_DELIVERED_EVENT.asString()),
                () -> assertThat(wrapped.resumedAt).as("positions the wrapped model resumed at").containsExactly(EARLIER_POSITION)
        );
        durable.shutdown();
    }

    @Test
    void a_duplicate_subscribe_of_a_running_subscription_at_the_checkpoint_a_start_position_moved_earlier_is_refused_and_leaves_the_mark_so_the_next_event_save_of_the_running_subscription_is_refused() {
        // Given a running subscription, and another node's start position that moved the stored position earlier
        wrapped.refusesAnIdItAlreadyHolds = true;
        DurableSubscriptionModel durable = subscribedWithAListener();
        storage.aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier();

        // When
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(EARLIER_POSITION), cloudEvent -> {
        }));
        boolean markedAfterTheDuplicate = storage.movedEarlier;
        Throwable saveOfTheRunningSubscription = catchThrowable(() -> wrapped.deliver(SUBSCRIPTION_ID, POSITION_OF_A_DELIVERED_EVENT));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the duplicate subscribe").isInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(markedAfterTheDuplicate).as("stored position marked as moved earlier after the duplicate subscribe").isTrue(),
                () -> assertThat(saveOfTheRunningSubscription).as("thrown by the save of the running subscription").isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(reported).as("refusals told to the listener").hasSize(1),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(EARLIER_POSITION.asString())
        );
        durable.shutdown();
    }

    @Test
    void a_duplicate_subscribe_of_a_paused_subscription_at_the_checkpoint_a_start_position_moved_earlier_is_refused_and_leaves_the_mark() {
        // Given a paused subscription, and another node's start position that moved the stored position earlier
        wrapped.refusesAnIdItAlreadyHolds = true;
        DurableSubscriptionModel durable = subscribedWithAListener();
        durable.pauseSubscription(SUBSCRIPTION_ID);
        storage.aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier();

        // When
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(EARLIER_POSITION), cloudEvent -> {
        }));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the duplicate subscribe").isInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(storage.movedEarlier).as("stored position marked as moved earlier").isTrue(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(EARLIER_POSITION.asString())
        );
        durable.shutdown();
    }

    @Test
    void a_duplicate_subscribe_of_a_subscription_only_the_wrapped_model_holds_at_the_checkpoint_a_start_position_moved_earlier_is_refused_and_leaves_the_mark() {
        // Given a subscription subscribed straight on the wrapped model, and a stored position a start position moved earlier
        wrapped.refusesAnIdItAlreadyHolds = true;
        wrapped.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        storage.anEarlierPositionStoredWithAnOlderVersionThatThisNodesStartMovedEarlier();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(VERSION));

        // When
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(EARLIER_POSITION), cloudEvent -> {
        }));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the duplicate subscribe").isInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(storage.movedEarlier).as("stored position marked as moved earlier").isTrue(),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").isNotEqualTo(OptionalLong.of(VERSION))
        );
        durable.shutdown();
    }

    @Test
    void a_duplicate_subscribe_after_the_wrapped_model_stopped_answering_running_and_paused_is_refused_and_leaves_the_mark() {
        // Given a subscription this model tracks whose wrapped model answers neither running nor paused but still refuses the id
        wrapped.refusesAnIdItAlreadyHolds = true;
        DurableSubscriptionModel durable = subscribedWithAListener();
        wrapped.reportsNothingRunning = true;
        storage.aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier();

        // When
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(EARLIER_POSITION), cloudEvent -> {
        }));

        // Then
        assertAll(
                () -> assertThat(wrapped.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model").isFalse(),
                () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("paused in the wrapped model").isFalse(),
                () -> assertThat(thrown).as("thrown by the duplicate subscribe").isInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(storage.movedEarlier).as("stored position marked as moved earlier").isTrue(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(EARLIER_POSITION.asString())
        );
        durable.shutdown();
    }

    @Test
    void a_subscribe_at_the_checkpoint_a_start_position_moved_earlier_stores_this_nodes_version_with_it_so_the_first_event_save_goes_through() {
        // Given a checkpoint another node stored with an older version, and a start position of this node that moved it earlier
        storage.anEarlierPositionStoredWithAnOlderVersionThatThisNodesStartMovedEarlier();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(VERSION));

        // When
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(EARLIER_POSITION), cloudEvent -> {
        });
        OptionalLong versionAfterTheSubscribe = storage.writeVersion(SUBSCRIPTION_ID);
        boolean markedAfterTheSubscribe = storage.movedEarlier;
        Throwable thrown = catchThrowable(() -> wrapped.deliver(SUBSCRIPTION_ID, POSITION_OF_A_DELIVERED_EVENT));

        // Then
        assertAll(
                () -> assertThat(versionAfterTheSubscribe).as("stored version after the subscribe").hasValue(VERSION),
                () -> assertThat(markedAfterTheSubscribe).as("stored position marked as moved earlier after the subscribe").isFalse(),
                () -> assertThat(thrown).as("thrown by the save of the first event delivered").isNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(POSITION_OF_A_DELIVERED_EVENT.asString())
        );
        durable.shutdown();
    }

    @Test
    void a_subscribe_at_a_checkpoint_other_than_the_stored_one_stores_no_version_and_leaves_the_mark() {
        // Given
        storage.anEarlierPositionStoredWithAnOlderVersionThatThisNodesStartMovedEarlier();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(VERSION));

        // When
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(LATER_POSITION), cloudEvent -> {
        });

        // Then
        assertAll(
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(EARLIER_POSITION.asString()),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(VERSION - 1),
                () -> assertThat(storage.movedEarlier).as("stored position marked as moved earlier").isTrue()
        );
        durable.shutdown();
    }

    @Test
    void a_subscribe_at_the_checkpoint_a_start_position_moved_earlier_starts_there_when_a_newer_version_is_stored() {
        // Given another node's lease stored a newer version with the checkpoint
        storage.delegate.save(SUBSCRIPTION_ID, EARLIER_POSITION, CheckpointWriteCondition.notOlderThan(VERSION + 3));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(VERSION));

        // When
        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(EARLIER_POSITION), cloudEvent -> {
        }));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the subscribe").isNull(),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(VERSION + 3),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(EARLIER_POSITION.asString())
        );
        durable.shutdown();
    }

    @Test
    void a_subscribe_at_the_model_default_stores_this_nodes_version_with_a_checkpoint_a_start_position_moved_earlier_and_the_first_event_save_goes_through() {
        // Given
        storage.anEarlierPositionStoredWithAnOlderVersionThatThisNodesStartMovedEarlier();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(VERSION));

        // When
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable thrown = catchThrowable(() -> wrapped.deliver(SUBSCRIPTION_ID, POSITION_OF_A_DELIVERED_EVENT));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown by the save of the first event delivered").isNull(),
                () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(VERSION),
                () -> assertThat(storage.movedEarlier).as("stored position marked as moved earlier").isFalse(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(POSITION_OF_A_DELIVERED_EVENT.asString())
        );
        durable.shutdown();
    }

    private DurableSubscriptionModel subscribedWithAListener() {
        DurableSubscriptionModel durable = subscribed();
        durable.addStartRefusalListener(new StartRefusalListener() {
            @Override
            public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            }

            @Override
            public void handleHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
            }

            @Override
            public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
            }

            @Override
            public void handleStoredPositionMovedEarlier(String subscriptionId, CheckpointWriteConditionNotFulfilledException refusal) {
                reported.add(refusal);
            }
        });
        return durable;
    }

    private DurableSubscriptionModel subscribed() {
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(VERSION));
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        return durable;
    }

    private List<ILoggingEvent> errors() {
        return logged.list.stream().filter(event -> event.getLevel().equals(Level.ERROR)).toList();
    }

    /**
     * Delegates to a real {@link InMemoryCheckpointStorage}, which evaluates write conditions, and stands in for the
     * MongoDB storages' refusal. After {@link #aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier()}
     * a {@code notOlderThan} save is refused with the stored version, until a write goes through, or a resolution with
     * a version, with or without creating a checkpoint, or a {@code storeWriteVersion} call keeps the stored checkpoint.
     */
    private static final class StorageWithAMovedEarlierMark implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        volatile boolean movedEarlier;

        void aLaterPositionStoredWithThisNodesVersionThatAnotherStartMovedEarlier() {
            delegate.save(SUBSCRIPTION_ID, LATER_POSITION, CheckpointWriteCondition.notOlderThan(VERSION));
            delegate.save(SUBSCRIPTION_ID, EARLIER_POSITION, CheckpointWriteCondition.any());
            movedEarlier = true;
        }

        void anEarlierPositionStoredWithAnOlderVersionThatThisNodesStartMovedEarlier() {
            delegate.save(SUBSCRIPTION_ID, EARLIER_POSITION, CheckpointWriteCondition.notOlderThan(VERSION - 1));
            movedEarlier = true;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (movedEarlier && condition instanceof CheckpointWriteCondition.NotOlderThan) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, delegate.writeVersion(subscriptionId), condition);
            }
            Checkpoint saved = delegate.save(subscriptionId, checkpoint, condition);
            movedEarlier = false;
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
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            Optional<Checkpoint> resolution = delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
            movedEarlier = false;
            return resolution;
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRaceWithoutCreatingACheckpoint(String subscriptionId, Checkpoint candidate, long writeVersion) {
            Optional<Checkpoint> resolution = delegate.resolveFirstCheckpointRaceWithoutCreatingACheckpoint(subscriptionId, candidate, writeVersion);
            if (resolution.isPresent()) {
                movedEarlier = false;
            }
            return resolution;
        }

        @Override
        public Optional<Checkpoint> storeWriteVersion(String subscriptionId, long writeVersion) {
            Optional<Checkpoint> stored = delegate.storeWriteVersion(subscriptionId, writeVersion);
            if (stored.isPresent()) {
                movedEarlier = false;
            }
            return stored;
        }
    }
}
