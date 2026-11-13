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

package org.occurrent.subscription.blocking.durable.catchup;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.internal.LostStartHold;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A catch-up owns each subscription made through it from subscribe until cancel. When the live model loses the history
 * of an owned subscription and no replay can take the loss over without skipping events, the subscription is paused for
 * good and nothing is ever restarted from now. A subscription made on the live model directly is left to the live model.
 * The live model restarts from now on any loss that is not taken over, as the MongoDB subscription model does by default.
 */
@DisplayName("StreamCatchupHistoryLossOwnership")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupHistoryLossOwnershipTest {

    private static final String ID = "subscription";
    private static final String OTHER_ID = "other";
    private static final String THIRD_ID = "third";
    private static final Checkpoint TOKEN = new StringBasedCheckpoint("token-1");
    private static final Checkpoint LATER_TOKEN = new StringBasedCheckpoint("later-token");

    private final Logger catchupLogger = (Logger) LoggerFactory.getLogger("org.occurrent.subscription.blocking.durable.catchup");
    private final LoggedEvents logged = new LoggedEvents();
    private final CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> signals = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private InMemoryEventStore eventStore;
    private List<DomainEvent> history;
    private InMemoryCheckpointStorage storage;
    private HistoryLosingLiveModel liveModel;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        LocalDateTime time = LocalDateTime.now();
        history = List.of(nameDefined(time, "event1"), nameDefined(time, "event2"), nameDefined(time, "event3"));
        history.forEach(this::write);
        storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                saved.add(id + "=" + checkpoint.asString());
                return super.save(id, checkpoint, condition);
            }
        };
        liveModel = new HistoryLosingLiveModel();
        logged.start();
        catchupLogger.addAppender(logged);
    }

    @AfterEach
    void shutdown() {
        catchupLogger.detachAppender(logged);
        if (catchup != null) {
            catchup.shutdown();
        }
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void throws_CatchupCannotResumeException_when_a_live_start_checkpoint_is_older_than_the_history_of_the_live_model() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();

        // When
        Throwable thrown = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.subscribeCalls).as("subscribes on the live model").isEmpty(),
                () -> assertThat(saved).as("positions stored").isEmpty(),
                () -> assertThat(storage.read(ID)).as("stored position").isNull(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse()
        );
    }

    @Test
    void a_live_start_checkpoint_the_live_model_has_the_history_back_to_is_subscribed_on_the_live_model() {
        // Given
        create();

        // When
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());

        // Then
        assertAll(
                () -> assertThat(liveModel.subscribeCalls).as("subscribes on the live model").containsExactly(ID),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue()
        );
    }

    @Test
    void a_subscribe_of_the_id_after_a_refused_live_start_succeeds_from_a_checkpoint_the_live_model_has_the_history_back_to() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();
        assertThatThrownBy(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action())).isInstanceOf(CatchupCannotResumeException.class);

        // When
        catchup.subscribe(ID, StartAt.checkpoint(LATER_TOKEN), action());

        // Then
        assertThat(catchup.isRunning(ID)).as("running").isTrue();
    }

    @Test
    void a_live_started_subscription_whose_token_ages_out_while_it_is_paused_is_paused_with_one_error_and_replays_nothing() {
        // Given
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        catchup.pauseSubscription(ID);
        liveModel.withoutHistoryBack(TOKEN);
        List<String> storedBefore = List.copyOf(saved);

        // When
        catchup.resumeSubscription(ID);

        // Then
        assertAll(
                () -> assertThat(liveModel.lossesReported).as("losses the live model reported").containsExactly(TOKEN),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(ID, TOKEN)),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(signals).as("catch-up signals, so no takeover replay ran").isEmpty(),
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(saved).as("positions stored").isEqualTo(storedBefore)
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_live_started_subscription_whose_token_aged_out_while_it_was_paused() {
        // Given
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        catchup.pauseSubscription(ID);
        liveModel.withoutHistoryBack(TOKEN);
        catchup.resumeSubscription(ID);
        int resumesBefore = liveModel.resumeCalls.size();

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").hasSize(resumesBefore),
                () -> assertThat(signals).as("catch-up signals, so no takeover replay ran").isEmpty(),
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").hasSize(1)
        );
    }

    @Test
    void a_loss_from_a_later_token_after_a_replay_handed_over_pauses_the_subscription_with_one_error_and_restarts_nothing() {
        // Given
        subscribeFromTheBeginningAndAwaitTheHandOver();
        List<String> storedBefore = List.copyOf(saved);

        // When
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);

        // Then
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(ID, LATER_TOKEN)),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(received).as("events delivered, the history once and no second replay").containsExactlyElementsOf(history),
                () -> assertThat(signals).as("catch-up signals, one replay").containsExactly("catchupStarted", "historyRead"),
                () -> assertThat(saved).as("positions stored").isEqualTo(storedBefore)
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_subscription_that_lost_history_from_a_later_token_after_a_replay_handed_over() {
        // Given
        subscribeFromTheBeginningAndAwaitTheHandOver();
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessage(HistoryLossMessages.historyLost(ID, LATER_TOKEN).getMessage()).hasMessageContaining("where it stopped");
        assertAll(
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty(),
                () -> assertThat(received).as("events delivered, so no takeover replay ran").containsExactlyElementsOf(history),
                () -> assertThat(signals).as("catch-up signals, so no takeover replay ran").containsExactly("catchupStarted", "historyRead"),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").hasSize(1)
        );
    }

    @Test
    void a_loss_reported_while_the_catch_up_is_shutting_down_pauses_the_subscription_and_stores_nothing() {
        // Given
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        List<String> storedBefore = List.copyOf(saved);
        // shutdown() clears the record and the listener, so the window where the flag is set and the record is not yet cleared is entered by setting the flag
        catchup.shuttingDown = true;

        // When
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);

        // Then
        assertAll(
                () -> assertThat(liveModel.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(ID, LATER_TOKEN)),
                () -> assertThat(signals).as("catch-up signals, so no takeover replay ran").isEmpty(),
                () -> assertThat(saved).as("positions stored").isEqualTo(storedBefore),
                () -> assertThat(storage.read(ID)).as("stored position").isNull()
        );
    }

    @Test
    void a_subscription_made_on_the_live_model_directly_is_restarted_from_now_by_the_live_model_on_a_loss() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();

        // When
        liveModel.subscribe(OTHER_ID, null, StartAt.checkpoint(TOKEN), cloudEvent -> {
        });

        // Then
        assertAll(
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").containsExactly(OTHER_ID),
                () -> assertThat(liveModel.isRunning(OTHER_ID)).as("running").isTrue(),
                () -> assertThat(errorsLogged()).as("errors logged by the catch-up").isEmpty()
        );
    }

    @Test
    void a_loss_reported_inside_the_subscribe_of_the_live_model_leaves_the_subscription_owned_and_paused_with_one_error_and_restarts_nothing() {
        // Given
        liveModel.reportingTheLossInsideSubscribe(LATER_TOKEN);
        create();

        // When
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());

        // Then
        assertAll(
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(liveModel.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(ID, LATER_TOKEN)),
                () -> assertThat(saved).as("positions stored").isEmpty()
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_subscription_whose_loss_was_reported_inside_the_subscribe_of_the_live_model() {
        // Given
        liveModel.reportingTheLossInsideSubscribe(LATER_TOKEN);
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(LATER_TOKEN.asString());
        assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty();
    }

    @Test
    void a_subscribe_the_live_model_throws_from_leaves_no_record_so_the_id_subscribes_again() {
        // Given
        liveModel.failingTheNextSubscribeWith(new IllegalStateException("the live model could not open"));
        create();
        assertThatThrownBy(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action())).isInstanceOf(IllegalStateException.class).hasMessage("the live model could not open");

        // When
        Throwable thrown = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));

        // Then
        assertThat(thrown).as("thrown by the second subscribe").isNull();
        assertThat(catchup.isRunning(ID)).as("running").isTrue();
    }

    @Test
    void a_loss_for_an_id_whose_subscribe_threw_is_left_to_the_live_model_since_the_catch_up_owns_nothing() {
        // Given
        liveModel.failingTheNextSubscribeWith(new IllegalStateException("the live model could not open"));
        liveModel.withoutHistoryBack(LATER_TOKEN);
        create();
        assertThatThrownBy(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action())).isInstanceOf(IllegalStateException.class);

        // When
        liveModel.subscribe(ID, null, StartAt.checkpoint(LATER_TOKEN), cloudEvent -> {
        });

        // Then
        assertAll(
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").containsExactly(ID),
                () -> assertThat(errorsLogged()).as("errors logged by the catch-up").isEmpty()
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_after_a_terminal_failure_and_no_takeover_replay_runs() {
        // Given
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(LATER_TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty(),
                () -> assertThat(liveModel.subscribeCalls).as("subscribes on the live model").containsExactly(ID),
                () -> assertThat(signals).as("catch-up signals, so no takeover replay ran").isEmpty(),
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue()
        );
    }

    @Test
    void cancelling_a_subscription_with_a_terminal_failure_clears_it_so_the_id_subscribes_again() {
        // Given
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);
        assertThatThrownBy(() -> catchup.resumeSubscription(ID)).isInstanceOf(CatchupCannotResumeException.class);

        // When
        catchup.cancelSubscription(ID);
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());

        // Then
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").hasSize(1)
        );
    }

    @Test
    void a_pause_and_resume_of_a_subscription_made_again_after_a_cancel_does_not_throw_the_terminal_failure_of_the_cancelled_one() {
        // Given
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);
        catchup.cancelSubscription(ID);
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        catchup.pauseSubscription(ID);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertThat(catchup.isRunning(ID)).as("running").isTrue();
    }

    @Test
    void a_pause_and_resume_does_not_throw_the_terminal_failure_once_the_subscription_is_resumed_at_a_kept_checkpoint() {
        // Given
        terminallyPausedAfterALoss();
        catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN));
        catchup.pauseSubscription(ID);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged, only the one for the loss").hasSize(1)
        );
    }

    @Test
    void a_stop_and_start_does_not_throw_the_terminal_failure_once_the_subscription_is_resumed_at_a_kept_checkpoint() {
        // Given
        terminallyPausedAfterALoss();
        catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN));
        catchup.stop();

        // When
        Throwable thrown = catchThrowable(() -> catchup.start(true));

        // Then
        assertThat(thrown).as("thrown by the start").isNull();
        assertAll(
                () -> assertThat(liveModel.isRunning(ID)).as("running on the live model").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged, only the one for the loss").hasSize(1)
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_after_the_live_model_threw_from_a_resume_at_a_kept_checkpoint() {
        // Given
        terminallyPausedAfterALoss();
        liveModel.failingTheNextRepositioningResumeWith(new IllegalStateException("live model refuses the resume"));
        assertThatThrownBy(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN))).isInstanceOf(IllegalStateException.class);
        int resumesBefore = liveModel.resumeCalls.size();

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(LATER_TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").hasSize(resumesBefore),
                () -> assertThat(liveModel.isPaused(ID)).as("paused on the live model").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty()
        );
    }

    @Test
    void a_loss_of_the_live_start_is_still_taken_over_after_a_resume_at_a_kept_checkpoint_threw_a_checked_exception_without_declaring_it() {
        // Given a replay that handed over at the live start the live model reads first
        subscribeFromTheBeginningAndAwaitTheHandOver();
        IOException liveFailure = new IOException("live model lost its connection");
        liveModel.beforeTheNextResumeAt(() -> {
            throw sneaky(liveFailure);
        });
        Throwable resumeAtFailure = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN)));

        // When the live model loses the history from that live start
        liveModel.losesTheHistoryOf(ID, new StringBasedCheckpoint("live-start-1"));

        // Then the catch-up takes the loss over instead of pausing the subscription for good
        assertThat(resumeAtFailure).as("thrown by the resume at a checkpoint").isSameAs(liveFailure);
        assertThat(errorsLogged()).as("errors logged").isEmpty();
        // Paused until the takeover replay has handed over again
        await().untilAsserted(() -> assertThat(catchup.isPaused(ID)).as("paused").isFalse());
        assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty();
    }

    @Test
    void start_resumes_the_other_healthy_paused_subscription_when_resuming_one_throws_a_checked_exception_without_declaring_it() {
        // Given the terminally failed subscription makes start resume each paused subscription itself, in an order the live model's id set doesn't fix
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        catchup.subscribe(OTHER_ID, StartAt.checkpoint(TOKEN), action());
        catchup.subscribe(THIRD_ID, StartAt.checkpoint(TOKEN), action());
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);
        catchup.stop();
        IOException liveFailure = new IOException("live model lost its connection");
        liveModel.beforeTheNextResume(() -> {
            throw sneaky(liveFailure);
        });

        // When
        Throwable thrown = catchThrowable(() -> catchup.start(true));

        // Then
        assertThat(thrown).isNotNull();
        List<Throwable> failures = Stream.concat(Stream.of(thrown), Stream.of(thrown.getSuppressed())).toList();
        assertAll(
                () -> assertThat(failures).as("failures thrown").hasSize(2).contains(liveFailure).anySatisfy(failure -> assertThat(failure).isInstanceOf(CatchupCannotResumeException.class)),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model that went through").hasSize(1),
                () -> assertThat(Stream.of(OTHER_ID, THIRD_ID).filter(liveModel::isRunning).count()).as("healthy subscriptions running on the live model").isEqualTo(1),
                () -> assertThat(liveModel.isPaused(ID)).as("failed subscription paused").isTrue()
        );
    }

    @Test
    void start_resumes_the_healthy_paused_subscription_and_leaves_the_terminally_failed_one_paused() {
        // Given
        terminallyFailedAndHealthyPausedSubscriptions();

        // When
        assertThatThrownBy(() -> catchup.start(true)).isInstanceOf(CatchupCannotResumeException.class);

        // Then
        assertAll(
                () -> assertThat(liveModel.isRunning(OTHER_ID)).as("healthy subscription running").isTrue(),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").containsExactly(OTHER_ID),
                () -> assertThat(liveModel.isPaused(ID)).as("failed subscription paused").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(signals).as("catch-up signals, so no takeover replay ran").isEmpty()
        );
    }

    @Test
    void throws_CatchupCannotResumeException_from_start_once_every_other_paused_subscription_is_resumed() {
        // Given
        terminallyFailedAndHealthyPausedSubscriptions();

        // When
        Throwable thrown = catchThrowable(() -> catchup.start(true));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(LATER_TOKEN.asString());
        assertThat(liveModel.isRunning(OTHER_ID)).as("healthy subscription running").isTrue();
    }

    @Test
    void a_loss_reported_for_a_run_a_cancel_ended_whose_takeover_the_live_model_never_runs_does_not_refuse_a_resume_of_the_subscription_made_since() {
        // Given an id subscribed, cancelled and subscribed again at another checkpoint
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        catchup.cancelSubscription(ID);
        catchup.subscribe(ID, StartAt.checkpoint(LATER_TOKEN), action());

        // When the live model is asked for the run the cancel ended and runs no takeover, and the new subscription is paused and resumed
        HistoryLossHandling answered = liveModel.asksTheListenersAbout(ID, TOKEN);
        catchup.pauseSubscription(ID);
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(answered).as("answer for the stale report").isInstanceOf(HistoryLossHandling.TakeOver.class),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_subscription_made_after_a_cancel_whose_loss_the_live_model_took_over() {
        // Given an id subscribed, cancelled and subscribed again at another checkpoint
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        catchup.cancelSubscription(ID);
        catchup.subscribe(ID, StartAt.checkpoint(LATER_TOKEN), action());

        // When the live model reports a loss of the new run and runs the takeover after pausing it
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(LATER_TOKEN.asString());
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(ID, LATER_TOKEN))
        );
    }

    @Test
    void a_loss_whose_takeover_runs_after_the_subscription_is_resumed_at_a_kept_checkpoint_does_not_refuse_a_pause_and_resume_of_the_healthy_subscription() {
        // Given a loss reported, with the subscription resumed at a kept checkpoint between the pause and what follows it
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        AtomicReference<Throwable> repositioning = new AtomicReference<>();
        liveModel.betweenTheNextTakeOverPauseAndWhatFollows(() -> repositioning.set(catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN)))));
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);

        // When the subscription is paused and resumed
        catchup.pauseSubscription(ID);
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(repositioning.get()).as("thrown by the resume at the kept checkpoint").isNull();
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_subscription_whose_loss_the_live_model_took_over_after_a_hook_that_repositioned_nothing() {
        // Given a loss reported, with something that does not reposition the subscription run between the pause and what follows it
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        AtomicInteger hookRuns = new AtomicInteger();
        liveModel.betweenTheNextTakeOverPauseAndWhatFollows(hookRuns::incrementAndGet);
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(LATER_TOKEN.asString());
        assertAll(
                () -> assertThat(hookRuns).as("times the hook ran").hasValue(1),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(ID, LATER_TOKEN))
        );
    }

    @Test
    void a_loss_the_old_run_reports_inside_a_resume_at_a_kept_checkpoint_does_not_refuse_a_pause_and_resume_of_the_healthy_subscription() {
        // Given a subscription whose old run reports the loss of its token inside a resume at a kept checkpoint, before the live model resumes
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        liveModel.beforeTheNextResumeAt(() -> liveModel.losesTheHistoryOf(ID, TOKEN));

        // When the subscription is resumed at the kept checkpoint, paused and resumed
        Throwable repositioning = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN)));
        catchup.pauseSubscription(ID);
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(repositioning).as("thrown by the resume at the kept checkpoint").isNull();
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(liveModel.lossesReported).as("losses the live model reported").containsExactly(TOKEN),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").containsExactly(ID, ID),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty()
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_subscription_that_lost_its_history_after_a_resume_at_a_kept_checkpoint() {
        // Given a subscription that a loss paused and a resume at a kept checkpoint resumed
        terminallyPausedAfterALoss();
        catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN));

        // When the resumed run loses the history of the kept checkpoint
        liveModel.losesTheHistoryOf(ID, TOKEN);
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString()).hasMessageNotContaining(LATER_TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model, the one at the kept checkpoint").containsExactly(ID),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged, one for each loss").containsExactly(terminalFailureLog(ID, LATER_TOKEN), terminalFailureLog(ID, TOKEN))
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_subscription_whose_run_reopened_inside_the_takeover_window_lost_the_history_again() {
        // Given a loss reported, with the subscription resumed between the pause and what follows it, so the run is open again
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        AtomicReference<Throwable> inTheWindow = new AtomicReference<>();
        liveModel.betweenTheNextTakeOverPauseAndWhatFollows(() -> inTheWindow.set(catchThrowable(() -> catchup.resumeSubscription(ID))));
        liveModel.losesTheHistoryOf(ID, TOKEN);
        assertThat(liveModel.isRunning(ID)).as("reopened by the resume inside the window").isTrue();

        // When the reopened run loses the history of the same token again
        liveModel.losesTheHistoryOf(ID, TOKEN);
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(inTheWindow.get()).as("thrown by the resume inside the window").isNull();
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.lossesReported).as("losses the live model reported").containsExactly(TOKEN, TOKEN),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model, the one inside the window").containsExactly(ID),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(signals).as("catch-up signals, so no takeover replay ran").isEmpty(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty()
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_subscription_whose_resume_at_a_gone_checkpoint_lost_it_before_the_resume_returned() {
        // Given a subscription a loss paused, and a checkpoint the live model has no history back to
        terminallyPausedAfterALoss();
        liveModel.withoutHistoryBack(TOKEN);

        // When it is resumed at that checkpoint, whose loss the live model reports before the resume returns, and then resumed again
        Throwable repositioning = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN)));
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then the second resume is refused for the loss of that checkpoint and not for the earlier one
        assertThat(repositioning).as("thrown by the resume at the gone checkpoint").isNull();
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString()).hasMessageNotContaining(LATER_TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.lossesReported).as("losses the live model reported").containsExactly(LATER_TOKEN, TOKEN),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model, the one at the gone checkpoint").containsExactly(ID),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged, one for each loss").containsExactly(terminalFailureLog(ID, LATER_TOKEN), terminalFailureLog(ID, TOKEN))
        );
    }

    @Test
    void logs_that_the_error_for_a_loss_no_longer_applies_when_a_resume_at_a_kept_checkpoint_clears_it() {
        // Given a subscription a loss paused
        terminallyPausedAfterALoss();
        StartAt keptCheckpoint = StartAt.checkpoint(TOKEN);

        // When it is resumed at a checkpoint the live model has the history back to
        catchup.resumeSubscription(ID, keptCheckpoint);

        // Then the resume logs that the error for the loss no longer applies
        assertAll(
                () -> assertThat(infosLogged()).as("infos logged").containsOnlyOnce(resumedAfterLossLog(ID, keptCheckpoint, LATER_TOKEN)),
                () -> assertThat(errorsLogged()).as("errors logged, only the one for the loss").containsExactly(terminalFailureLog(ID, LATER_TOKEN))
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_subscription_whose_resume_at_a_gone_checkpoint_lost_it_and_recorded_it_after_the_resume_returned() {
        // Given a subscription a loss paused, a checkpoint the live model has no history back to, and a takeover that keeps what follows its pause
        terminallyPausedAfterALoss();
        liveModel.withoutHistoryBack(TOKEN).defersTheNextAfterPause();

        // When it is resumed at that checkpoint, what follows the pause for its loss runs only after the resume has returned, and it is resumed again
        Throwable repositioning = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(TOKEN)));
        liveModel.runsTheDeferredAfterPause();
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then the second resume is refused for the loss of that checkpoint and not for the earlier one
        assertThat(repositioning).as("thrown by the resume at the gone checkpoint").isNull();
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString()).hasMessageNotContaining(LATER_TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.lossesReported).as("losses the live model reported").containsExactly(LATER_TOKEN, TOKEN),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model, the one at the gone checkpoint").containsExactly(ID),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged, one for each loss").containsExactly(terminalFailureLog(ID, LATER_TOKEN), terminalFailureLog(ID, TOKEN))
        );
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable throwable) throws E {
        throw (E) throwable;
    }

    @Test
    void throws_CatchupCannotResumeException_from_subscribePaused_when_a_live_start_checkpoint_is_older_than_the_history_of_the_live_model() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();

        // When
        Throwable thrown = catchThrowable(() -> catchup.subscribePaused(ID, null, StartAt.checkpoint(TOKEN), action()));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.subscribePausedCalls).as("paused subscribes on the live model").isEmpty(),
                () -> assertThat(liveModel.isPaused(ID)).as("paused on the live model").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @Test
    void holds_a_lost_live_start_paused_with_one_error_when_the_hold_is_asked_for_subscribe() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();

        // When
        Throwable thrown = catchThrowable(() -> LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action())));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isNull();
        assertHeldPausedAfterALostLiveStart();
        assertThat(liveModel.subscribePausedCalls).as("paused subscribes on the live model").containsExactly(ID);
        assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").isEmpty();
    }

    @Test
    void holds_a_lost_live_start_paused_with_one_error_when_the_hold_is_asked_for_subscribePaused() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();

        // When
        Throwable thrown = catchThrowable(() -> LostStartHold.askedDuring(() -> catchup.subscribePaused(ID, null, StartAt.checkpoint(TOKEN), action())));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isNull();
        assertHeldPausedAfterALostLiveStart();
        assertThat(liveModel.subscribePausedCalls).as("paused subscribes on the live model").containsExactly(ID);
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_lost_live_start_held_paused() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();
        LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessage(HistoryLossMessages.historyLost(ID, TOKEN).getMessage());
        assertAll(
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").hasSize(1)
        );
    }

    @Test
    void runs_a_lost_live_start_held_paused_and_delivers_a_later_event_when_resumed_at_a_checkpoint_the_live_model_has_the_history_back_to() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();
        LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));
        DomainEvent later = nameDefined(LocalDateTime.now(), "later");

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN)));
        liveModel.deliver(ID, cloudEventConverter.toCloudEvents(List.of(later)).get(0));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(received).as("events delivered").containsExactly(later),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged, only the one for the loss").hasSize(1)
        );
    }

    @Test
    void a_live_start_the_live_model_has_the_history_back_to_runs_also_when_the_hold_is_asked() {
        // Given
        create();

        // When
        LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));

        // Then
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @Test
    void is_not_held_by_a_hold_asked_for_an_earlier_subscribe_that_has_returned() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();
        LostStartHold.askedDuring(() -> catchup.subscribe(OTHER_ID, StartAt.checkpoint(TOKEN), action()));

        // When
        Throwable thrown = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID);
        assertThat(catchup.isPaused(ID)).as("paused").isFalse();
    }

    static Stream<Arguments> storedCheckpointsOfAReplayThatStartedAboveTheBeginning() {
        return Stream.of(
                Arguments.of(Named.of("a position replay from position 1", new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, TOKEN)))),
                Arguments.of(Named.of("a time replay from 2026-01-01T00:00:00Z", new StringBasedCheckpoint(CatchupCheckpoints.format("2026-01-01T00:00:05Z", "2026-01-01T00:00:00Z", TOKEN)))));
    }

    @ParameterizedTest
    @MethodSource("storedCheckpointsOfAReplayThatStartedAboveTheBeginning")
    void throws_CatchupCannotResumeException_before_anything_replays_when_the_stored_live_start_of_a_replay_that_started_above_the_beginning_is_gone(Checkpoint stored) {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();

        // When
        Throwable thrown = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(stored), action()));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(signals).as("catch-up signals, so no replay ran").isEmpty(),
                () -> assertThat(saved).as("positions stored").isEmpty(),
                () -> assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").isEmpty(),
                () -> assertThat(liveModel.subscribePausedCalls).as("paused subscribes on the live model").isEmpty(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse()
        );
    }

    @ParameterizedTest
    @MethodSource("storedCheckpointsOfAReplayThatStartedAboveTheBeginning")
    void holds_the_subscription_paused_at_the_stored_live_start_with_one_error_and_refuses_a_resume_when_the_hold_is_asked_and_the_live_start_of_a_replay_that_started_above_the_beginning_is_gone(Checkpoint stored) {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();

        // When
        Throwable thrown = catchThrowable(() -> LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(stored), action())));
        Throwable resume = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isNull();
        assertThat(resume).as("thrown by the resume").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.isPaused(ID)).as("paused on the live model").isTrue(),
                () -> assertThat(liveModel.subscribePausedCalls).as("paused subscribes on the live model").containsExactly(ID),
                () -> assertThat(liveModel.subscribePausedStartAts).as("starts the live model was subscribed paused at").hasSize(1).allSatisfy(startAt -> assertThat(startAt.get(new SubscriptionModelContext(HistoryLosingLiveModel.class)))
                        .isInstanceOfSatisfying(StartAtCheckpoint.class, at -> assertThat(at.checkpoint).isEqualTo(TOKEN))),
                () -> assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").isEmpty(),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(liveStartLostLog(ID, TOKEN)),
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(signals).as("catch-up signals, so no replay ran").isEmpty(),
                () -> assertThat(storage.read(ID)).as("stored position").isNull()
        );
    }

    @Test
    void runs_a_subscription_whose_replay_lost_its_live_start_before_it_went_live_and_delivers_a_later_event_when_resumed_at_a_checkpoint_the_live_model_has_the_history_back_to() {
        // Given
        heldPausedBeforeItWentLive();
        DomainEvent later = nameDefined(LocalDateTime.now(), "later");

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN)));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        liveModel.deliver(ID, cloudEventConverter.toCloudEvents(List.of(later)).get(0));
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").containsExactly(ID),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty(),
                () -> assertThat(received).as("events delivered, the history after position 1 and then the later event").containsExactly(history.get(1), history.get(2), later),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(liveStartLostLog(ID, TOKEN))
        );
    }

    @Test
    void a_pause_and_resume_does_not_throw_the_terminal_failure_once_a_subscription_whose_replay_lost_its_live_start_before_it_went_live_is_resumed_at_a_kept_checkpoint() {
        // Given
        heldPausedBeforeItWentLive();
        catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN));
        catchup.pauseSubscription(ID);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").containsExactly(ID),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(liveStartLostLog(ID, TOKEN))
        );
    }

    @Test
    void a_pause_and_resume_does_not_throw_the_terminal_failure_once_a_subscription_whose_replay_lost_its_live_start_before_it_went_live_is_resumed_at_a_kept_checkpoint_a_dynamic_start_answers() {
        // Given
        heldPausedBeforeItWentLive();
        catchup.resumeSubscription(ID, StartAt.dynamic(context -> StartAt.checkpoint(LATER_TOKEN)));
        catchup.pauseSubscription(ID);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").containsExactly(ID),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(liveStartLostLog(ID, TOKEN))
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_replay_held_before_it_went_live_after_a_resume_at_a_checkpoint_whose_live_start_is_gone_held_it_paused_again() {
        // Given
        heldPausedBeforeItWentLive();
        Checkpoint lostLiveStart = new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, TOKEN));

        // When
        LostStartHold.Answer<Subscription> resumedAt = LostStartHold.askedDuringCall(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(lostLiveStart)));
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(resumedAt.heldPaused()).as("held paused by the resume").isTrue();
        assertThat(thrown).as("thrown by the resume").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty(),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty()
        );
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_replay_held_before_it_went_live_after_a_resume_at_a_position_threw_from_the_live_model() {
        // Given
        heldPausedBeforeItWentLive();
        liveModel.failingTheNextSubscribeWith(new IllegalStateException("live model down"));

        // When
        Throwable resumedAt = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN)));
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(resumedAt).as("thrown by the resume at a position").isInstanceOf(IllegalStateException.class).hasMessage("live model down");
        assertThat(thrown).as("thrown by the resume").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty()
        );
    }

    @Test
    void stores_the_present_position_of_the_live_model_when_a_replay_held_before_it_went_live_is_resumed_at_now() {
        // Given
        heldPausedBeforeItWentLive();

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        Checkpoint stored = storage.read(ID);
        assertThat(stored).as("stored position").isNotNull();
        assertAll(
                () -> assertThat(stored.asString()).as("stored position, the one the live model answered").startsWith("live-start-"),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue()
        );
    }

    @Test
    void takes_the_present_position_of_a_replay_held_before_it_went_live_and_resumed_at_now_from_the_event_store_when_the_live_model_cannot_answer() {
        // Given
        String reportedByTheEventStore = "event-store-live-start";
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel) {
            @Override
            public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
                return new ForwardingCatchupReadScope(super.openCatchupReadScope(notBefore)) {
                    @Override
                    public @Nullable String liveStart() {
                        return reportedByTheEventStore;
                    }
                };
            }
        };
        history.forEach(this::write);
        heldPausedBeforeItWentLive();
        liveModel.answeringNothingForTheCurrentPosition();

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(storage.read(ID)).as("stored position, the live start the event store reported").isEqualTo(new StringBasedCheckpoint(reportedByTheEventStore)),
                () -> assertThat(liveModel.startOf(ID)).as("start the live model was subscribed at").isInstanceOfSatisfying(StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint).as("position the live model was subscribed at").isEqualTo(new StringBasedCheckpoint(reportedByTheEventStore))),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue()
        );
    }

    @Test
    void keeps_the_position_a_resume_at_a_position_stored_when_nothing_was_stored_before_and_the_live_model_throws_from_the_subscribe_in_place() {
        // Given
        heldPausedBeforeItWentLive();
        storage.delete(ID);
        liveModel.failingTheNextSubscribeWith(new IllegalStateException("live model down"));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN)));

        // Then
        assertThat(thrown).as("thrown by the resume").isInstanceOf(IllegalStateException.class).hasMessage("live model down");
        assertThat(storage.read(ID)).as("stored position").isEqualTo(LATER_TOKEN);
    }

    @Test
    void puts_the_position_stored_before_back_with_the_write_condition_of_the_replacing_write_when_the_live_model_throws_from_the_subscribe_in_place() {
        // Given
        AtomicLong writeVersion = new AtomicLong(5);
        heldPausedBeforeItWentLive(writeVersion);
        Checkpoint storedBefore = storage.read(ID);
        assertThat(storage.writeVersion(ID)).as("version stored by the held replay").hasValue(5);
        liveModel.beforeTheNextSubscribe(() -> writeVersion.set(7)).failingTheNextSubscribeWith(new IllegalStateException("live model down"));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN)));

        // Then
        assertThat(thrown).as("thrown by the resume").isInstanceOf(IllegalStateException.class).hasMessage("live model down");
        assertAll(
                () -> assertThat(storedBefore).as("position stored before the resume").isNotNull(),
                () -> assertThat(storage.read(ID)).as("stored position").isEqualTo(storedBefore),
                () -> assertThat(storage.writeVersion(ID)).as("version stored, the one the replacing write had").hasValue(5)
        );
    }

    @Test
    void hands_the_live_model_the_stored_live_start_when_a_lost_one_is_held_paused_for_a_dynamic_start_and_a_start_that_answers_null_for_a_wrapper_still_does() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();
        Checkpoint stored = new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, TOKEN));
        StartAt dynamic = StartAt.dynamic(context -> context.subscriptionModelType() == StreamCatchupSubscriptionModel.class ? StartAt.checkpoint(stored)
                : SubscriptionModelWrapper.class.isAssignableFrom(context.subscriptionModelType()) ? null : StartAt.now());

        // When
        Throwable thrown = catchThrowable(() -> LostStartHold.askedDuring(() -> catchup.subscribe(ID, dynamic, action())));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isNull();
        assertThat(liveModel.subscribePausedStartAts).as("starts the live model was subscribed paused at").hasSize(1);
        StartAt heldAt = liveModel.subscribePausedStartAts.get(0);
        assertAll(
                () -> assertThat(heldAt.get(new SubscriptionModelContext(WrapperLikeDurableModel.class))).as("start for a wrapper").isNull(),
                () -> assertThat(heldAt.get(new SubscriptionModelContext(HistoryLosingLiveModel.class))).as("start for the live model").isInstanceOfSatisfying(StartAtCheckpoint.class,
                        at -> assertThat(at.checkpoint).isEqualTo(TOKEN))
        );
    }

    @Test
    void delivers_an_event_committed_between_the_store_of_the_present_position_and_the_subscribe_of_the_live_model_when_a_replay_held_before_it_went_live_is_resumed_at_now() {
        // Given
        heldPausedBeforeItWentLive();
        received.clear();
        NameDefined committedInTheWindow = nameDefined(LocalDateTime.now(), "committed-in-the-window");
        liveModel.beforeTheNextSubscribe(() -> liveModel.commits(cloudEventConverter.toCloudEvents(List.of(committedInTheWindow)).get(0)));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(storage.read(ID)).as("stored position").isNotNull().extracting(Checkpoint::asString).asString().startsWith("live-start-"),
                () -> assertThat(received).as("events delivered").containsExactly(committedInTheWindow)
        );
    }

    @Test
    void subscribes_the_live_model_from_the_present_position_it_stored_for_a_dynamic_start_that_answers_now_for_the_catch_up_model_and_null_for_a_wrapper() {
        // Given
        heldPausedBeforeItWentLive();
        StartAt dynamic = StartAt.dynamic(context -> context.subscriptionModelType() == StreamCatchupSubscriptionModel.class ? StartAt.now()
                : SubscriptionModelWrapper.class.isAssignableFrom(context.subscriptionModelType()) ? null : StartAt.now());

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, dynamic));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        Checkpoint stored = storage.read(ID);
        assertThat(stored).as("stored position").isNotNull();
        StartAt subscribedAt = liveModel.startOf(ID);
        assertAll(
                () -> assertThat(subscribedAt.get(new SubscriptionModelContext(WrapperLikeDurableModel.class))).as("start for a wrapper").isNull(),
                () -> assertThat(subscribedAt.get(new SubscriptionModelContext(HistoryLosingLiveModel.class))).as("start for the live model").isInstanceOfSatisfying(StartAtCheckpoint.class,
                        at -> assertThat(at.checkpoint).isEqualTo(stored))
        );
    }

    @Test
    void subscribes_the_live_model_from_now_when_a_replay_held_before_it_went_live_is_resumed_at_now_and_no_checkpoint_storage_is_used() {
        // Given
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, new CatchupSubscriptionModelConfig(100));
        Checkpoint start = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, TOKEN));
        Subscription subscribed = catchup.subscribe(ID, StartAt.checkpoint(start), cloudEvent -> liveModel.withoutHistoryBack(TOKEN));
        assertThat(catchThrowable(() -> subscribed.waitUntilStarted(Duration.ofSeconds(10)))).as("thrown by waiting for the subscription to start")
                .isInstanceOf(CatchupCannotResumeException.class);
        assertThat(catchup.isPaused(ID)).as("held paused").isTrue();

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(liveModel.startOf(ID).isNow()).as("live model subscribed from now").isTrue(),
                () -> assertThat(saved).as("positions stored").isEmpty()
        );
    }

    @Test
    void refuses_a_resume_of_a_stored_catch_up_position_held_paused_for_its_lost_live_start_without_naming_that_live_start_as_where_it_stopped() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        create();
        Checkpoint stored = new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, TOKEN));
        LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(stored), action()));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isInstanceOf(CatchupCannotResumeException.class)
                .hasMessage(HistoryLossMessages.liveStartLostOnResume(ID, TOKEN).getMessage())
                .hasMessageNotContaining("where it stopped");
        assertThat(errorsLogged()).as("errors logged").containsExactly(liveStartLostLog(ID, TOKEN));
    }

    @Test
    void a_start_refusal_listener_told_that_a_replay_is_held_for_a_lost_live_start_resumes_it_at_a_checkpoint_the_live_model_has_the_history_back_to() {
        // Given
        create();
        AtomicReference<Subscription> resumedByTheListener = new AtomicReference<>();
        AtomicReference<Throwable> thrownByTheResumeOfTheListener = new AtomicReference<>();
        AtomicBoolean told = new AtomicBoolean();
        catchup.addStartRefusalListener(new StartRefusalListener() {
            @Override
            public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
                throw new AssertionError("A catch-up subscription model was not expected to refuse the start of " + subscriptionId, refusal);
            }

            @Override
            public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
                try {
                    resumedByTheListener.set(catchup.resumeSubscription(subscriptionId, StartAt.checkpoint(LATER_TOKEN)));
                } catch (Throwable e) {
                    thrownByTheResumeOfTheListener.set(e);
                } finally {
                    told.set(true);
                }
            }
        });
        DomainEvent later = nameDefined(LocalDateTime.now(), "later");

        // When
        subscribeAboveTheBeginningWithAnActionThatAgesOutTheLiveStart();
        await().untilTrue(told);

        // Then
        assertThat(thrownByTheResumeOfTheListener.get()).as("thrown by the resume the listener made").isNull();
        liveModel.deliver(ID, cloudEventConverter.toCloudEvents(List.of(later)).get(0));
        assertAll(
                () -> assertThat(resumedByTheListener.get()).as("subscription the resume returned").isNotNull().extracting(Subscription::id).isEqualTo(ID),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(received).as("events delivered, the history after position 1 and then the later event").containsExactly(history.get(1), history.get(2), later)
        );
    }

    @Test
    void takes_the_present_position_of_a_replay_held_before_it_went_live_and_resumed_at_now_from_the_event_store_before_the_live_model_when_both_answer_one() {
        // Given
        StringBasedCheckpoint reportedByTheEventStore = new StringBasedCheckpoint("event-store-live-start");
        eventStore = eventStoreReportingTheLiveStart(reportedByTheEventStore.asString());
        history.forEach(this::write);
        heldPausedBeforeItWentLive();
        assertThat(liveModel.globalCheckpoint()).as("current position the live model answers").isNotNull().isNotEqualTo(reportedByTheEventStore);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(storage.read(ID)).as("stored position, the live start the event store reported").isEqualTo(reportedByTheEventStore),
                () -> assertThat(liveModel.startOf(ID)).as("start the live model was subscribed at").isInstanceOfSatisfying(StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint).as("position the live model was subscribed at").isEqualTo(reportedByTheEventStore))
        );
    }

    @Test
    void throws_IllegalStateException_and_changes_nothing_when_a_replay_held_before_it_went_live_is_resumed_at_now_and_neither_the_event_store_nor_the_live_model_answers_a_present_position() {
        // Given
        eventStore = eventStoreReportingTheLiveStart(null);
        history.forEach(this::write);
        heldPausedBeforeItWentLive();
        liveModel.answeringNothingForTheCurrentPosition();
        Checkpoint storedBefore = storage.read(ID);
        List<String> savedBefore = List.copyOf(saved);

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.now()));

        // Then
        assertThat(thrown).as("thrown by the resume").isInstanceOf(IllegalStateException.class).hasMessageContaining(ID).hasMessageContaining("answered nothing when asked for the current position");
        assertAll(
                () -> assertThat(storedBefore).as("position stored before the resume").isNotNull(),
                () -> assertThat(storage.read(ID)).as("stored position").isEqualTo(storedBefore),
                () -> assertThat(saved).as("positions stored").isEqualTo(savedBefore),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("replay held").isTrue(),
                () -> assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").isEmpty()
        );
    }

    @Test
    void is_held_paused_for_the_refusal_it_reported_while_the_replay_is_held_for_a_lost_live_start() {
        // Given
        CatchupCannotResumeException reported = heldPausedBeforeItWentLiveAndReported();

        // When
        boolean heldPaused = catchup.isHeldPausedFor(ID, reported);

        // Then
        assertThat(heldPaused).as("held paused for the refusal reported").isTrue();
    }

    @Test
    void is_not_held_paused_for_a_refusal_other_than_the_one_it_reported() {
        // Given
        heldPausedBeforeItWentLiveAndReported();

        // When
        boolean heldPaused = catchup.isHeldPausedFor(ID, HistoryLossMessages.liveStartLostOnResume(ID, TOKEN));

        // Then
        assertThat(heldPaused).as("held paused for another refusal").isFalse();
    }

    @Test
    void is_no_longer_held_paused_for_the_refusal_it_reported_once_the_subscription_is_resumed_at_a_checkpoint() {
        // Given
        CatchupCannotResumeException reported = heldPausedBeforeItWentLiveAndReported();
        catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN));

        // When
        boolean heldPaused = catchup.isHeldPausedFor(ID, reported);

        // Then
        assertThat(heldPaused).as("held paused for the refusal reported").isFalse();
    }

    @Test
    void is_no_longer_held_paused_for_the_refusal_it_reported_once_the_subscription_is_cancelled() {
        // Given
        CatchupCannotResumeException reported = heldPausedBeforeItWentLiveAndReported();
        catchup.cancelSubscription(ID);

        // When
        boolean heldPaused = catchup.isHeldPausedFor(ID, reported);

        // Then
        assertThat(heldPaused).as("held paused for the refusal reported").isFalse();
    }

    @Test
    void a_removed_start_refusal_listener_is_not_told_that_a_replay_is_held_for_a_lost_live_start() {
        // Given
        create();
        HeldPausedReports removed = new HeldPausedReports();
        HeldPausedReports kept = new HeldPausedReports();
        catchup.addStartRefusalListener(removed);
        catchup.addStartRefusalListener(kept);
        catchup.removeStartRefusalListener(removed);

        // When
        subscribeAboveTheBeginningWithAnActionThatAgesOutTheLiveStart();

        // Then
        await().untilAsserted(() -> assertThat(kept.heldPaused).as("held paused reports to the listener kept").hasSize(1));
        assertThat(removed.heldPaused).as("held paused reports to the listener removed").isEmpty();
    }

    @Test
    void removing_a_start_refusal_listener_removes_it_from_a_live_model_that_reports_start_refusals() {
        // Given
        RefusalReportingLiveModel reportingLiveModel = new RefusalReportingLiveModel(inMemorySubscriptionModel);
        catchup = new StreamCatchupSubscriptionModel(reportingLiveModel, eventStore, new CatchupSubscriptionModelConfig(100));
        HeldPausedReports listener = new HeldPausedReports();
        catchup.addStartRefusalListener(listener);
        assertThat(reportingLiveModel.listeners).as("listeners the live model was given").containsExactly(listener);

        // When
        catchup.removeStartRefusalListener(listener);

        // Then
        assertThat(reportingLiveModel.listeners).as("listeners the live model keeps").isEmpty();
    }

    // Held as heldPausedBeforeItWentLive holds it, with the refusal the catch-up reported to a start refusal listener
    private CatchupCannotResumeException heldPausedBeforeItWentLiveAndReported() {
        create();
        HeldPausedReports reports = new HeldPausedReports();
        catchup.addStartRefusalListener(reports);
        subscribeAboveTheBeginningWithAnActionThatAgesOutTheLiveStart();
        await().untilAsserted(() -> assertThat(reports.heldPaused).as("held paused reports").hasSize(1));
        assertThat(catchup.isPaused(ID)).as("held paused").isTrue();
        return reports.heldPaused.get(0);
    }

    private InMemoryEventStore eventStoreReportingTheLiveStart(@Nullable String liveStart) {
        return new InMemoryEventStore(inMemorySubscriptionModel) {
            @Override
            public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
                return new ForwardingCatchupReadScope(super.openCatchupReadScope(notBefore)) {
                    @Override
                    public @Nullable String liveStart() {
                        return liveStart;
                    }
                };
            }
        };
    }

    // A live model that keeps the start refusal listeners it is given, and delegates everything else
    private static final class RefusalReportingLiveModel implements CheckpointAwareSubscriptionModel, StartRefusalReportingSubscriptions {
        private final InMemorySubscriptionModel delegate;
        private final List<StartRefusalListener> listeners = new CopyOnWriteArrayList<>();

        private RefusalReportingLiveModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public void addStartRefusalListener(StartRefusalListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeStartRefusalListener(StartRefusalListener listener) {
            listeners.remove(listener);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, StartAt.now(), action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("live start read now");
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }
    }

    private abstract static class WrapperLikeDurableModel implements SubscriptionModelWrapper {
    }

    // A replay a subscribe started above the beginning, whose action ages out the live start the replay would go live
    // from, so the live model never gets the subscription and the loss is recorded
    private void heldPausedBeforeItWentLive() {
        heldPausedBeforeItWentLive(useCheckpointStorage(storage));
    }

    // As above, with every checkpoint write stamped with the version writeVersion holds when it is written
    private void heldPausedBeforeItWentLive(AtomicLong writeVersion) {
        heldPausedBeforeItWentLive(useCheckpointStorage(storage, id -> OptionalLong.of(writeVersion.get())));
    }

    private void heldPausedBeforeItWentLive(CheckpointStorageConfig.UseCheckpointInStorage checkpointStorage) {
        create(checkpointStorage);
        Subscription subscribed = subscribeAboveTheBeginningWithAnActionThatAgesOutTheLiveStart();
        assertThat(catchThrowable(() -> subscribed.waitUntilStarted(Duration.ofSeconds(10)))).as("thrown by waiting for the subscription to start")
                .isInstanceOf(CatchupCannotResumeException.class);
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("held paused").isTrue(),
                () -> assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").isEmpty(),
                () -> assertThat(catchThrowable(() -> catchup.resumeSubscription(ID))).as("thrown by a resume").isInstanceOf(CatchupCannotResumeException.class)
                        .hasMessage(HistoryLossMessages.liveStartLostOnResume(ID, TOKEN).getMessage())
        );
    }

    // The replay starts at position 1 and goes live from TOKEN, which its action ages out, so the replay is held for the
    // lost live start on its own thread once it has delivered the history
    private Subscription subscribeAboveTheBeginningWithAnActionThatAgesOutTheLiveStart() {
        Checkpoint stored = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, TOKEN));
        return catchup.subscribe(ID, StartAt.checkpoint(stored), cloudEvent -> {
            liveModel.withoutHistoryBack(TOKEN);
            received.add(cloudEventConverter.toDomainEvent(cloudEvent));
        });
    }

    private void assertHeldPausedAfterALostLiveStart() {
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(liveModel.isPaused(ID)).as("paused on the live model").isTrue(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(ID, TOKEN)),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(signals).as("catch-up signals, so no replay ran").isEmpty(),
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(saved).as("positions stored").isEmpty(),
                () -> assertThat(storage.read(ID)).as("stored position").isNull()
        );
    }

    private void terminallyPausedAfterALoss() {
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);
        assertThat(catchup.isPaused(ID)).as("paused by the loss").isTrue();
    }

    private void terminallyFailedAndHealthyPausedSubscriptions() {
        create();
        catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action());
        catchup.subscribe(OTHER_ID, StartAt.checkpoint(TOKEN), action());
        liveModel.losesTheHistoryOf(ID, LATER_TOKEN);
        catchup.stop();
        assertThat(liveModel.isPaused(OTHER_ID)).as("healthy subscription paused by the stop").isTrue();
    }

    private void subscribeFromTheBeginningAndAwaitTheHandOver() {
        create();
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        await().untilAsserted(() -> assertThat(liveModel.isRunning(ID)).as("handed over to the live model").isTrue());
        await().untilAsserted(() -> assertThat(received).as("history replayed").containsExactlyElementsOf(history));
    }

    private void create() {
        create(useCheckpointStorage(storage));
    }

    private void create(CheckpointStorageConfig.UseCheckpointInStorage checkpointStorage) {
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, checkpointStorage.andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, config);
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                signals.add("catchupStarted");
            }

            @Override
            public void historyRead(Object episode) {
                signals.add("historyRead");
            }
        });
    }

    private Consumer<CloudEvent> action() {
        return cloudEvent -> received.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private List<String> errorsLogged() {
        return logged.events.stream().filter(event -> event.getLevel().equals(Level.ERROR)).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> infosLogged() {
        return logged.events.stream().filter(event -> event.getLevel().equals(Level.INFO)).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private static String resumedAfterLossLog(String subscriptionId, StartAt resumedAt, Checkpoint lostFrom) {
        return MessageFormatter.arrayFormat(HistoryLossMessages.RESUMED_AFTER_LOSS_LOG, new Object[]{subscriptionId, resumedAt, lostFrom.asString()}).getMessage();
    }

    private static String terminalFailureLog(String subscriptionId, Checkpoint lostFrom) {
        String lost = lostFrom.asString();
        return MessageFormatter.arrayFormat(HistoryLossMessages.TERMINAL_FAILURE_LOG, new Object[]{subscriptionId, lost, lost}).getMessage();
    }

    private static String liveStartLostLog(String subscriptionId, Checkpoint liveStart) {
        String lost = liveStart.asString();
        return MessageFormatter.arrayFormat(HistoryLossMessages.LIVE_START_LOST_LOG, new Object[]{subscriptionId, lost, lost}).getMessage();
    }

    // Records each refusal it is told of a subscription held paused, and refuses to be told of any other refusal
    private static final class HeldPausedReports implements StartRefusalListener {
        private final List<CatchupCannotResumeException> heldPaused = new CopyOnWriteArrayList<>();

        @Override
        public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            throw new AssertionError("A catch-up subscription model was not expected to refuse the start of " + subscriptionId, refusal);
        }

        @Override
        public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
            heldPaused.add(refusal);
        }
    }

    private static final class LoggedEvents extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }
    }

    private static NameDefined nameDefined(LocalDateTime time, String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }
}
