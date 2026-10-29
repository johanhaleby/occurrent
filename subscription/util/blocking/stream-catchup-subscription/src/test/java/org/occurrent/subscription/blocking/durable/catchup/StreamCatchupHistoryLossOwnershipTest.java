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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

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
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(LATER_TOKEN.asString());
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
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
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

    private static String terminalFailureLog(String subscriptionId, Checkpoint lostFrom) {
        String lost = lostFrom.asString();
        return MessageFormatter.arrayFormat(HistoryLossMessages.TERMINAL_FAILURE_LOG, new Object[]{subscriptionId, lost, lost}).getMessage();
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
