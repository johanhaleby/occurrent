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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.internal.LostStartHold;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A live start the live model has no history back to is refused by the live branch of the DCB catch-up, before anything
 * is subscribed or stored.
 */
@DisplayName("DcbCatchupHistoryLossOwnership")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DcbCatchupHistoryLossOwnershipTest {

    private static final String ID = "subscription";
    private static final Checkpoint TOKEN = new StringBasedCheckpoint("token-1");
    private static final Checkpoint LATER_TOKEN = new StringBasedCheckpoint("later-token");

    private final Logger catchupLogger = (Logger) LoggerFactory.getLogger("org.occurrent.subscription.blocking.durable.catchup");
    private final LoggedEvents logged = new LoggedEvents();
    private final CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private InMemoryCheckpointStorage storage;
    private HistoryLosingLiveModel liveModel;
    private DcbCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                saved.add(id + "=" + checkpoint.asString());
                return super.save(id, checkpoint, condition);
            }
        };
        liveModel = new HistoryLosingLiveModel();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchup = new DcbCatchupSubscriptionModel(liveModel, new InMemoryEventStore(inMemorySubscriptionModel), DcbCriteria.tags(Tag.parse("name:1")), config);
        logged.start();
        catchupLogger.addAppender(logged);
    }

    @AfterEach
    void shutdown() {
        catchupLogger.detachAppender(logged);
        catchup.shutdown();
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void throws_CatchupCannotResumeException_when_a_live_start_checkpoint_is_older_than_the_history_of_the_live_model() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);

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
        assertThatThrownBy(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action())).isInstanceOf(CatchupCannotResumeException.class);

        // When
        catchup.subscribe(ID, StartAt.checkpoint(LATER_TOKEN), action());

        // Then
        assertThat(catchup.isRunning(ID)).as("running").isTrue();
    }

    @Test
    void throws_CatchupCannotResumeException_from_subscribePaused_when_a_live_start_checkpoint_is_older_than_the_history_of_the_live_model() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);

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

        // When
        Throwable thrown = catchThrowable(() -> LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action())));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isNull();
        assertHeldPausedAfterALostLiveStart();
        assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").isEmpty();
    }

    @Test
    void holds_a_lost_live_start_paused_with_one_error_when_the_hold_is_asked_for_subscribePaused() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);

        // When
        Throwable thrown = catchThrowable(() -> LostStartHold.askedDuring(() -> catchup.subscribePaused(ID, null, StartAt.checkpoint(TOKEN), action())));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isNull();
        assertHeldPausedAfterALostLiveStart();
    }

    @Test
    void throws_CatchupCannotResumeException_when_resuming_a_lost_live_start_held_paused() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(errorsLogged()).as("errors logged").hasSize(1)
        );
    }

    @Test
    void runs_a_lost_live_start_held_paused_and_delivers_a_later_event_when_resumed_at_a_checkpoint_the_live_model_has_the_history_back_to() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(TOKEN), action()));
        CloudEvent later = DcbCloudEvents.withTags(CloudEventBuilder.v1().withId("later").withSource(URI.create("urn:test")).withType("test").build(), List.of(Tag.parse("name:1")));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN)));
        liveModel.deliver(ID, later);

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
    void a_resume_at_a_time_after_the_beginning_does_not_store_a_marked_time_over_the_stored_dcb_position_so_a_later_resume_does_not_throw() {
        // Given
        catchup.subscribe(ID, StartAt.now(), action());
        catchup.pauseSubscription(ID);
        storage.save(ID, GlobalCheckpoint.of(5));
        Checkpoint timeAfterTheBeginning = TimeBasedCheckpoint.from(OffsetDateTime.parse("2026-10-01T12:00:00Z"));

        // When
        catchup.resumeSubscription(ID, StartAt.checkpoint(timeAfterTheBeginning));

        // Then
        assertThat(CatchupCheckpoints.isMarkedTime(storage.read(ID))).as("the stored position is a marked time").isFalse();
        catchup.pauseSubscription(ID);
        assertThatCode(() -> catchup.resumeSubscription(ID)).as("a plain resume after a pause").doesNotThrowAnyException();
    }

    @Test
    void throws_CatchupCannotResumeException_before_anything_replays_when_the_stored_live_start_of_a_replay_that_started_above_the_beginning_is_gone() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        Checkpoint stored = new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, TOKEN));

        // When
        Throwable thrown = catchThrowable(() -> catchup.subscribe(ID, StartAt.checkpoint(stored), action()));

        // Then
        assertThat(thrown).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(ID).hasMessageContaining(TOKEN.asString());
        assertAll(
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(saved).as("positions stored").isEmpty(),
                () -> assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").isEmpty(),
                () -> assertThat(liveModel.subscribePausedCalls).as("paused subscribes on the live model").isEmpty(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse()
        );
    }

    @Test
    void holds_the_subscription_paused_at_the_stored_live_start_with_one_error_and_refuses_a_resume_when_the_hold_is_asked_and_the_live_start_of_a_replay_that_started_above_the_beginning_is_gone() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        Checkpoint stored = new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, TOKEN));

        // When
        Throwable thrown = catchThrowable(() -> LostStartHold.askedDuring(() -> catchup.subscribe(ID, StartAt.checkpoint(stored), action())));
        Throwable resume = catchThrowable(() -> catchup.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isNull();
        assertThat(resume).as("thrown by the resume").isInstanceOf(CatchupCannotResumeException.class).hasMessage(HistoryLossMessages.liveStartLostOnResume(ID, TOKEN).getMessage())
                .hasMessageNotContaining("where it stopped");
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.isPaused(ID)).as("paused on the live model").isTrue(),
                () -> assertThat(liveModel.subscribePausedCalls).as("paused subscribes on the live model").containsExactly(ID),
                () -> assertThat(liveModel.subscribePausedStartAts).as("starts the live model was subscribed paused at").hasSize(1).allSatisfy(startAt -> assertThat(startAt.get(new SubscriptionModelContext(HistoryLosingLiveModel.class)))
                        .isInstanceOfSatisfying(StartAtCheckpoint.class, at -> assertThat(at.checkpoint).isEqualTo(TOKEN))),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(liveStartLostLog(ID, TOKEN)),
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(storage.read(ID)).as("stored position").isNull()
        );
    }

    @Test
    void hands_the_live_model_the_stored_live_start_when_a_lost_one_is_held_paused_for_a_dynamic_start_and_a_start_that_answers_null_for_a_wrapper_still_does() {
        // Given
        liveModel.withoutHistoryBack(TOKEN);
        Checkpoint stored = new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, TOKEN));
        StartAt dynamic = StartAt.dynamic(context -> context.subscriptionModelType() == DcbCatchupSubscriptionModel.class ? StartAt.checkpoint(stored)
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

    private abstract static class WrapperLikeDurableModel implements SubscriptionModelWrapper {
    }

    private void assertHeldPausedAfterALostLiveStart() {
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(liveModel.isPaused(ID)).as("paused on the live model").isTrue(),
                () -> assertThat(liveModel.subscribePausedCalls).as("paused subscribes on the live model").containsExactly(ID),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(ID, TOKEN)),
                () -> assertThat(liveModel.restartedFromNow).as("restarts from now").isEmpty(),
                () -> assertThat(received).as("events delivered").isEmpty(),
                () -> assertThat(saved).as("positions stored").isEmpty(),
                () -> assertThat(storage.read(ID)).as("stored position").isNull()
        );
    }

    private Consumer<CloudEvent> action() {
        return received::add;
    }

    private List<String> errorsLogged() {
        return logged.events.stream().filter(event -> event.getLevel().equals(Level.ERROR)).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private static String terminalFailureLog(String subscriptionId, Checkpoint lostFrom) {
        String lost = lostFrom.asString();
        return MessageFormatter.arrayFormat(HistoryLossMessages.TERMINAL_FAILURE_LOG, new Object[]{subscriptionId, lost, lost}).getMessage();
    }

    private static String liveStartLostLog(String subscriptionId, Checkpoint liveStart) {
        String lost = liveStart.asString();
        return MessageFormatter.arrayFormat(HistoryLossMessages.LIVE_START_LOST_LOG, new Object[]{subscriptionId, lost, lost}).getMessage();
    }

    private static final class LoggedEvents extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }
    }
}
