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

package org.occurrent.subscription.blocking.competingconsumers;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.internal.LostStartHold;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A wrapped model that refuses to start or resume a consumer with {@link CatchupCannotResumeException} keeps refusing
 * until the user acts, so the consumer is held paused instead of being put back for the next grant to try again. A
 * subscribe that asks {@link LostStartHold} to hold a lost start returns instead of throwing, and the wrapped model
 * never sees the hold asked, since it would hold the start itself and not refuse.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerHoldsARefusedStartPausedTest {

    private static final String NODE = "node";
    private static final String ID = "subscription";
    private static final Duration NO_FURTHER_CALLS_FOR = Duration.ofMillis(600);

    private final Logger modelLogger = (Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);
    private final LoggedEvents logged = new LoggedEvents();
    private WrappedModel wrapped;
    private Strategy strategy;
    private CompetingConsumerSubscriptionModel model;

    @BeforeEach
    void create_instances() {
        wrapped = new WrappedModel();
        strategy = new Strategy();
        model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        logged.start();
        modelLogger.addAppender(logged);
    }

    @AfterEach
    void shutdown() {
        modelLogger.detachAppender(logged);
        model.shutdown();
    }

    @ParameterizedTest(name = "hold asked at the subscribe = {0}")
    @ValueSource(booleans = {true, false})
    void holds_a_waiting_consumer_paused_with_one_error_and_stops_calling_the_wrapped_model_when_a_grant_is_refused(boolean holdAsked) {
        // Given
        subscribeWaiting(holdAsked);
        wrapped.refusing = true;
        int callsBefore = wrapped.calls.get();

        // When
        Throwable thrown = grantTheLease();

        // Then
        assertThat(thrown).as("thrown by the grant to the strategy").isNull();
        assertHeldPaused();
        assertThat(wrapped.calls.get()).as("calls into the wrapped model, the refused one only").isEqualTo(callsBefore + 1);
        int callsAfterTheRefusal = wrapped.calls.get();
        grantTheLease();
        grantTheLease();
        model.onConsumeProhibited(ID, NODE);
        await().during(NO_FURTHER_CALLS_FOR).atMost(NO_FURTHER_CALLS_FOR.plusSeconds(5)).untilAsserted(() ->
                assertThat(wrapped.calls.get()).as("calls into the wrapped model after the refusal").isEqualTo(callsAfterTheRefusal));
        assertThat(errorsLogged()).as("errors logged").hasSize(1);
    }

    @Test
    void holds_a_consumer_the_system_paused_paused_with_one_error_when_its_resume_on_a_grant_is_refused() {
        // Given
        strategy.grantOnRegister = true;
        subscribe(false);
        strategy.holders.remove(ID);
        model.onConsumeProhibited(ID, NODE);
        assertThat(wrapped.isPaused(ID)).as("paused in the wrapped model by the lease loss").isTrue();
        wrapped.refusing = true;
        int callsBefore = wrapped.calls.get();

        // When
        Throwable thrown = grantTheLease();

        // Then
        assertThat(thrown).as("thrown by the grant to the strategy").isNull();
        assertHeldPaused();
        int callsAfterTheRefusal = wrapped.calls.get();
        assertThat(callsAfterTheRefusal).as("calls into the wrapped model, the refused one only").isEqualTo(callsBefore + 1);
        grantTheLease();
        await().during(NO_FURTHER_CALLS_FOR).atMost(NO_FURTHER_CALLS_FOR.plusSeconds(5)).untilAsserted(() ->
                assertThat(wrapped.calls.get()).as("calls into the wrapped model after the refusal").isEqualTo(callsAfterTheRefusal));
        assertThat(errorsLogged()).as("errors logged").hasSize(1);
    }

    @Test
    void competes_again_when_the_user_resumes_a_held_consumer_and_is_held_again_while_the_wrapped_model_still_refuses() {
        // Given
        subscribeWaiting(false);
        wrapped.refusing = true;
        grantTheLease();
        assertHeldPaused();
        strategy.grantOnRegister = true;

        // When
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isInstanceOf(CatchupCannotResumeException.class);
        assertHeldPaused();
        assertThat(errorsLogged()).as("errors logged, one for each refusal").hasSize(2);
    }

    @Test
    void runs_a_held_consumer_when_the_user_resumes_it_once_the_wrapped_model_stops_refusing() {
        // Given
        subscribeWaiting(false);
        wrapped.refusing = true;
        grantTheLease();
        assertHeldPaused();
        wrapped.refusing = false;
        strategy.grantOnRegister = true;

        // When
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(model.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(wrapped.isRunning(ID)).as("running in the wrapped model").isTrue(),
                () -> assertThat(strategy.registrations).as("registered with the strategy").contains(ID),
                () -> assertThat(errorsLogged()).as("errors logged, only the one for the refusal").hasSize(1)
        );
    }

    @Test
    void returns_from_a_subscribe_that_asks_for_the_hold_and_holds_the_consumer_paused_with_one_error_when_the_wrapped_model_refuses_it() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.refusing = true;

        // When
        Throwable thrown = catchThrowable(() -> subscribe(true));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isNull();
        assertHeldPaused();
        assertThat(wrapped.calls.get()).as("calls into the wrapped model").isPositive();
        assertThat(wrapped.askedDuringACall).as("calls into the wrapped model made while the hold was asked").isEmpty();
    }

    @Test
    void throws_from_a_subscribe_that_does_not_ask_for_the_hold_and_leaves_nothing_so_the_id_subscribes_again_when_the_wrapped_model_refuses_it() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.refusing = true;

        // When
        Throwable thrown = catchThrowable(() -> subscribe(false));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isInstanceOf(CatchupCannotResumeException.class);
        assertAll(
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(strategy.registrations).as("registered with the strategy").isEmpty(),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty()
        );
        wrapped.refusing = false;
        assertThatCode(() -> subscribe(false)).as("subscribing the id again").doesNotThrowAnyException();
        assertThat(model.isRunning(ID)).as("running").isTrue();
    }

    @Test
    void the_wrapped_model_never_sees_the_hold_asked_by_a_caller_that_asks_for_it() {
        // Given
        strategy.grantOnRegister = true;

        // When
        LostStartHold.askedDuring(() -> {
            subscribe(false);
            model.pauseSubscription(ID);
            model.resumeSubscription(ID);
            model.cancelSubscription(ID);
            strategy.grantOnRegister = false;
            subscribe(false);
            strategy.holders.add(ID);
            model.onConsumeGranted(ID, NODE);
        });

        // Then
        assertThat(wrapped.calls.get()).as("calls into the wrapped model").isPositive();
        assertAll(
                () -> assertThat(wrapped.askedDuringACall).as("calls into the wrapped model made while the hold was asked").isEmpty(),
                () -> assertThat(LostStartHold.isAsked()).as("asked on the test thread afterwards").isFalse()
        );
    }

    @Test
    void throws_from_a_subscribe_that_asks_for_the_hold_and_leaves_nothing_so_the_id_subscribes_again_when_the_wrapped_model_refuses_it_for_a_reason_other_than_a_lost_position() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.refusing = true;
        wrapped.refusingForAnotherReason = true;

        // When
        Throwable thrown = catchThrowable(() -> subscribe(true));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isInstanceOf(CatchupCannotResumeException.class);
        assertAll(
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(strategy.registrations).as("registered with the strategy").isEmpty(),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty()
        );
        wrapped.refusing = false;
        assertThatCode(() -> subscribe(true)).as("subscribing the id again").doesNotThrowAnyException();
        assertThat(model.isRunning(ID)).as("running").isTrue();
    }

    @Test
    void does_not_hold_a_waiting_consumer_paused_when_a_grant_is_refused_for_a_reason_other_than_a_lost_position_but_gives_the_lease_back_and_competes_again() {
        // Given
        subscribeWaiting(true);
        wrapped.refusing = true;
        wrapped.refusingForAnotherReason = true;

        // When
        Throwable thrown = grantTheLease();

        // Then
        assertThat(thrown).as("thrown by the grant to the strategy").isInstanceOf(CatchupCannotResumeException.class);
        assertAll(
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(warningsLogged()).as("warnings logged").hasSize(1),
                () -> assertThat(strategy.registrations).as("registered with the strategy, competing again").contains(ID),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
        wrapped.refusing = false;
        grantTheLease();
        assertThat(model.isRunning(ID)).as("running once the wrapped model stops refusing").isTrue();
    }

    @Test
    void keeps_a_held_consumer_paused_through_start_false_and_does_not_ask_the_wrapped_model_to_subscribe_or_resume_it_again_when_the_model_was_built_over_a_wrapped_model_that_was_not_running() {
        // Given
        useAModelBuiltOverAWrappedModelThatIsNotRunning();
        strategy.grantOnRegister = true;
        wrapped.refusing = true;
        subscribe(true);
        assertHeldPaused();
        int callsBefore = wrapped.calls.get();

        // When
        model.start(false);

        // Then
        assertHeldPaused();
        await().during(NO_FURTHER_CALLS_FOR).atMost(NO_FURTHER_CALLS_FOR.plusSeconds(5)).untilAsserted(() ->
                assertThat(wrapped.calls.get()).as("calls into the wrapped model after start(false)").isEqualTo(callsBefore));
        assertThat(errorsLogged()).as("errors logged").hasSize(1);
    }

    @Test
    void competes_again_for_a_held_consumer_when_start_true_follows_a_start_false_on_a_model_built_over_a_wrapped_model_that_was_not_running() {
        // Given
        useAModelBuiltOverAWrappedModelThatIsNotRunning();
        strategy.grantOnRegister = true;
        wrapped.refusing = true;
        subscribe(true);
        model.start(false);
        assertHeldPaused();
        wrapped.refusing = false;

        // When
        model.start(true);

        // Then
        assertThat(model.isRunning(ID)).as("running").isTrue();
    }

    @Test
    void competes_again_for_a_held_consumer_when_the_user_resumes_it_after_a_start_false_on_a_model_built_over_a_wrapped_model_that_was_not_running() {
        // Given
        useAModelBuiltOverAWrappedModelThatIsNotRunning();
        strategy.grantOnRegister = true;
        wrapped.refusing = true;
        subscribe(true);
        model.start(false);
        assertHeldPaused();
        wrapped.refusing = false;

        // When
        model.resumeSubscription(ID);

        // Then
        assertThat(model.isRunning(ID)).as("running").isTrue();
    }

    @Test
    void start_true_of_a_manual_start_model_over_it_returns_and_leaves_a_consumer_held_paused_with_one_error_when_the_wrapped_model_refuses_the_subscribe_that_asked_for_the_hold() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.refusing = true;
        ManualStartSubscriptionModel manual = ManualStartSubscriptionModel.stoppedByDefault(model);
        LostStartHold.askedDuring(() -> manual.subscribe(ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }));

        // When
        Throwable thrown = catchThrowable(() -> manual.start(true));

        // Then
        assertThat(thrown).as("thrown by start(true)").isNull();
        assertHeldPaused();
        assertThat(errorsLogged()).as("errors logged").hasSize(1);
    }

    @Test
    void resumes_a_subscription_that_asked_for_the_hold_through_a_manual_start_model_that_was_stopped_when_the_wrapped_model_does_not_refuse() {
        // Given
        strategy.grantOnRegister = true;
        ManualStartSubscriptionModel manual = ManualStartSubscriptionModel.stoppedByDefault(model);
        manual.start(true);
        manual.stop();
        LostStartHold.askedDuring(() -> manual.subscribe(ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }));

        // When
        Throwable thrown = catchThrowable(() -> manual.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(model.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @Test
    void a_second_start_true_of_a_manual_start_model_over_it_after_a_stop_returns_and_leaves_every_consumer_that_asked_for_the_hold_held_paused_while_the_wrapped_model_still_refuses() {
        // Given
        String other = "other";
        strategy.grantOnRegister = true;
        wrapped.refusing = true;
        ManualStartSubscriptionModel manual = ManualStartSubscriptionModel.stoppedByDefault(model);
        LostStartHold.askedDuring(() -> manual.subscribe(ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }));
        manual.start(true);
        manual.stop();
        LostStartHold.askedDuring(() -> manual.subscribe(other, null, StartAt.subscriptionModelDefault(), __ -> {
        }));

        // When
        Throwable thrown = catchThrowable(() -> manual.start(true));

        // Then
        assertThat(thrown).as("thrown by the second start(true)").isNull();
        assertAll(
                () -> assertThat(model.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(model.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(model.isPaused(other)).as("paused, registered after the stop").isTrue(),
                () -> assertThat(model.isRunning(other)).as("running, registered after the stop").isFalse(),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty()
        );
    }

    @Test
    void resume_of_a_deferred_subscription_that_asked_for_the_hold_through_a_manual_start_model_over_a_stopped_one_returns_and_leaves_the_consumer_held_paused_with_one_error_when_the_wrapped_model_refuses_it() {
        // Given
        strategy.grantOnRegister = true;
        ManualStartSubscriptionModel manual = ManualStartSubscriptionModel.stoppedByDefault(model);
        manual.start(true);
        manual.stop();
        assertThat(model.isRunning()).as("the competing-consumer model is running").isFalse();
        wrapped.refusing = true;
        LostStartHold.askedDuring(() -> manual.subscribe(ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }));

        // When
        Throwable thrown = catchThrowable(() -> manual.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertHeldPaused();
        assertThat(errorsLogged()).as("errors logged").hasSize(1);
    }

    @Test
    void holds_a_consumer_the_system_paused_paused_with_one_error_when_its_resume_on_a_grant_is_refused_as_a_history_loss_of_a_resume() {
        // Given
        strategy.grantOnRegister = true;
        subscribe(false);
        strategy.holders.remove(ID);
        model.onConsumeProhibited(ID, NODE);
        wrapped.refusing = true;
        wrapped.refusingAResumeAsAHistoryLoss = true;
        wrapped.callNames.clear();

        // When
        Throwable thrown = grantTheLease();

        // Then
        assertThat(thrown).as("thrown by the grant to the strategy").isNull();
        assertThat(wrapped.callNames).as("calls into the wrapped model").containsExactly("resumeSubscription");
        assertHeldPaused();
        assertThat(errorsLogged()).as("errors logged").hasSize(1);
    }

    @Test
    void makes_the_subscription_in_the_wrapped_model_when_a_consumer_held_while_it_waited_is_resumed_and_granted_the_lease() {
        // Given
        subscribeWaiting(true);
        wrapped.refusing = true;
        grantTheLease();
        assertHeldPaused();
        wrapped.refusing = false;
        strategy.grantOnRegister = true;

        // When
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(wrapped.callNames.getLast()).as("last call into the wrapped model").isEqualTo("subscribe"),
                () -> assertThat(model.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(wrapped.isRunning(ID)).as("running in the wrapped model").isTrue()
        );
    }

    @Test
    void throws_a_refusal_as_a_history_loss_from_a_subscribe_that_asked_for_the_hold_when_the_id_was_cancelled_during_the_subscribe() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.refusing = true;
        wrapped.beforeRefusing = () -> model.cancelSubscription(ID);

        // When
        Throwable thrown = catchThrowable(() -> subscribe(true));

        // Then
        assertThat(thrown).as("thrown by the subscribe").isInstanceOf(CatchupCannotResumeException.class);
        assertAll(
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(model.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(strategy.registrations).as("registered with the strategy").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @Test
    void throws_a_history_loss_that_a_grant_inside_the_registration_met_to_the_resume_that_registered_a_consumer_held_while_it_waited() {
        // Given
        subscribeWaiting(true);
        wrapped.refusing = true;
        grantTheLease();
        assertHeldPaused();
        strategy.grantInsideRegister = true;
        wrapped.callNames.clear();

        // When
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isInstanceOf(CatchupCannotResumeException.class);
        assertAll(
                () -> assertThat(wrapped.callNames).as("calls into the wrapped model, the refused one only").containsExactly("subscribe"),
                () -> assertThat(model.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(model.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty()
        );
    }

    @Test
    void holds_a_running_consumer_paused_with_one_error_and_gives_up_the_lease_when_the_wrapped_model_tells_that_it_holds_the_subscription_paused() {
        // Given a consumer that runs and holds the lease
        ReportingWrappedModel reporting = useAModelOverAWrappedModelThatReportsHeldPaused();
        strategy.grantOnRegister = true;
        subscribe(false);
        assertThat(model.isRunning(ID)).as("running before the report").isTrue();
        assertThat(strategy.holders).as("holding the lease before the report").containsExactly(ID);
        int callsBefore = wrapped.calls.get();

        // When the wrapped model holds it paused because the live start its replay would go live from is gone
        reporting.holdsPausedAndTells(ID, HistoryLossMessages.liveStartLost("The live start is gone", (Checkpoint) () -> "live start", true));

        // Then
        await().untilAsserted(this::assertHeldPaused);
        assertThat(errorsLogged()).as("errors logged").singleElement().asString()
                .contains("replays the subscription from the beginning again").doesNotContain("is refused again");
        grantTheLease();
        model.onConsumeProhibited(ID, NODE);
        await().during(NO_FURTHER_CALLS_FOR).atMost(NO_FURTHER_CALLS_FOR.plusSeconds(5)).untilAsserted(() ->
                assertThat(wrapped.calls.get()).as("calls into the wrapped model after the report").isEqualTo(callsBefore));
    }

    @Test
    void holds_a_running_consumer_paused_and_gives_up_the_lease_when_the_wrapped_model_fails_the_first_question_whether_it_holds_the_subscription_paused() {
        // Given a consumer that runs and holds the lease, over a wrapped model that fails the first such question
        ReportingWrappedModel reporting = useAModelOverAWrappedModelThatReportsHeldPaused();
        strategy.grantOnRegister = true;
        subscribe(false);
        reporting.heldPausedQueriesToFail.set(1);

        // When the wrapped model holds it paused and tells
        reporting.holdsPausedAndTells(ID, HistoryLossMessages.liveStartLost("The live start is gone", (Checkpoint) () -> "live start", true));

        // Then the report is still there for the retry
        await().atMost(Duration.ofSeconds(20)).untilAsserted(this::assertHeldPaused);
        assertThat(reporting.heldPausedQueriesToFail.get()).as("failures left to throw").isZero();
        assertThat(errorsLogged()).as("errors logged").singleElement().asString().contains("replays the subscription from the beginning again");
    }

    @Test
    void holds_a_running_consumer_paused_on_the_thread_that_tells_it_before_the_wrapped_model_returns_from_telling_that_it_holds_the_subscription_paused() {
        // Given a consumer that runs and holds the lease, with no call for it under way
        ReportingWrappedModel reporting = useAModelOverAWrappedModelThatReportsHeldPaused();
        strategy.grantOnRegister = true;
        subscribe(false);

        // When the wrapped model holds it paused because the live start its replay would go live from is gone
        reporting.holdsPausedAndTells(ID, HistoryLossMessages.liveStartLost("The live start is gone", (Checkpoint) () -> "live start", true));

        // Then
        String tellingThread = Thread.currentThread().getName();
        assertHeldPaused();
        assertThat(logged.events.stream().filter(event -> event.getLevel().equals(Level.ERROR)).map(ILoggingEvent::getThreadName).toList())
                .as("threads that logged an error").containsExactly(tellingThread);
    }

    @Test
    void competes_again_and_resumes_the_wrapped_model_when_the_user_resumes_a_consumer_the_wrapped_model_told_it_holds_paused() {
        // Given
        ReportingWrappedModel reporting = useAModelOverAWrappedModelThatReportsHeldPaused();
        strategy.grantOnRegister = true;
        subscribe(false);
        reporting.holdsPausedAndTells(ID, HistoryLossMessages.liveStartLost("The live start is gone", (Checkpoint) () -> "live start", true));
        await().untilAsserted(this::assertHeldPaused);

        // When
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(ID));

        // Then
        assertThat(thrown).as("thrown by the resume").isNull();
        assertAll(
                () -> assertThat(model.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(wrapped.isRunning(ID)).as("running in the wrapped model").isTrue(),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID)
        );
    }

    @Test
    void leaves_a_running_consumer_running_with_its_lease_when_a_held_paused_report_deferred_by_a_call_comes_after_the_wrapped_model_ran_the_subscription_again_and_a_pause_there_held_it_once_more() {
        // Given a consumer that runs and holds the lease, and a call of its own that holds the subscription's lock
        ReportingWrappedModel reporting = useAModelOverAWrappedModelThatReportsHeldPaused();
        strategy.grantOnRegister = true;
        subscribe(false);
        CountDownLatch callHoldsTheLock = new CountDownLatch(1);
        CountDownLatch letTheCallReturn = new CountDownLatch(1);
        reporting.afterTheNextAnswerToIsRunning.set(() -> {
            callHoldsTheLock.countDown();
            awaitLatch(letTheCallReturn);
        });
        AtomicReference<@Nullable Throwable> callThrew = new AtomicReference<>();
        Thread call = new Thread(() -> callThrew.set(catchThrowable(() -> model.resumeSubscription(ID))), "call-holding-the-lock");
        call.start();
        awaitLatch(callHoldsTheLock);

        // When the wrapped model holds the subscription paused and tells, so the report waits for the lock
        reporting.holdsPausedAndTells(ID, HistoryLossMessages.liveStartLost("The live start is gone", (Checkpoint) () -> "live start", true));
        // And runs it again, which ends that hold, and a pause of its own holds it paused once more
        reporting.resumeSubscription(ID);
        reporting.pauseSubscription(ID);
        letTheCallReturn.countDown();

        // Then the report that was left to wait holds nothing
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                () -> assertThat(callThrew.get()).as("thrown by the call").isInstanceOf(SubscriptionAlreadyRunningException.class),
                () -> assertThat(model.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(strategy.registrations).as("registered with the strategy").containsExactly(ID),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        ));
    }

    @Test
    void holds_a_running_consumer_paused_and_gives_up_the_lease_when_a_late_held_paused_report_for_an_earlier_hold_comes_after_the_report_for_the_current_hold_while_a_call_holds_the_lock() {
        // Given a consumer that runs and holds the lease, and a call of its own that holds the subscription's lock
        ReportingWrappedModel reporting = useAModelOverAWrappedModelThatReportsHeldPaused();
        strategy.grantOnRegister = true;
        subscribe(false);
        CountDownLatch callHoldsTheLock = new CountDownLatch(1);
        CountDownLatch letTheCallReturn = new CountDownLatch(1);
        reporting.afterTheNextAnswerToIsRunning.set(() -> {
            callHoldsTheLock.countDown();
            awaitLatch(letTheCallReturn);
        });
        AtomicReference<@Nullable Throwable> callThrew = new AtomicReference<>();
        Thread call = new Thread(() -> callThrew.set(catchThrowable(() -> model.resumeSubscription(ID))), "call-holding-the-lock");
        call.start();
        awaitLatch(callHoldsTheLock);
        CatchupCannotResumeException earlierRefusal = HistoryLossMessages.liveStartLost("The live start is gone", (Checkpoint) () -> "live start", true);
        CatchupCannotResumeException currentRefusal = HistoryLossMessages.liveStartLost("The live start is gone again", (Checkpoint) () -> "live start", true);

        // When the wrapped model holds the subscription paused for the current refusal and tells, so the report waits for the lock
        reporting.holdsPausedAndTells(ID, currentRefusal);
        // And a late report for an earlier refusal, which it no longer holds the subscription for, comes after it
        reporting.tellsOnly(ID, earlierRefusal);
        letTheCallReturn.countDown();

        // Then the consumer is held paused for the current refusal and has given up the lease
        await().atMost(Duration.ofSeconds(10)).untilAsserted(this::assertHeldPaused);
        joinTheCall(call);
        assertThat(callThrew.get()).as("thrown by the call").isInstanceOf(SubscriptionAlreadyRunningException.class);
        assertThat(errorsLogged()).as("errors logged").hasSize(1);
    }

    @Test
    void holds_a_running_consumer_paused_once_when_the_wrapped_model_answers_that_it_holds_the_subscription_paused_for_two_reports_that_waited_for_the_lock() {
        // Given a consumer that runs and holds the lease, and a call of its own that holds the subscription's lock
        ReportingWrappedModel reporting = useAModelOverAWrappedModelThatReportsHeldPaused();
        strategy.grantOnRegister = true;
        subscribe(false);
        CountDownLatch callHoldsTheLock = new CountDownLatch(1);
        CountDownLatch letTheCallReturn = new CountDownLatch(1);
        reporting.afterTheNextAnswerToIsRunning.set(() -> {
            callHoldsTheLock.countDown();
            awaitLatch(letTheCallReturn);
        });
        Thread call = new Thread(() -> catchThrowable(() -> model.resumeSubscription(ID)), "call-holding-the-lock");
        call.start();
        awaitLatch(callHoldsTheLock);
        reporting.answersHeldPausedForEveryReport.set(true);

        // When the wrapped model tells twice that it holds the subscription paused, so both reports wait for the lock
        reporting.holdsPausedAndTells(ID, HistoryLossMessages.liveStartLost("The live start is gone", (Checkpoint) () -> "live start", true));
        reporting.tellsOnly(ID, HistoryLossMessages.liveStartLost("The live start is gone again", (Checkpoint) () -> "live start", true));
        letTheCallReturn.countDown();

        // Then the consumer is held paused once and has given up the lease
        await().atMost(Duration.ofSeconds(10)).untilAsserted(this::assertHeldPaused);
        joinTheCall(call);
        assertThat(errorsLogged()).as("errors logged").hasSize(1);
    }

    private static void joinTheCall(Thread call) {
        try {
            call.join(Duration.ofSeconds(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        assertThat(call.isAlive()).as("call still running").isFalse();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private ReportingWrappedModel useAModelOverAWrappedModelThatReportsHeldPaused() {
        model.shutdown();
        ReportingWrappedModel reporting = new ReportingWrappedModel();
        wrapped = reporting;
        strategy = new Strategy();
        model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        return reporting;
    }

    private void useAModelBuiltOverAWrappedModelThatIsNotRunning() {
        model.shutdown();
        wrapped = new WrappedModel(false);
        strategy = new Strategy();
        model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
    }

    private void subscribeWaiting(boolean holdAsked) {
        strategy.grantOnRegister = false;
        subscribe(holdAsked);
        assertThat(model.isPaused(ID)).as("paused while waiting").isFalse();
        assertThat(strategy.registrations).as("registered with the strategy while waiting").contains(ID);
    }

    private void subscribe(boolean holdAsked) {
        if (holdAsked) {
            LostStartHold.askedDuring(() -> model.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
            }));
        } else {
            model.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
            });
        }
    }

    // What the strategy does when it hands the lease to this node, on a thread of its own in production
    private @Nullable Throwable grantTheLease() {
        strategy.holders.add(ID);
        return catchThrowable(() -> model.onConsumeGranted(ID, NODE));
    }

    private void assertHeldPaused() {
        assertAll(
                () -> assertThat(model.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(model.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(wrapped.isRunning(ID)).as("running in the wrapped model").isFalse(),
                () -> assertThat(strategy.registrations).as("registered with the strategy").isEmpty(),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty()
        );
    }

    private List<String> warningsLogged() {
        return logged.events.stream().filter(event -> event.getLevel().equals(Level.WARN)).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> errorsLogged() {
        return logged.events.stream().filter(event -> event.getLevel().equals(Level.ERROR)).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private static final class LoggedEvents extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }
    }

    // Grants the lease on register when told to, and otherwise leaves the consumer waiting for a grant the test gives
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> registrations = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private volatile boolean grantOnRegister;
        private volatile boolean grantInsideRegister;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.add(subscriptionId);
            if (grantInsideRegister) {
                holders.add(subscriptionId);
                listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
                return true;
            }
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.remove(subscriptionId);
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }

        @Override
        public void shutdown() {
        }
    }

    // A model that throws CatchupCannotResumeException from every call that starts or resumes a subscription while the
    // test says so, as a catch-up does for a position it has no history back to
    private static class WrappedModel implements SubscriptionModel {
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private final AtomicInteger calls = new AtomicInteger();
        private final List<String> askedDuringACall = new CopyOnWriteArrayList<>();
        private volatile boolean refusing;
        private volatile boolean refusingForAnotherReason;
        private volatile boolean refusingAResumeAsAHistoryLoss;
        private volatile @Nullable Runnable beforeRefusing;
        private final List<String> callNames = new CopyOnWriteArrayList<>();
        private boolean running = true;

        private WrappedModel() {
        }

        private WrappedModel(boolean running) {
            this.running = running;
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            enter("subscribe", subscriptionId);
            (running ? runningIds : pausedIds).add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            enter("subscribePaused", subscriptionId);
            pausedIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            enter("resumeSubscription", subscriptionId);
            if (!pausedIds.contains(subscriptionId) && !runningIds.contains(subscriptionId)) {
                throw new IllegalStateException("Resume of " + subscriptionId + ", which was never subscribed in the wrapped model");
            }
            pausedIds.remove(subscriptionId);
            runningIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            noteTheHold("pauseSubscription");
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            noteTheHold("cancelSubscription");
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public synchronized void stop() {
            running = false;
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
        }

        @Override
        public synchronized boolean isRunning() {
            return running;
        }

        @Override
        public synchronized boolean isRunning(String subscriptionId) {
            noteTheHold("isRunning");
            return runningIds.contains(subscriptionId);
        }

        @Override
        public synchronized boolean isPaused(String subscriptionId) {
            noteTheHold("isPaused");
            return pausedIds.contains(subscriptionId);
        }

        synchronized void holdPaused(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.add(subscriptionId);
        }

        private void enter(String call, String subscriptionId) {
            calls.incrementAndGet();
            callNames.add(call);
            noteTheHold(call);
            if (refusing) {
                Runnable before = beforeRefusing;
                beforeRefusing = null;
                if (before != null) {
                    before.run();
                }
                if (refusingAResumeAsAHistoryLoss && call.equals("resumeSubscription")) {
                    throw HistoryLossMessages.historyLost(subscriptionId, (Checkpoint) () -> "stored position");
                }
                if (refusingForAnotherReason) {
                    throw new CatchupCannotResumeException("The position stored for " + subscriptionId + " cannot be resumed, though no history is lost");
                }
                throw HistoryLossMessages.cannotStartFrom(subscriptionId, (Checkpoint) () -> "stored position");
            }
        }

        private void noteTheHold(String call) {
            if (LostStartHold.isAsked()) {
                askedDuringACall.add(call);
            }
        }
    }

    // A wrapped model that holds a subscription paused and tells its listeners, as a catch-up does when the live start of
    // its replay is gone
    private static final class ReportingWrappedModel extends WrappedModel implements StartRefusalReportingSubscriptions {
        private final List<StartRefusalListener> listeners = new CopyOnWriteArrayList<>();
        private final Map<String, CatchupCannotResumeException> heldFor = new ConcurrentHashMap<>();
        private final AtomicReference<@Nullable Runnable> afterTheNextAnswerToIsRunning = new AtomicReference<>();
        private final AtomicInteger heldPausedQueriesToFail = new AtomicInteger();
        private final AtomicBoolean answersHeldPausedForEveryReport = new AtomicBoolean();

        @Override
        public void addStartRefusalListener(StartRefusalListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeStartRefusalListener(StartRefusalListener listener) {
            listeners.remove(listener);
        }

        void holdsPausedAndTells(String subscriptionId, CatchupCannotResumeException refusal) {
            holdPaused(subscriptionId);
            heldFor.put(subscriptionId, refusal);
            tellsOnly(subscriptionId, refusal);
        }

        void tellsOnly(String subscriptionId, CatchupCannotResumeException refusal) {
            listeners.forEach(listener -> listener.handleHeldPaused(subscriptionId, refusal));
        }

        @Override
        public boolean isHeldPausedFor(String subscriptionId, CatchupCannotResumeException refusal) {
            if (heldPausedQueriesToFail.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new IllegalStateException("The wrapped model cannot tell whether it holds the subscription paused");
            }
            return answersHeldPausedForEveryReport.get() || heldFor.get(subscriptionId) == refusal;
        }

        // The answer is read before the hook runs, so a call that is held up there answers from before
        @Override
        public boolean isRunning(String subscriptionId) {
            boolean running = super.isRunning(subscriptionId);
            @Nullable Runnable hook = afterTheNextAnswerToIsRunning.getAndSet(null);
            if (hook != null) {
                hook.run();
            }
            return running;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            heldFor.remove(subscriptionId);
            return super.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            heldFor.remove(subscriptionId);
            return super.subscribePaused(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            heldFor.remove(subscriptionId);
            return super.resumeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            heldFor.remove(subscriptionId);
            super.cancelSubscription(subscriptionId);
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            if (resumeSubscriptionsAutomatically) {
                heldFor.clear();
            }
            super.start(resumeSubscriptionsAutomatically);
        }
    }

    private record WrappedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
