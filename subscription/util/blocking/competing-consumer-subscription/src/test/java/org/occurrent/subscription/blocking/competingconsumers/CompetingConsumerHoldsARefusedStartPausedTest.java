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
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.internal.LostStartHold;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final class WrappedModel implements SubscriptionModel {
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

    private record WrappedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
