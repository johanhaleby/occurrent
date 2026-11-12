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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static java.util.Objects.requireNonNull;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A wrapped model that reports through {@link StartRefusalReportingSubscriptions} that it refused the start of a
 * subscription after {@code subscribe} returned. The competing consumer it refused is forgotten, gives its lease back
 * and frees its id, and its handle throws the refusal. A report for a consumer the wrapped model never started, or for
 * an id this model has no competing consumer for, changes nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerStartRefusedByTheWrappedModelTest {

    private static final String NODE = "node";
    private static final String ID = "subscription";
    private static final Duration EVENTUALLY = Duration.ofSeconds(10);

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

    @Test
    void a_refusal_reported_while_the_wrapped_model_subscribes_is_acted_on_once_the_subscribe_returns() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.whileSubscribing.set(wrapped::refuse);

        // When
        Subscription subscription = subscribe();

        // Then
        assertRefusedAndForgotten(subscription);
    }

    @Test
    void a_refusal_reported_for_a_consumer_waiting_for_its_lease_leaves_it_waiting() {
        // Given
        Subscription subscription = subscribe();

        // When
        wrapped.report(ID);

        // Then
        AtomicBoolean started = new AtomicBoolean();
        assertThatCode(() -> started.set(subscription.waitUntilStarted(Duration.ofMillis(100)))).as("handle of the waiting consumer").doesNotThrowAnyException();
        assertThat(started).as("handle of the waiting consumer answers").isTrue();
        assertThat(strategy.registrations).as("registered with the strategy").containsExactly(ID);
        grantTheLease();
        assertThat(wrapped.isRunning(ID)).as("running in the wrapped model once granted the lease").isTrue();
        assertThat(model.isRunning(ID)).as("running once granted the lease").isTrue();
        assertThat(errorsLogged()).as("errors logged").isEmpty();
    }

    @Test
    void a_refusal_reported_for_an_unknown_id_or_one_that_does_not_compete_changes_nothing() {
        // Given a consumer the wrapped model no longer has, whose refusal is not reported yet
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        model.subscribe(NODE, "plain", null, StartAt.dynamic(context -> context.hasSubscriptionModelType(CompetingConsumerSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault()), __ -> {
        });
        wrapped.drop(ID);

        // When
        wrapped.report("unknown");
        wrapped.report("plain");

        // Then
        assertAll(
                () -> assertThatCode(() -> subscription.waitUntilStarted(Duration.ofMillis(100))).as("handle of the consumer").doesNotThrowAnyException(),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").contains(ID),
                () -> assertThat(strategy.registrations).as("registered with the strategy").containsExactly(ID),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID),
                () -> assertThat(wrapped.isRunning("plain")).as("non-competing subscription running in the wrapped model").isTrue(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );

        // And the report for the consumer's own id is acted on
        wrapped.report(ID);
        assertRefusedAndForgotten(subscription);
    }

    @Test
    void a_refusal_reported_for_a_consumer_paused_by_the_user_after_it_ran_forgets_it() {
        // Given
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.drop(ID);
        model.pauseSubscription(ID);
        assertThat(model.isPaused(ID)).as("paused by the user").isTrue();

        // When
        wrapped.report(ID);

        // Then
        assertRefusedAndForgotten(subscription);
    }

    @Test
    void the_id_of_a_refused_consumer_can_be_subscribed_again_by_the_same_subscriber_while_the_old_handle_still_throws() {
        // Given
        strategy.grantOnRegister = true;
        wrapped.whileSubscribing.set(wrapped::refuse);
        Subscription refused = subscribe();
        assertRefusedAndForgotten(refused);

        // When
        Subscription again = subscribe();

        // Then
        AtomicBoolean started = new AtomicBoolean();
        assertThatCode(() -> started.set(again.waitUntilStarted(Duration.ofMillis(100)))).as("new handle").doesNotThrowAnyException();
        assertAll(
                () -> assertThat(started).as("new handle started").isTrue(),
                () -> assertThatThrownBy(() -> refused.waitUntilStarted(Duration.ofMillis(100))).as("old handle").isInstanceOf(UnsupportedStartAtException.class),
                () -> assertThat(wrapped.isRunning(ID)).as("running in the wrapped model").isTrue(),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID)
        );
    }

    @Test
    void shutdown_removes_the_listener_from_the_wrapped_model() {
        // Given
        assertThat(wrapped.listeners).as("listeners added by the competing consumer model").hasSize(1);

        // When
        model.shutdown();

        // Then
        assertThat(wrapped.listeners).as("listeners left after shutdown").isEmpty();
    }

    @Test
    void a_handle_of_a_consumer_that_won_the_lease_throws_only_once_the_refusal_is_acted_on_and_the_id_is_free() throws Exception {
        // Given the wrapped handle throws as soon as the subscription was dropped, and the wrapped model reports later
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.dropAndThrowFromTheHandle(ID);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicReference<@Nullable Snapshot> atTheThrow = new AtomicReference<>();
        Thread caller = startThread(() -> {
            try {
                subscription.waitUntilStarted(Duration.ofSeconds(30));
            } catch (Throwable e) {
                thrown.set(e);
                atTheThrow.set(snapshot());
            }
        });

        // When the report is delivered from another thread after the handle has started waiting
        awaitWaitingOrDone(caller);
        wrapped.report(ID);

        // Then
        caller.join(EVENTUALLY.toMillis());
        assertThat(thrown.get()).as("thrown by the handle").isInstanceOf(UnsupportedStartAtException.class);
        Snapshot snapshot = requireNonNull(atTheThrow.get());
        assertAll(
                () -> assertThat(snapshot.subscriptionIds()).as("subscription ids when the handle threw").doesNotContain(ID),
                () -> assertThat(snapshot.registrations()).as("registered with the strategy when the handle threw").isEmpty(),
                () -> assertThat(snapshot.holders()).as("holding the lease when the handle threw").isEmpty(),
                () -> assertThat(snapshot.subscribeAgain()).as("subscribing the same id right after the throw").isNull()
        );
    }

    @Test
    void a_handle_over_a_wrapped_model_that_does_not_report_refusals_throws_the_wrapped_refusal_at_once() {
        // Given
        WrappedModel inner = new WrappedModel();
        Strategy ownStrategy = new Strategy();
        ownStrategy.grantOnRegister = true;
        CompetingConsumerSubscriptionModel ownModel = new CompetingConsumerSubscriptionModel(new NonReportingModel(inner), ownStrategy);
        try {
            Subscription subscription = ownModel.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
            });
            inner.dropAndThrowFromTheHandle(ID);

            // When
            long before = System.nanoTime();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(30)));
            Duration waited = Duration.ofNanos(System.nanoTime() - before);

            // Then
            assertAll(
                    () -> assertThat(thrown).as("thrown by the handle").isInstanceOf(UnsupportedStartAtException.class).isSameAs(inner.thrownByAHandle.get()),
                    () -> assertThat(waited).as("time the handle waited").isLessThan(Duration.ofSeconds(1))
            );
        } finally {
            ownModel.shutdown();
        }
    }

    @Test
    void shutdown_releases_a_handle_waiting_for_a_refusal_that_nothing_reports() {
        // Given
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.dropAndThrowFromTheHandle(ID);
        CompletableFuture<Void> waiting = new CompletableFuture<>();
        Thread caller = startThread(() -> {
            try {
                subscription.waitUntilStarted(Duration.ofSeconds(30));
                waiting.complete(null);
            } catch (Throwable e) {
                waiting.completeExceptionally(e);
            }
        });
        awaitWaitingOrDone(caller);

        // When
        model.shutdown();

        // Then
        assertThat(waiting).as("call waiting for the refusal").failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class).havingCause().isInstanceOf(UnsupportedStartAtException.class).isSameAs(wrapped.thrownByAHandle.get());
    }

    @Test
    void a_handle_waiting_without_a_timeout_is_released_by_the_report_and_throws_the_refusal() throws Exception {
        // Given the wrapped handle has thrown and the handle waits for the refusal to be acted on with no timeout
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.dropAndThrowFromTheHandle(ID);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        Thread caller = startThread(() -> {
            try {
                subscription.waitUntilStarted();
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
        awaitWaitingOrDone(caller);

        // When
        wrapped.report(ID);

        // Then
        caller.join(EVENTUALLY.toMillis());
        assertAll(
                () -> assertThat(caller.isAlive()).as("call waiting without a timeout is released").isFalse(),
                () -> assertThat(thrown.get()).as("thrown by the handle").isInstanceOf(UnsupportedStartAtException.class),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").doesNotContain(ID)
        );
    }

    @Test
    void a_cancel_between_the_wrapped_handle_throwing_and_the_refusal_being_reported_releases_a_waiting_handle_at_once() throws Exception {
        // Given the wrapped handle has thrown and the handle waits for the refusal to be acted on
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.dropAndThrowFromTheHandle(ID);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        Thread caller = startThread(() -> {
            try {
                subscription.waitUntilStarted(Duration.ofSeconds(30));
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
        awaitWaitingOrDone(caller);

        // When the consumer is cancelled before the refusal is reported
        model.cancelSubscription(ID);

        // Then
        caller.join(EVENTUALLY.toMillis());
        assertAll(
                () -> assertThat(thrown.get()).as("thrown by the handle well before its timeout").isInstanceOf(UnsupportedStartAtException.class),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").doesNotContain(ID),
                () -> assertThat(strategy.registrations).as("registered with the strategy").isEmpty()
        );
    }

    @Test
    void a_handle_returned_by_a_resume_the_wrapped_model_refuses_throws_only_once_the_id_is_free() throws Exception {
        // Given a paused consumer whose resume the wrapped model refuses after the resume returned
        strategy.grantOnRegister = true;
        subscribe();
        model.pauseSubscription(ID);
        Subscription resumed = model.resumeSubscription(ID);
        wrapped.dropAndThrowFromTheHandle(ID);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicReference<@Nullable Snapshot> atTheThrow = new AtomicReference<>();
        Thread caller = startThread(() -> {
            try {
                resumed.waitUntilStarted(Duration.ofSeconds(30));
            } catch (Throwable e) {
                thrown.set(e);
                atTheThrow.set(snapshot());
            }
        });

        // When the report is delivered from another thread after the handle has started waiting
        awaitWaitingOrDone(caller);
        wrapped.report(ID);

        // Then
        caller.join(EVENTUALLY.toMillis());
        assertThat(thrown.get()).as("thrown by the resume handle").isInstanceOf(UnsupportedStartAtException.class);
        Snapshot snapshot = requireNonNull(atTheThrow.get());
        assertAll(
                () -> assertThat(snapshot.subscriptionIds()).as("subscription ids when the resume handle threw").doesNotContain(ID),
                () -> assertThat(snapshot.registrations()).as("registered with the strategy when the resume handle threw").isEmpty(),
                () -> assertThat(snapshot.holders()).as("holding the lease when the resume handle threw").isEmpty(),
                () -> assertThat(snapshot.subscribeAgain()).as("subscribing the same id right after the throw").isNull()
        );
    }

    @Test
    void an_interrupt_of_a_handle_waiting_for_the_model_to_act_on_a_refusal_ends_the_wait_and_reaches_the_caller() throws Exception {
        // Given the wrapped handle has thrown, no report is delivered and the handle waits for the refusal to be acted on
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.dropAndThrowFromTheHandle(ID);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicReference<@Nullable Boolean> interruptedWhenThrown = new AtomicReference<>();
        Thread caller = startThread(() -> {
            try {
                subscription.waitUntilStarted(Duration.ofSeconds(30));
            } catch (Throwable e) {
                interruptedWhenThrown.set(Thread.currentThread().isInterrupted());
                thrown.set(e);
            }
        });
        awaitWaitingOrDone(caller);

        // When the caller's thread is interrupted
        caller.interrupt();

        // Then
        caller.join(EVENTUALLY.toMillis());
        assertAll(
                () -> assertThat(thrown.get()).as("thrown by the handle well before its timeout").isInstanceOf(UnsupportedStartAtException.class),
                () -> assertThat(interruptedWhenThrown.get()).as("interrupt flag of the caller's thread when the handle threw").isTrue()
        );
    }

    @Test
    void a_refusal_reported_after_the_wrapped_model_runs_the_consumer_again_leaves_it_running_and_holding_the_lease() {
        // Given a consumer that holds the lease and that the wrapped model runs
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();

        // When the report of a refusal from before the wrapped model made it again arrives
        wrapped.report(ID);

        // Then
        await().during(Duration.ofMillis(300)).atMost(EVENTUALLY).untilAsserted(() -> assertAll(
                () -> assertThatCode(() -> subscription.waitUntilStarted(Duration.ofMillis(100))).as("handle of the consumer").doesNotThrowAnyException(),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").contains(ID),
                () -> assertThat(model.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(wrapped.isRunning(ID)).as("running in the wrapped model").isTrue(),
                () -> assertThat(strategy.registrations).as("registered with the strategy").containsExactly(ID),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        ));
    }

    @Test
    void a_refusal_reported_after_the_wrapped_model_holds_the_consumer_paused_again_leaves_it_paused_and_registered() {
        // Given a consumer that holds the lease and that the wrapped model holds paused
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.pauseSubscription(ID);
        assertThat(wrapped.isPaused(ID)).as("held paused by the wrapped model").isTrue();

        // When the report of a refusal from before the wrapped model made it again arrives
        wrapped.report(ID);

        // Then
        await().during(Duration.ofMillis(300)).atMost(EVENTUALLY).untilAsserted(() -> assertAll(
                () -> assertThatCode(() -> subscription.waitUntilStarted(Duration.ofMillis(100))).as("handle of the consumer").doesNotThrowAnyException(),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").contains(ID),
                () -> assertThat(wrapped.isPaused(ID)).as("held paused by the wrapped model").isTrue(),
                () -> assertThat(strategy.registrations).as("registered with the strategy").containsExactly(ID),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        ));
    }

    @Test
    void a_handle_of_a_refused_consumer_whose_lease_could_not_be_given_up_throws_the_refusal_and_a_later_try_unregisters_it() throws Exception {
        // Given a lease strategy that fails the first unregister, and a handle waiting for the refusal to be acted on
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.dropAndThrowFromTheHandle(ID);
        strategy.unregisterFailuresLeft.set(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        Thread caller = startThread(() -> {
            try {
                subscription.waitUntilStarted(Duration.ofSeconds(30));
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
        awaitWaitingOrDone(caller);

        // When the report is delivered from another thread
        wrapped.report(ID);

        // Then the handle throws the refusal well before its timeout, with the failure to unregister added to it
        caller.join(EVENTUALLY.toMillis());
        assertAll(
                () -> assertThat(caller.isAlive()).as("handle still waiting").isFalse(),
                () -> assertThat(thrown.get()).as("thrown by the handle").isInstanceOf(UnsupportedStartAtException.class).hasCauseInstanceOf(UnsupportedStartAtException.class),
                () -> assertThat(thrown.get()).as("failure to unregister").hasStackTraceContaining("The lease strategy is down"),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").doesNotContain(ID),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").singleElement().asString().contains("a later try unregisters this node from its lease").contains("subscriptionId=" + ID)
        );

        // And a later try unregisters it, after which the id can be subscribed again
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(strategy.registrations).as("registered with the strategy").isEmpty());
        wrapped.handlesThatThrow.remove(ID);
        Subscription again = subscribe();
        assertAll(
                () -> assertThatCode(() -> again.waitUntilStarted(Duration.ofMillis(100))).as("new handle").doesNotThrowAnyException(),
                () -> assertThat(wrapped.isRunning(ID)).as("running in the wrapped model").isTrue(),
                () -> assertThat(strategy.holders).as("holding the lease").containsExactly(ID)
        );
    }

    @Test
    void a_handle_waiting_for_a_refusal_that_nothing_reports_gives_up_when_its_timeout_has_passed() throws Exception {
        // Given the wrapped handle has thrown and no report is delivered
        strategy.grantOnRegister = true;
        Subscription subscription = subscribe();
        wrapped.dropAndThrowFromTheHandle(ID);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        Thread caller = startThread(() -> {
            try {
                subscription.waitUntilStarted(Duration.ofMillis(200));
            } catch (Throwable e) {
                thrown.set(e);
            }
        });

        // When
        caller.join(Duration.ofSeconds(3));

        // Then
        assertAll(
                () -> assertThat(caller.isAlive()).as("handle still waiting after the bound").isFalse(),
                () -> assertThat(thrown.get()).as("thrown by the handle").isInstanceOf(UnsupportedStartAtException.class).isSameAs(wrapped.thrownByAHandle.get())
        );
    }

    private record Snapshot(Set<String> subscriptionIds, Set<String> registrations, Set<String> holders, @Nullable Throwable subscribeAgain) {
    }

    private Snapshot snapshot() {
        Set<String> subscriptionIds = Set.copyOf(model.subscriptionIds());
        Set<String> registrations = Set.copyOf(strategy.registrations);
        Set<String> holders = Set.copyOf(strategy.holders);
        return new Snapshot(subscriptionIds, registrations, holders, catchThrowable(this::subscribe));
    }

    private static Thread startThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    // Parked in a timed wait, or finished, which is what a handle that doesn't wait for the refusal ends up as
    private static void awaitWaitingOrDone(Thread thread) {
        await().atMost(EVENTUALLY).until(() -> thread.getState() == Thread.State.TIMED_WAITING || !thread.isAlive());
    }

    private Subscription subscribe() {
        return model.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    // What the strategy does when it hands the lease to this node, on a thread of its own in production
    private void grantTheLease() {
        strategy.holders.add(ID);
        model.onConsumeGranted(ID, NODE);
    }

    private void assertRefusedAndForgotten(Subscription subscription) {
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThatThrownBy(() -> subscription.waitUntilStarted(Duration.ofMillis(100)))
                .as("handle of the refused consumer").isInstanceOf(UnsupportedStartAtException.class));
        assertAll(
                () -> assertThat(model.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(model.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(model.subscriptionIds()).as("subscription ids").doesNotContain(ID),
                () -> assertThat(strategy.registrations).as("registered with the strategy").isEmpty(),
                () -> assertThat(strategy.holders).as("holding the lease").isEmpty(),
                () -> assertThat(errorsLogged()).as("errors logged").singleElement().asString().contains("subscriptionId=" + ID)
        );
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

    // Grants the lease on register when told to, and otherwise the consumer waits for a grant the test gives
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> registrations = ConcurrentHashMap.newKeySet();
        private volatile boolean grantOnRegister;
        private final AtomicInteger unregisterFailuresLeft = new AtomicInteger();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.add(subscriptionId);
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            if (unregisterFailuresLeft.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new IllegalStateException("The lease strategy is down");
            }
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
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    // Refuses a start the way a MongoDB model does once subscribe has returned, by dropping the subscription and then
    // telling its listeners. A test can also drop without telling, or tell without dropping.
    private static final class WrappedModel implements SubscriptionModel, StartRefusalReportingSubscriptions {
        private final Set<String> runningIds = ConcurrentHashMap.newKeySet();
        private final Set<String> pausedIds = ConcurrentHashMap.newKeySet();
        private final List<StartRefusalListener> listeners = new CopyOnWriteArrayList<>();
        private final Set<String> handlesThatThrow = ConcurrentHashMap.newKeySet();
        private final AtomicReference<@Nullable UnsupportedStartAtException> thrownByAHandle = new AtomicReference<>();
        // Run once, by the next subscribe before it returns
        private final AtomicReference<@Nullable Consumer<String>> whileSubscribing = new AtomicReference<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            runningIds.add(subscriptionId);
            @Nullable Consumer<String> during = whileSubscribing.getAndSet(null);
            if (during != null) {
                during.accept(subscriptionId);
            }
            return new WrappedSubscription(subscriptionId);
        }

        private void refuse(String subscriptionId) {
            drop(subscriptionId);
            report(subscriptionId);
        }

        // The handle throws from waitUntilStarted the way the MongoDB one does once it dropped the subscription, whether
        // or not the listeners are told
        private void dropAndThrowFromTheHandle(String subscriptionId) {
            handlesThatThrow.add(subscriptionId);
            drop(subscriptionId);
        }

        private void drop(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        private void report(String subscriptionId) {
            listeners.forEach(listener -> listener.handleStartRefusal(subscriptionId, new UnsupportedStartAtException(StartAt.subscriptionModelDefault(), "Refused the start of " + subscriptionId)));
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
        public Subscription resumeSubscription(String subscriptionId) {
            if (!pausedIds.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            runningIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            drop(subscriptionId);
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
            return runningIds.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        private final class WrappedSubscription implements Subscription {
            private final String id;

            private WrappedSubscription(String id) {
                this.id = id;
            }

            @Override
            public String id() {
                return id;
            }

            @Override
            public boolean waitUntilStarted(Duration timeout) {
                if (handlesThatThrow.contains(id)) {
                    UnsupportedStartAtException refusal = new UnsupportedStartAtException(StartAt.subscriptionModelDefault(), "Refused the start of " + id);
                    thrownByAHandle.set(refusal);
                    throw refusal;
                }
                return true;
            }
        }
    }

    // Delegates to a model that does not report refusals, so nothing here would act on one
    private static final class NonReportingModel implements SubscriptionModel {
        private final SubscriptionModel delegate;

        private NonReportingModel(SubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
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
    }
}
