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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A scheduled executor cancels every later execution of a task once one execution throws, and nothing reads the
 * {@code ScheduledFuture} that would hold what it threw. A rethrow from {@link ScheduledRefresh.Scheduler} would
 * therefore reach nobody and only stop every later refresh, so the node would never refresh or acquire a lease again.
 * That is why every throwable is logged and the next refresh runs, a {@link VirtualMachineError} included, and why a
 * throwable from logging it is caught as well.
 * <p>
 * A notifier that throws on taking a notification, as a cached thread pool does when it can't start a thread, has the
 * hand-over dropped and logged once per refresh. The strategy's next refresh compares again and hands over what is still
 * due, so nothing here keeps a notification for it.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@DisplayName("ScheduledRefreshThrowable")
class ScheduledRefreshThrowableTest {

    private static final Duration PERIOD = Duration.ofMillis(20);

    private final Logged logged = new Logged();
    private final Logger scheduledRefreshLogger = (Logger) LoggerFactory.getLogger(ScheduledRefresh.class);
    private ScheduledRefresh scheduledRefresh;

    @BeforeEach
    void captureTheLogOfScheduledRefresh() {
        logged.start();
        scheduledRefreshLogger.addAppender(logged);
    }

    @AfterEach
    void stopCapturingAndTheRefresh() {
        scheduledRefreshLogger.detachAppender(logged);
        logged.stop();
        if (scheduledRefresh != null) {
            scheduledRefresh.close();
        }
    }

    @Nested
    @DisplayName("when a refresh throws on its first run")
    class When_a_refresh_throws_on_its_first_run {

        @ParameterizedTest
        @MethodSource("org.occurrent.subscription.mongodb.blocking.ccs.internal.ScheduledRefreshThrowableTest#schedulesAndThrowables")
        @DisplayName("the next scheduled refresh still runs, since a rethrow would only cancel every later refresh")
        void the_next_scheduled_refresh_still_runs(Schedule schedule, Throwable thrown) {
            // Given
            AtomicInteger runs = new AtomicInteger();

            // When
            scheduleThatThrowsOnTheFirstRun(schedule, runs, thrown);

            // Then
            await().atMost(3, SECONDS).until(runs::get, count -> count >= 3);
        }

        @ParameterizedTest
        @MethodSource("org.occurrent.subscription.mongodb.blocking.ccs.internal.ScheduledRefreshThrowableTest#schedulesAndThrowables")
        @DisplayName("it is logged at ERROR with the class name of what was thrown")
        void it_is_logged_at_error_level_with_the_class_name_of_what_was_thrown(Schedule schedule, Throwable thrown) {
            // Given
            AtomicInteger runs = new AtomicInteger();

            // When
            scheduleThatThrowsOnTheFirstRun(schedule, runs, thrown);

            // Then
            await().atMost(3, SECONDS).untilAsserted(() -> {
                List<ILoggingEvent> errors = logged.events();
                assertThat(errors).as("the throwable is logged once").hasSize(1);
                assertAll(
                        () -> assertThat(errors.getFirst().getLevel()).isEqualTo(Level.ERROR),
                        () -> assertThat(errors.getFirst().getFormattedMessage()).contains(thrown.getClass().getName()),
                        () -> assertThat(errors.getFirst().getThrowableProxy().getClassName()).isEqualTo(thrown.getClass().getName())
                );
            });
        }
    }

    @Nested
    @DisplayName("when a refresh does not throw")
    class When_a_refresh_does_not_throw {

        @ParameterizedTest
        @MethodSource("org.occurrent.subscription.mongodb.blocking.ccs.internal.ScheduledRefreshThrowableTest#schedules")
        @DisplayName("nothing is logged at ERROR")
        void nothing_is_logged_at_error_level(Schedule schedule) {
            // Given
            AtomicInteger runs = new AtomicInteger();
            scheduledRefresh = new ScheduledRefresh((lease, scheduler) -> schedule.schedule(scheduler));

            // When
            scheduledRefresh.scheduleInBackground(runs::incrementAndGet, Duration.ofSeconds(1));
            await().atMost(3, SECONDS).until(runs::get, count -> count >= 3);

            // Then
            assertThat(logged.events()).noneMatch(event -> event.getLevel().isGreaterOrEqual(Level.ERROR));
        }
    }

    @Nested
    @DisplayName("when logging what a refresh threw throws too")
    class When_logging_what_a_refresh_threw_throws_too {

        @ParameterizedTest
        @MethodSource("org.occurrent.subscription.mongodb.blocking.ccs.internal.ScheduledRefreshThrowableTest#schedules")
        @DisplayName("the next scheduled refresh still runs")
        void the_next_scheduled_refresh_still_runs(Schedule schedule) {
            // Given
            AtomicInteger runs = new AtomicInteger();

            // When
            scheduleThatThrowsOnTheFirstRun(schedule, runs, new ThrowingWhenLogged());

            // Then
            await().atMost(3, SECONDS).untilAsserted(() ->
                    assertThat(runs.get()).as("the refreshes run so far").isGreaterThanOrEqualTo(3));
        }
    }

    @Nested
    @DisplayName("when the notifier throws an Error on taking a notification")
    class When_the_notifier_throws_an_Error_on_taking_a_notification {

        private final NotifierThatThrowsWhenTold notifier = new NotifierThatThrowsWhenTold();
        private final AtomicInteger refreshes = new AtomicInteger();
        private volatile Runnable duringTheRefresh = () -> {
        };
        private Runnable round;

        @BeforeEach
        void holdTheRefresh() {
            AtomicReference<Runnable> held = new AtomicReference<>();
            scheduledRefresh = new ScheduledRefresh((lease, scheduler) -> held.set(scheduler.refresh()), notifier);
            scheduledRefresh.scheduleInBackground(() -> {
                refreshes.incrementAndGet();
                duringTheRefresh.run();
            }, Duration.ofSeconds(1));
            round = held.get();
        }

        @Test
        @DisplayName("it is logged at ERROR with the subscription id and the class name of what was thrown")
        void it_is_logged_at_error_level_with_the_subscription_id_and_the_class_name_of_what_was_thrown() {
            // Given
            notifier.throwOnEveryTask(new OutOfMemoryError("unable to create native thread"));

            // When
            catchThrowable(() -> scheduledRefresh.notifyInBackground("a-subscription", () -> {
            }));

            // Then
            assertThat(logged.events()).singleElement().satisfies(error -> assertAll(
                    () -> assertThat(error.getLevel()).isEqualTo(Level.ERROR),
                    () -> assertThat(error.getArgumentArray()).contains("a-subscription", OutOfMemoryError.class.getName()),
                    () -> assertThat(error.getThrowableProxy().getClassName()).isEqualTo(OutOfMemoryError.class.getName())
            ));
        }

        @Test
        @DisplayName("the hand-over is dropped, nothing is thrown to the caller and no later refresh delivers it")
        void the_hand_over_is_dropped_nothing_is_thrown_to_the_caller_and_no_later_refresh_delivers_it() throws InterruptedException {
            // Given
            List<String> delivered = new CopyOnWriteArrayList<>();
            notifier.throwOnEveryTask(new OutOfMemoryError("unable to create native thread"));

            // When
            Throwable thrown = catchThrowable(() -> scheduledRefresh.notifyInBackground("a-subscription", () -> delivered.add("first")));
            notifier.takeEveryTask();
            round.run();
            notifier.awaitEveryTask();

            // Then
            assertAll(
                    () -> assertThat(thrown).as("thrown to the caller that handed the notification over").isNull(),
                    () -> assertThat(delivered).isEmpty()
            );
        }

        // The refresh round that handed the notification over would otherwise skip the consumers after it
        @Test
        @DisplayName("and logging it throws too, nothing is thrown to the caller")
        void and_logging_it_throws_too_nothing_is_thrown_to_the_caller() {
            // Given
            notifier.throwOnEveryTask(new ErrorThatThrowsWhenLogged());

            // When
            Throwable thrown = catchThrowable(() -> scheduledRefresh.notifyInBackground("a-subscription", () -> {
            }));

            // Then
            assertThat(thrown).as("thrown to the caller that handed the notification over").isNull();
        }

        @Test
        @DisplayName("a later hand-over in the same refresh is dropped without asking the notifier")
        void a_later_hand_over_in_the_same_refresh_is_dropped_without_asking_the_notifier() throws InterruptedException {
            // Given
            List<String> delivered = new CopyOnWriteArrayList<>();
            notifier.throwOnEveryTask(new OutOfMemoryError("unable to create native thread"));
            duringTheRefresh = () -> {
                scheduledRefresh.notifyInBackground("subscription-1", () -> delivered.add("subscription-1"));
                notifier.takeEveryTask();
                scheduledRefresh.notifyInBackground("subscription-2", () -> delivered.add("subscription-2"));
            };

            // When
            round.run();
            notifier.awaitEveryTask();

            // Then
            assertThat(delivered).as("delivered by a notifier that took every task but the first").isEmpty();
        }

        @Test
        @DisplayName("it is logged once per refresh, however many hand-overs the notifier throws on")
        void it_is_logged_once_per_refresh_however_many_hand_overs_the_notifier_throws_on() {
            // Given
            notifier.throwOnEveryTask(new OutOfMemoryError("unable to create native thread"));
            duringTheRefresh = () -> {
                scheduledRefresh.notifyInBackground("subscription-1", () -> {
                });
                scheduledRefresh.notifyInBackground("subscription-2", () -> {
                });
                scheduledRefresh.notifyInBackground("subscription-3", () -> {
                });
            };

            // When
            round.run();
            int loggedAfterTheFirstRefresh = logged.events().size();
            round.run();
            int loggedAfterTheSecondRefresh = logged.events().size();

            // Then
            assertAll(
                    () -> assertThat(loggedAfterTheFirstRefresh).as("logged after the first refresh").isEqualTo(1),
                    () -> assertThat(loggedAfterTheSecondRefresh).as("logged after the second refresh").isEqualTo(2)
            );
        }

        @Test
        @DisplayName("the next refresh asks the notifier again, and delivers only what it hands over itself")
        void the_next_refresh_asks_the_notifier_again_and_delivers_only_what_it_hands_over_itself() throws InterruptedException {
            // Given
            List<String> delivered = new CopyOnWriteArrayList<>();
            notifier.throwOnEveryTask(new OutOfMemoryError("unable to create native thread"));
            scheduledRefresh.notifyInBackground("subscription-1", () -> delivered.add("dropped"));
            notifier.takeEveryTask();
            duringTheRefresh = () -> scheduledRefresh.notifyInBackground("subscription-1", () -> delivered.add("handed over by the next refresh"));

            // When
            round.run();
            notifier.awaitEveryTask();

            // Then
            assertThat(delivered).containsExactly("handed over by the next refresh");
        }

        @Test
        @DisplayName("and logging it throws during a refresh, the refresh runs to the end and the next one hands over again")
        void and_logging_it_throws_during_a_refresh_the_refresh_runs_to_the_end_and_the_next_one_hands_over_again() throws InterruptedException {
            // Given
            List<String> delivered = new CopyOnWriteArrayList<>();
            AtomicInteger handOversMade = new AtomicInteger();
            notifier.throwOnEveryTask(new ErrorThatThrowsWhenLogged());
            duringTheRefresh = () -> {
                scheduledRefresh.notifyInBackground("subscription-1", () -> delivered.add("subscription-1"));
                scheduledRefresh.notifyInBackground("subscription-2", () -> delivered.add("subscription-2"));
                handOversMade.incrementAndGet();
            };

            // When
            Throwable thrown = catchThrowable(round::run);
            int handOversMadeInTheRoundTheNotifierThrewIn = handOversMade.get();
            notifier.takeEveryTask();
            round.run();
            notifier.awaitEveryTask();

            // Then
            assertAll(
                    () -> assertThat(thrown).as("thrown by the round the notifier threw in").isNull(),
                    () -> assertThat(handOversMadeInTheRoundTheNotifierThrewIn).as("hand-overs made by the round the notifier threw in").isEqualTo(1),
                    () -> assertThat(delivered).containsExactlyInAnyOrder("subscription-1", "subscription-2")
            );
        }

        // A notifier that isn't shut down rejected the task for another reason than close(), so it is handled as any
        // other throw. The strategy's next refresh compares again and hands over what is still due.
        @Test
        @DisplayName("and it is a RejectedExecutionException while the notifier is not shut down, it is logged at ERROR and the hand-over is dropped")
        void and_it_is_a_RejectedExecutionException_while_the_notifier_is_not_shut_down_it_is_logged_at_ERROR_and_the_hand_over_is_dropped() throws InterruptedException {
            // Given
            List<String> delivered = new CopyOnWriteArrayList<>();
            notifier.throwOnEveryTask(new RejectedExecutionException("Queue capacity exceeded"));

            // When
            Throwable thrown = catchThrowable(() -> scheduledRefresh.notifyInBackground("a-subscription", () -> delivered.add("first")));
            notifier.takeEveryTask();
            round.run();
            notifier.awaitEveryTask();

            // Then
            assertAll(
                    () -> assertThat(thrown).as("thrown to the caller that handed the notification over").isNull(),
                    () -> assertThat(logged.events()).singleElement().satisfies(error -> assertAll(
                            () -> assertThat(error.getLevel()).isEqualTo(Level.ERROR),
                            () -> assertThat(error.getThrowableProxy().getClassName()).isEqualTo(RejectedExecutionException.class.getName())
                    )),
                    () -> assertThat(delivered).isEmpty()
            );
        }
    }

    private void scheduleThatThrowsOnTheFirstRun(Schedule schedule, AtomicInteger runs, Throwable thrown) {
        scheduledRefresh = new ScheduledRefresh((lease, scheduler) -> schedule.schedule(scheduler));
        scheduledRefresh.scheduleInBackground(() -> {
            if (runs.incrementAndGet() == 1) {
                sneakyThrow(thrown);
            }
        }, Duration.ofSeconds(1));
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable thrown) throws T {
        throw (T) thrown;
    }

    // An Error from reading its message, as a second OutOfMemoryError while logging the first one would be
    private static class ThrowingWhenLogged extends RuntimeException {
        @Override
        public String getMessage() {
            throw new OutOfMemoryError("thrown while logging");
        }
    }

    private static class Logged extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }

        List<ILoggingEvent> events() {
            return List.copyOf(events);
        }

        void clear() {
            events.clear();
        }
    }

    enum Schedule {
        FIXED_RATE(scheduler -> scheduler.fixedRate(Duration.ZERO, PERIOD)),
        FIXED_DELAY(scheduler -> scheduler.fixedDelay(Duration.ZERO, PERIOD));

        private final Consumer<ScheduledRefresh.Scheduler> schedule;

        Schedule(Consumer<ScheduledRefresh.Scheduler> schedule) {
            this.schedule = schedule;
        }

        void schedule(ScheduledRefresh.Scheduler scheduler) {
            schedule.accept(scheduler);
        }
    }

    static Stream<Schedule> schedules() {
        return Stream.of(Schedule.values());
    }

    // Instances are thrown deliberately. Nothing here runs out of memory or overflows a stack.
    static Stream<Arguments> schedulesAndThrowables() {
        return schedules().flatMap(schedule -> Stream.of(
                Arguments.of(schedule, new IllegalStateException("a refresh that threw")),
                Arguments.of(schedule, new AssertionError("an Error that is not a VirtualMachineError")),
                Arguments.of(schedule, new StackOverflowError("a VirtualMachineError")),
                Arguments.of(schedule, new OutOfMemoryError("another VirtualMachineError"))));
    }
}
