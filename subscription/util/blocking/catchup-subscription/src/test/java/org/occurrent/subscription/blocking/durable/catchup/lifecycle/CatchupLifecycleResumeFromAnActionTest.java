package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import io.cloudevents.CloudEvent;
import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * The calls the action of a live call or a replay makes for a subscription of a {@link CatchupSubscriptionModel} take
 * effect once the action has returned, in the order the action made them, together with the {@code stop()} it handed
 * over. A {@code stop()} and a {@code resumeSubscription(..)} from other threads leave the model as one of the two
 * orders would, never as a stopped model whose live delegate runs the subscription.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupLifecycleResumeFromAnActionTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final ControllableLiveModel live = new ControllableLiveModel();
    private final GatedHistory history = new GatedHistory(cloudEvent("1"));
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, history);
    private final CountDownLatch actionReturned = new CountDownLatch(1);
    private final CountDownLatch inTheActionOfX = new CountDownLatch(1);
    private final CountDownLatch goOn = new CountDownLatch(1);
    private final CountDownLatch resumeDecided = new CountDownLatch(1);
    private final CountDownLatch stopReturned = new CountDownLatch(1);
    private final CountDownLatch resumeReleased = new CountDownLatch(1);
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> firstHandle = new AtomicReference<>();
    private final AtomicReference<@Nullable Subscription> secondHandle = new AtomicReference<>();

    @BeforeEach
    void hold_x_paused_by_the_live_delegate() {
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
    }

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        goOn.countDown();
        resumeDecided.countDown();
        stopReturned.countDown();
        resumeReleased.countDown();
        CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model")).get(10, TimeUnit.SECONDS);
    }

    @Test
    void a_stop_and_then_a_resume_from_a_live_action_leave_the_model_running_and_the_resumed_subscription_running_and_the_handle_started() throws Exception {
        // Given the action of y is a live action that stops the model and resumes x
        model.subscribe(Y, null, StartAt.now(), actionOf(() -> {
            model.stop();
            firstHandle.set(model.resumeSubscription(X));
        }));

        // When y receives an event, and the calls the action handed over have run
        runOnAThread(() -> live.publish(cloudEvent("live-1")));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        awaitTheHandedCalls();

        // Then the outcome is that of the stop followed by the resume, which finds the model stopped and x paused
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning()).as("whether the model runs after the stop and the resume the action handed over ran").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the stop and the resume the action handed over ran").isTrue();
        assertThat(model.isPaused(X)).as("whether x is paused after the stop and the resume the action handed over ran").isFalse();
        assertThat(firstHandle.get().waitUntilStarted(WAIT)).as("whether the handle of resumeSubscription(x) answers started").isTrue();
    }

    @Test
    void a_stop_and_then_a_resume_from_the_action_of_a_replay_leave_the_model_running_and_the_resumed_subscription_running_and_the_handle_started() throws Exception {
        // Given the action of the replay of y stops the model and resumes x
        model.subscribe(Y, StartAtTime.beginningOfTime(), actionOf(() -> {
            model.stop();
            firstHandle.set(model.resumeSubscription(X));
        }));

        // When the replay of y reaches its action, and the calls the action handed over have run
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of the replay of y returned").isTrue();
        awaitTheHandedCalls();

        // Then the outcome is that of the stop followed by the resume, which finds the model stopped and x paused
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning()).as("whether the model runs after the stop and the resume the action handed over ran").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the stop and the resume the action handed over ran").isTrue();
        assertThat(model.isPaused(X)).as("whether x is paused after the stop and the resume the action handed over ran").isFalse();
        assertThat(firstHandle.get().waitUntilStarted(WAIT)).as("whether the handle of resumeSubscription(x) answers started").isTrue();
    }

    @Test
    void a_stop_from_another_thread_made_after_the_action_of_a_replay_called_resume_for_a_held_subscription_leaves_the_model_and_the_subscription_as_one_order_of_the_two_calls_would() throws Exception {
        // Given y that the live delegate holds paused, and a replay of w whose action resumes y and then waits for a stop from another thread to return
        model.subscribe(Y, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(Y);
        assertThat(model.isPaused(Y)).as("whether y is held paused before the test starts").isTrue();
        // The live delegate's resume waits for the stop, so a resume that reached it before the stop ran after the stop.
        // A stop made after the resume was handed over runs after it and cannot return first, so the wait ends
        live.beforeResume = () -> {
            resumeDecided.countDown();
            awaitUpTo(stopReturned, Duration.ofMillis(1500));
        };
        model.subscribe("w", StartAtTime.beginningOfTime(), event -> {
            if (inTheActionOfX.getCount() == 0) {
                return;
            }
            inTheActionOfX.countDown();
            awaitLatch(goOn);
            try {
                firstHandle.set(model.resumeSubscription(Y));
            } catch (Throwable e) {
                thrownByTheAction.set(e);
            }
            resumeDecided.countDown();
            awaitUpTo(stopReturned, Duration.ofMillis(1500));
            actionReturned.countDown();
        });
        assertThat(inTheActionOfX.await(10, TimeUnit.SECONDS)).as("whether the replay of w reached its action").isTrue();
        CompletableFuture<Void> stop = CompletableFuture.runAsync(() -> {
            awaitLatch(resumeDecided);
            model.stop();
            stopReturned.countDown();
        }, daemonExecutor("stop-of-the-model"));

        // When the action calls resume for y, a stop from another thread is made, and the action returns
        goOn.countDown();
        stop.get(30, TimeUnit.SECONDS);
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of the replay of w returned").isTrue();
        awaitTheHandedCalls();
        assertThat(thrownByTheAction.get()).as("what resumeSubscription(y) threw in the action").isNull();
        assertThat(firstHandle.get()).as("the handle resumeSubscription(y) returned to the action").isNotNull();

        // Then y and a later historical subscription read as the resume followed by the stop, or the stop followed by the resume
        assertThat(readYAndALaterHistoricalSubscription(Y))
                .as("whether y runs, y is paused and a later historical subscription replays: resume then stop is [false, true, false], stop then resume is [true, false, true]")
                .isIn(List.of(false, true, false), List.of(true, false, true));
    }

    @Test
    void a_stop_from_another_thread_made_while_a_resume_from_another_thread_waits_in_the_live_delegate_leaves_the_model_and_the_subscription_as_one_order_of_the_two_calls_would() throws Exception {
        // Given a running model where x is paused, and a live delegate whose resume of x waits for a latch
        live.beforeResume = () -> {
            resumeDecided.countDown();
            awaitLatch(resumeReleased);
        };
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X), daemonExecutor("resume-of-x"));
        assertThat(resumeDecided.await(10, TimeUnit.SECONDS)).as("whether the live delegate was asked to resume x").isTrue();

        // When a stop from another thread is made, given 500 ms to complete, and the live delegate then lets the resume go on
        CompletableFuture<Void> stop = CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model"));
        try {
            stop.get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
            // The stop waits for the lock the resume holds, and runs once it has been released
        }
        resumeReleased.countDown();
        stop.get(30, TimeUnit.SECONDS);
        resume.get(30, TimeUnit.SECONDS);

        // Then x and a later historical subscription read as the resume followed by the stop, or the stop followed by the resume
        assertThat(readYAndALaterHistoricalSubscription(X))
                .as("whether x runs, x is paused and a later historical subscription replays: resume then stop is [false, true, false], stop then resume is [true, false, true]")
                .isIn(List.of(false, true, false), List.of(true, false, true));
    }

    @Test
    void a_resume_and_then_a_pause_of_the_same_subscription_from_a_live_action_leave_it_paused_and_the_pause_is_not_refused() throws Exception {
        // Given the action of y is a live action that resumes x and then pauses it
        model.subscribe(Y, null, StartAt.now(), actionOf(() -> {
            firstHandle.set(model.resumeSubscription(X));
            model.pauseSubscription(X);
        }));

        // When y receives an event, and the calls the action handed over have run
        runOnAThread(() -> live.publish(cloudEvent("live-1")));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        awaitTheHandedCalls();

        // Then the outcome is that of the resume followed by the pause, which finds x running
        assertThat(thrownByTheAction.get()).as("what the action threw, as the pause finds x still paused when it runs at once").isNull();
        assertThat(model.isPaused(X)).as("whether x is paused after the resume and the pause the action handed over ran").isTrue();
        assertThat(model.isRunning(X)).as("whether x runs after the resume and the pause the action handed over ran").isFalse();
    }

    @Test
    void a_resume_and_then_a_cancel_of_the_same_subscription_from_a_live_action_run_in_that_order_so_x_is_started_and_then_unknown_to_the_model() throws Exception {
        // Given the action of y is a live action that resumes x and then cancels it
        model.subscribe(Y, null, StartAt.now(), actionOf(() -> {
            firstHandle.set(model.resumeSubscription(X));
            model.cancelSubscription(X);
        }));

        // When y receives an event, and the calls the action handed over have run
        runOnAThread(() -> live.publish(cloudEvent("live-1")));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        awaitTheHandedCalls();

        // Then the outcome is that of the resume followed by the cancel, so x is unknown to the model, and the resume had started it before the cancel took it away
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning(X)).as("whether x runs after the resume and the cancel the action made ran").isFalse();
        assertThat(model.isPaused(X)).as("whether x is paused after the resume and the cancel the action made ran").isFalse();
        AtomicReference<Boolean> started = new AtomicReference<>();
        assertThat(catchThrowable(() -> started.set(firstHandle.get().waitUntilStarted(WAIT)))).as("what the handle of resumeSubscription(x) threw").isNull();
        assertThat(started.get()).as("whether the handle of resumeSubscription(x) reports the subscription started, as the resume ran before the cancel").isTrue();
    }

    @Test
    void a_start_and_then_a_subscribe_from_the_action_of_a_replay_on_a_stopped_model_start_the_model_and_replay_the_new_subscription() throws Exception {
        // Given a replay of w whose action is held, and a model that a stop from the test thread parked it on
        AtomicBoolean zReceivedTheHistory = new AtomicBoolean();
        model.subscribe("w", StartAtTime.beginningOfTime(), actionOf(() -> {
            inTheActionOfX.countDown();
            awaitLatch(goOn);
            model.start(false);
            model.subscribe("z", StartAtTime.beginningOfTime(), event -> zReceivedTheHistory.set(true));
        }));
        assertThat(inTheActionOfX.await(10, TimeUnit.SECONDS)).as("whether the replay of w reached its action").isTrue();
        CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model")).get(10, TimeUnit.SECONDS);
        assertThat(model.isRunning()).as("whether the model reads as stopped before the action goes on").isFalse();

        // When the action starts the model and subscribes z from the beginning of time, and the calls it handed over have run
        goOn.countDown();
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of the replay of w returned").isTrue();
        awaitTheHandedCalls();

        // Then the outcome is that of the start followed by the subscribe, so the model runs, and z replays the history and is not paused
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning()).as("whether the model runs after the start the action handed over ran").isTrue();
        try {
            await().atMost(WAIT).until(() -> zReceivedTheHistory.get() || model.isPaused("z"));
        } catch (ConditionTimeoutException ignored) {
            // The assertions below say which of the two did not hold
        }
        assertThat(zReceivedTheHistory.get()).as("whether z replayed the history after the start and the subscribe the action made").isTrue();
        assertThat(model.isPaused("z")).as("whether z is paused after the start and the subscribe the action made").isFalse();
    }

    @Test
    void a_resume_and_then_a_resume_at_a_position_of_the_same_subscription_from_a_live_action_start_it_and_refuse_the_second_through_its_handle_as_already_running() throws Exception {
        // Given the action of y is a live action that resumes x and then resumes it at a position
        model.subscribe(Y, null, StartAt.now(), actionOf(() -> {
            firstHandle.set(model.resumeSubscription(X));
            secondHandle.set(model.resumeSubscription(X, StartAt.now()));
        }));

        // When y receives an event, and the calls the action handed over have run
        runOnAThread(() -> live.publish(cloudEvent("live-1")));
        assertThat(actionReturned.await(10, TimeUnit.SECONDS)).as("whether the action of y returned").isTrue();
        awaitTheHandedCalls();

        // Then the first resume started x, and the second, which found x running, reports it through its handle
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(catchThrowable(() -> firstHandle.get().waitUntilStarted(WAIT))).as("what the handle of resumeSubscription(x) threw").isNull();
        assertThat(catchThrowable(() -> secondHandle.get().waitUntilStarted(WAIT))).as("what the handle of resumeSubscription(x, startAt) threw").isInstanceOf(SubscriptionAlreadyRunningException.class);
        assertThat(model.isRunning(X)).as("whether x runs after the two resumes the action handed over ran").isTrue();
    }

    private Consumer<CloudEvent> actionOf(Runnable calls) {
        return event -> {
            if (actionReturned.getCount() == 0) {
                return;
            }
            try {
                calls.run();
            } catch (Throwable e) {
                thrownByTheAction.set(e);
            } finally {
                actionReturned.countDown();
            }
        };
    }

    // Reads whether the subscription runs and is paused, and then subscribes z from the beginning of time and reads whether
    // its replay delivered the history. A stopped model parks the replay of z, so z delivering nothing and reading paused
    // tells a stopped model from a started one, whatever the live delegate runs
    private List<Boolean> readYAndALaterHistoricalSubscription(String subscriptionId) {
        boolean runs = model.isRunning(subscriptionId);
        boolean paused = model.isPaused(subscriptionId);
        AtomicBoolean zReceivedTheHistory = new AtomicBoolean();
        model.subscribe("z", StartAtTime.beginningOfTime(), event -> zReceivedTheHistory.set(true));
        try {
            await().atMost(WAIT).until(() -> zReceivedTheHistory.get() || model.isPaused("z"));
        } catch (ConditionTimeoutException ignored) {
            // The assertion on the state says that z neither delivered nor read as paused
        }
        return List.of(runs, paused, zReceivedTheHistory.get());
    }

    // A resume of a subscription nobody made runs behind the calls handed to the lifecycle thread, and changes nothing
    private void awaitTheHandedCalls() {
        assertThat(catchThrowable(() -> model.resumeSubscription("nobody"))).as("what a resume of a subscription nobody made threw").isInstanceOf(UnknownSubscriptionException.class);
    }

    private static void runOnAThread(Runnable body) {
        daemonExecutor("call-on-a-thread").execute(body);
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    private static void awaitUpTo(CountDownLatch latch, Duration timeout) {
        try {
            latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
