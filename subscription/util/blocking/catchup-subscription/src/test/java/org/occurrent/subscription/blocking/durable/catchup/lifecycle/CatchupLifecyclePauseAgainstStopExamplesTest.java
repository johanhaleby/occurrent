package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import net.jqwik.api.Example;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.InterleavingRunner.Run;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Kind;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class CatchupLifecyclePauseAgainstStopExamplesTest {

    private static final String OK = "ok";
    private static final String NOT_RUNNING = SubscriptionNotRunningException.class.getSimpleName();
    private static final String UNKNOWN = UnknownSubscriptionException.class.getSimpleName();

    private static Run plainRun() {
        return new Run(new Scenario(false, 0, 0, List.of(List.of())));
    }

    private static String execute(Run run, Kind kind, String id) {
        return InterleavingRunner.execute(run, LifecycleCall.of(kind, id));
    }

    private static void subscribeWithTheReplayHeldInFlight(Run run, String id) throws InterruptedException {
        run.history.gate = new CountDownLatch(1);
        assertThat(execute(run, Kind.SUBSCRIBE, id)).isEqualTo(OK);
        assertThat(run.history.queried.await(5, TimeUnit.SECONDS)).as("the replay of " + id + " is in flight").isTrue();
    }

    private static void releaseAndClose(Run run) {
        run.history.gate.countDown();
        InterleavingRunner.quiesce(run);
        run.model.shutdown();
    }

    @Example
    void a_pause_that_follows_a_stop_which_parked_a_subscribe_started_replay_is_refused_and_the_subscription_reads_paused() throws Exception {
        Run run = plainRun();
        try {
            subscribeWithTheReplayHeldInFlight(run, "a");
            assertThat(execute(run, Kind.STOP, null)).isEqualTo(OK);
            assertThat(execute(run, Kind.PAUSE, "a")).isEqualTo(NOT_RUNNING);
            assertThat(run.model.isPaused("a")).isTrue();
            assertThat(run.model.isRunning("a")).isFalse();
        } finally {
            releaseAndClose(run);
        }
    }

    @Example
    void a_pause_of_a_subscription_that_was_subscribed_on_a_stopped_model_is_refused_and_the_subscription_reads_paused() {
        Run run = plainRun();
        try {
            assertThat(execute(run, Kind.STOP, null)).isEqualTo(OK);
            assertThat(execute(run, Kind.SUBSCRIBE, "a")).isEqualTo(OK);
            assertThat(execute(run, Kind.PAUSE, "a")).isEqualTo(NOT_RUNNING);
            InterleavingRunner.quiesce(run);
            assertThat(run.model.isPaused("a")).isTrue();
            assertThat(run.model.isRunning("a")).isFalse();
        } finally {
            releaseAndClose(run);
        }
    }

    @Example
    void a_pause_of_a_subscribe_started_replay_that_is_followed_by_a_stop_succeeds_and_the_subscription_reads_paused() throws Exception {
        Run run = plainRun();
        try {
            subscribeWithTheReplayHeldInFlight(run, "b");
            assertThat(execute(run, Kind.PAUSE, "b")).isEqualTo(OK);
            assertThat(execute(run, Kind.STOP, null)).isEqualTo(OK);
            assertThat(run.model.isPaused("b")).isTrue();
            assertThat(run.model.isRunning("b")).isFalse();
        } finally {
            releaseAndClose(run);
        }
    }

    @Example
    void a_pause_of_a_subscription_that_was_never_subscribed_is_unknown() {
        Run run = plainRun();
        try {
            assertThat(execute(run, Kind.PAUSE, "b")).isEqualTo(UNKNOWN);
            assertThat(run.model.isPaused("b")).isFalse();
            assertThat(run.model.isRunning("b")).isFalse();
        } finally {
            releaseAndClose(run);
        }
    }
}
