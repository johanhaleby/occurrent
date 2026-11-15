package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import net.jqwik.api.Disabled;
import net.jqwik.api.Example;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.InterleavingRunner.Run;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Kind;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.StartFailure;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class CatchupLifecycleExamplesTest {

    private static Run plainRun() {
        return new Run(new Scenario(false, 0, 0, List.of(List.of())));
    }

    private static LifecycleCall call(Kind kind, String id) {
        return LifecycleCall.of(kind, id);
    }

    @Disabled("#1524: the second pause returns ok")
    @Example
    void a_second_pause_during_a_subscribe_started_replay_is_refused() throws Exception {
        Run run = plainRun();
        try {
            run.history.gate = new CountDownLatch(1);
            assertThat(InterleavingRunner.execute(run, call(Kind.SUBSCRIBE, "a"))).isEqualTo("ok");
            assertThat(run.history.queried.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(InterleavingRunner.execute(run, call(Kind.PAUSE, "a"))).isEqualTo("ok");
            assertThat(InterleavingRunner.execute(run, call(Kind.PAUSE, "a"))).isEqualTo(SubscriptionNotRunningException.class.getSimpleName());
        } finally {
            run.history.gate.countDown();
            InterleavingRunner.quiesce(run);
            run.model.shutdown();
        }
    }

    @Disabled("#1256: the replay delivers after the pause returned")
    @Example
    void no_event_is_delivered_to_the_action_once_a_pause_during_a_subscribe_started_replay_has_returned() throws Exception {
        Run run = plainRun();
        try {
            run.history.gate = new CountDownLatch(1);
            assertThat(InterleavingRunner.execute(run, call(Kind.SUBSCRIBE, "a"))).isEqualTo("ok");
            assertThat(run.history.queried.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(InterleavingRunner.execute(run, call(Kind.PAUSE, "a"))).isEqualTo("ok");
            run.history.gate.countDown();
            InterleavingRunner.quiesce(run);
            assertThat(run.monitor.s2Violations).isEmpty();
        } finally {
            run.history.gate.countDown();
            run.model.shutdown();
        }
    }

    @Example
    void a_cancel_ends_the_replays_again_that_a_lost_live_start_asks_for() throws Exception {
        Run run = plainRun();
        AtomicInteger checks = new AtomicInteger();
        CountDownLatch insideTheFirstCheck = new CountDownLatch(1);
        CountDownLatch cancelReturned = new CountDownLatch(1);
        run.live.canResume = checkpoint -> {
            checks.incrementAndGet();
            insideTheFirstCheck.countDown();
            try {
                cancelReturned.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        };
        try {
            assertThat(InterleavingRunner.execute(run, call(Kind.SUBSCRIBE, "a"))).isEqualTo("ok");
            assertThat(insideTheFirstCheck.await(5, TimeUnit.SECONDS)).as("the replay is inside canResume when the cancel is made").isTrue();
            assertThat(InterleavingRunner.execute(run, call(Kind.CANCEL, "a"))).isEqualTo("ok");
            cancelReturned.countDown();
            InterleavingRunner.quiesce(run);
            assertThat(checks.get()).as("checks, where more than the one made before the cancel returned is a replay again that ran after it").isEqualTo(1);
            assertThat(run.monitor.s2Violations).isEmpty();
            assertThat(run.model.isRunning("a") || run.model.isPaused("a")).isFalse();
        } finally {
            cancelReturned.countDown();
            run.model.shutdown();
        }
    }

    @Example
    void a_subscribe_after_a_cancel_of_the_same_id_in_one_thread_is_known_at_the_end() {
        Scenario scenario = new Scenario(false, 0, 0, List.of(List.of(call(Kind.CANCEL, "a"), call(Kind.SUBSCRIBE, "a")), List.of(call(Kind.SUBSCRIBE, "b"))));
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.outcomes.get(0)).as(run.toString()).containsExactly("ok", "ok");
        assertThat(run.knownAtTheEnd).as("a is subscribed after its only cancel returned").contains("a");
    }

    @Example
    void a_start_that_throws_after_it_took_effect_does_not_park_a_later_subscribe() {
        Run run = plainRun();
        try {
            assertThat(InterleavingRunner.execute(run, call(Kind.STOP, "a"))).isEqualTo("ok");
            assertThat(InterleavingRunner.execute(run, new LifecycleCall(Kind.START_TRUE, null, StartFailure.AFTER_EFFECT))).isEqualTo("InjectedStartFailure");
            assertThat(InterleavingRunner.execute(run, call(Kind.SUBSCRIBE, "a"))).isEqualTo("ok");
            InterleavingRunner.quiesce(run);
            assertThat(run.model.isRunning("a")).isTrue();
        } finally {
            run.model.shutdown();
        }
    }

    @Example
    void a_resume_after_a_start_that_failed_before_it_took_effect_starts_the_model_and_the_replay() {
        Run run = plainRun();
        try {
            assertThat(InterleavingRunner.execute(run, call(Kind.STOP, "a"))).isEqualTo("ok");
            assertThat(InterleavingRunner.execute(run, new LifecycleCall(Kind.START_FALSE, null, StartFailure.BEFORE_EFFECT))).isEqualTo("InjectedStartFailure");
            assertThat(InterleavingRunner.execute(run, call(Kind.SUBSCRIBE, "a"))).isEqualTo("ok");
            assertThat(InterleavingRunner.execute(run, call(Kind.RESUME, "a"))).isEqualTo("ok");
            InterleavingRunner.quiesce(run);
            assertThat(run.model.isRunning()).isTrue();
            assertThat(run.model.isRunning("a")).isTrue();
            assertThat(run.monitor.currentGeneration("a").received).containsExactly("1", "2");
        } finally {
            run.model.shutdown();
        }
    }

    @Disabled("#1516: the model can end stopped after resume(b) returned ok")
    @Example
    void a_resume_that_follows_a_stop_leaves_the_model_started_when_the_subscribe_it_resumes_overlaps_the_stop() {
        int unexplained = 0;
        for (int i = 0; i < 500; i++) {
            Scenario scenario = new Scenario(false, 0, 0, List.of(List.of(call(Kind.SUBSCRIBE, "b")), List.of(call(Kind.STOP, null), call(Kind.RESUME, "b"))));
            Run run = InterleavingRunner.run(scenario, false);
            if (!CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)) {
                unexplained++;
            }
        }
        assertThat(unexplained).isZero();
    }

    @Disabled("#1520: a start(true) that throws after the live model started leaves the parked replay parked, where a start(true) that returns runs it again")
    @Example
    void a_start_true_that_throws_after_it_took_effect_runs_the_parked_replay_again() throws Exception {
        Run run = plainRun();
        try {
            run.history.gate = new CountDownLatch(1);
            assertThat(InterleavingRunner.execute(run, call(Kind.SUBSCRIBE, "a"))).isEqualTo("ok");
            assertThat(run.history.queried.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(InterleavingRunner.execute(run, call(Kind.STOP, null))).isEqualTo("ok");
            run.history.gate.countDown();
            InterleavingRunner.quiesce(run);
            assertThat(run.model.isRunning("a")).isFalse();
            assertThat(InterleavingRunner.execute(run, new LifecycleCall(Kind.START_TRUE, null, StartFailure.AFTER_EFFECT))).isEqualTo("InjectedStartFailure");
            InterleavingRunner.quiesce(run);
            assertThat(run.model.isRunning()).isTrue();
            assertThat(run.model.isRunning("a")).as("the parked replay of a runs again").isTrue();
        } finally {
            run.history.gate.countDown();
            run.model.shutdown();
        }
    }
}
