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

    @Disabled("#1256: the second pause returns ok")
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
            InterleavingRunner.execute(run, call(Kind.SUBSCRIBE, "a"));
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
    void a_cancel_ends_the_replays_again_that_a_lost_live_start_asks_for() {
        Run run = plainRun();
        AtomicInteger checks = new AtomicInteger();
        run.live.canResume = checkpoint -> {
            checks.incrementAndGet();
            return false;
        };
        try {
            InterleavingRunner.execute(run, call(Kind.SUBSCRIBE, "a"));
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (checks.get() == 0 && System.nanoTime() < until) {
                Thread.onSpinWait();
            }
            assertThat(InterleavingRunner.execute(run, call(Kind.CANCEL, "a"))).isEqualTo("ok");
            int atCancel = checks.get();
            InterleavingRunner.quiesce(run);
            assertThat(checks.get()).as("checks after the cancel returned").isEqualTo(atCancel);
            assertThat(run.model.isRunning("a") || run.model.isPaused("a")).isFalse();
        } finally {
            run.model.shutdown();
        }
    }

    @Example
    void a_start_that_throws_after_it_took_effect_does_not_park_a_later_subscribe() {
        Run run = plainRun();
        try {
            InterleavingRunner.execute(run, call(Kind.STOP, "a"));
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
            InterleavingRunner.execute(run, call(Kind.STOP, "a"));
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

    @Disabled("Unowned (nearest #1225, #1237): about 6 of 10 runs end with the model stopped after resume(b) returned ok")
    @Example
    void a_resume_that_follows_a_stop_leaves_the_model_started_when_the_subscribe_it_resumes_overlaps_the_stop() {
        int unexplained = 0;
        for (int i = 0; i < 500; i++) {
            Scenario scenario = new Scenario(false, 0, 0, List.of(List.of(call(Kind.SUBSCRIBE, "b")), List.of(call(Kind.STOP, null), call(Kind.RESUME, "b"))));
            Run run = InterleavingRunner.run(scenario, false);
            if (!CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.observed, InterleavingRunner.IDS)) {
                unexplained++;
            }
        }
        assertThat(unexplained).isZero();
    }
}
