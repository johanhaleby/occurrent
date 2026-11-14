package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import io.cloudevents.CloudEvent;
import net.jqwik.api.Example;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.Observed;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.Phase;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.Step;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.Timing;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Kind;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.StartFailure;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * Checks of the two oracles of the harness on hand-made input: the reference model's merge, and the monitor of S2.
 */
class CatchupLifecycleOracleTest {

    private static final CloudEvent EVENT = cloudEvent("1");

    private static LifecycleCall call(Kind kind, String id) {
        return LifecycleCall.of(kind, id);
    }

    private static Timing timing(long[] invoked0, long[] returned0, long[] invoked1, long[] returned1) {
        return new Timing(List.of(invoked0, invoked1), List.of(returned0, returned1));
    }

    private static final Scenario SUBSCRIBE_THEN_CANCEL = new Scenario(false, 0, 0, List.of(List.of(call(Kind.SUBSCRIBE, "a")), List.of(call(Kind.CANCEL, "a"))));
    private static final List<List<String>> BOTH_OK = List.of(List.of("ok"), List.of("ok"));
    private static final Observed A_IS_RUNNING = new Observed(true, Map.of("a", true, "b", false), Map.of("a", false, "b", false));

    @Example
    void a_run_that_ends_with_a_running_a_is_rejected_when_subscribe_a_returned_before_cancel_a_began() {
        Timing subscribeReturnedFirst = timing(new long[]{1}, new long[]{2}, new long[]{3}, new long[]{4});
        assertThat(CatchupLifecycleSpec.accepts(SUBSCRIBE_THEN_CANCEL, BOTH_OK, subscribeReturnedFirst, A_IS_RUNNING, Set.of("a", "b")))
                .as("a cancel that began after the subscribe returned cannot be ordered before it").isFalse();
    }

    @Example
    void a_run_that_ends_with_a_running_a_is_accepted_when_cancel_a_overlapped_subscribe_a() {
        Timing overlapping = timing(new long[]{1}, new long[]{4}, new long[]{2}, new long[]{3});
        assertThat(CatchupLifecycleSpec.accepts(SUBSCRIBE_THEN_CANCEL, BOTH_OK, overlapping, A_IS_RUNNING, Set.of("a", "b")))
                .as("a cancel that overlapped the subscribe can take effect first").isTrue();
    }

    @Example
    void a_pause_of_a_replaying_subscription_holds_it_like_a_parked_one() {
        Step paused = CatchupLifecycleSpec.apply(CatchupLifecycleSpec.initial().with("a", Phase.REPLAYING), call(Kind.PAUSE, "a"));
        assertThat(paused.outcome()).isEqualTo(CatchupLifecycleSpec.OK);
        assertThat(paused.next().subscriptions().get("a")).isEqualTo(Phase.PARKED);
        assertThat(CatchupLifecycleSpec.apply(paused.next(), call(Kind.PAUSE, "a")).outcome()).isEqualTo(CatchupLifecycleSpec.NOT_RUNNING);
    }

    private static final LifecycleCall SUBSCRIBE_A = call(Kind.SUBSCRIBE, "a");
    private static final LifecycleCall PAUSE_A = call(Kind.PAUSE, "a");
    private static final LifecycleCall RESUME_A = call(Kind.RESUME, "a");
    private static final LifecycleCall STOP = call(Kind.STOP, null);
    private static final LifecycleCall START_TRUE_FAILING_BEFORE_ITS_EFFECT = new LifecycleCall(Kind.START_TRUE, null, StartFailure.BEFORE_EFFECT);
    private static final LifecycleCall RESUME_FAILING_AFTER_THE_MODEL_STARTED = new LifecycleCall(Kind.RESUME, "a", StartFailure.AFTER_EFFECT);

    private static ExclusiveAction.Generation subscribed(ExclusiveAction monitor) {
        ExclusiveAction.Generation generation = monitor.newGeneration("a");
        monitor.begin(SUBSCRIBE_A);
        monitor.end(SUBSCRIBE_A, generation, true);
        return generation;
    }

    private static void callToCompletion(ExclusiveAction monitor, LifecycleCall call, boolean returned) {
        monitor.begin(call);
        monitor.end(call, null, returned);
    }

    @Example
    void a_stop_that_returned_bars_the_action_until_a_resume_takes_effect() {
        ExclusiveAction monitor = new ExclusiveAction(Set.of("a"));
        ExclusiveAction.Generation generation = subscribed(monitor);
        callToCompletion(monitor, STOP, true);
        monitor.actionOf(generation).accept(EVENT);
        assertThat(monitor.s2Violations).hasSize(1);
    }

    @Example
    void a_start_true_that_failed_before_its_effect_leaves_a_stop_in_force() {
        ExclusiveAction monitor = new ExclusiveAction(Set.of("a"));
        ExclusiveAction.Generation generation = subscribed(monitor);
        callToCompletion(monitor, STOP, true);
        callToCompletion(monitor, START_TRUE_FAILING_BEFORE_ITS_EFFECT, false);
        monitor.actionOf(generation).accept(EVENT);
        assertThat(monitor.s2Violations).as("an action after a stop that no start(true) undid").hasSize(1);
    }

    @Example
    void a_start_true_that_ended_without_returning_and_had_no_injected_failure_leaves_a_stop_in_force() {
        ExclusiveAction monitor = new ExclusiveAction(Set.of("a"));
        ExclusiveAction.Generation generation = subscribed(monitor);
        callToCompletion(monitor, STOP, true);
        callToCompletion(monitor, LifecycleCall.of(Kind.START_TRUE, null), false);
        monitor.actionOf(generation).accept(EVENT);
        assertThat(monitor.s2Violations).as("an action after a stop that no start(true) undid").hasSize(1);
    }

    @Example
    void a_start_true_that_failed_after_its_effect_lifts_a_stop() {
        ExclusiveAction monitor = new ExclusiveAction(Set.of("a"));
        ExclusiveAction.Generation generation = subscribed(monitor);
        callToCompletion(monitor, STOP, true);
        callToCompletion(monitor, new LifecycleCall(Kind.START_TRUE, null, StartFailure.AFTER_EFFECT), false);
        monitor.actionOf(generation).accept(EVENT);
        assertThat(monitor.s2Violations).isEmpty();
    }

    @Example
    void a_resume_whose_model_start_failed_after_it_took_effect_leaves_a_pause_in_force() {
        ExclusiveAction monitor = new ExclusiveAction(Set.of("a"));
        ExclusiveAction.Generation generation = subscribed(monitor);
        callToCompletion(monitor, PAUSE_A, true);
        callToCompletion(monitor, RESUME_FAILING_AFTER_THE_MODEL_STARTED, false);
        monitor.actionOf(generation).accept(EVENT);
        assertThat(monitor.s2Violations).as("an action after a pause that no resume undid").hasSize(1);
    }

    @Example
    void a_start_true_that_returned_lifts_a_stop() {
        ExclusiveAction monitor = new ExclusiveAction(Set.of("a"));
        ExclusiveAction.Generation generation = subscribed(monitor);
        callToCompletion(monitor, STOP, true);
        callToCompletion(monitor, LifecycleCall.of(Kind.START_TRUE, null), true);
        monitor.actionOf(generation).accept(EVENT);
        assertThat(monitor.s2Violations).isEmpty();
    }

    @Example
    void a_pause_that_a_resume_overlapped_from_its_beginning_to_its_return_bars_nothing() throws Exception {
        ExclusiveAction monitor = new ExclusiveAction(Set.of("a"));
        ExclusiveAction.Generation generation = subscribed(monitor);
        ExecutorService pausing = Executors.newSingleThreadExecutor();
        try {
            pausing.submit(() -> monitor.begin(PAUSE_A)).get();
            callToCompletion(monitor, RESUME_A, true);
            pausing.submit(() -> monitor.end(PAUSE_A, null, true)).get();
        } finally {
            pausing.shutdown();
        }
        monitor.actionOf(generation).accept(EVENT);
        assertThat(monitor.s2Violations).as("the resume can have taken effect after the pause").isEmpty();
    }

    @Example
    void a_pause_that_no_enabling_call_overlapped_bars_the_action() throws Exception {
        ExclusiveAction monitor = new ExclusiveAction(Set.of("a"));
        ExclusiveAction.Generation generation = subscribed(monitor);
        callToCompletion(monitor, RESUME_A, true);
        ExecutorService pausing = Executors.newSingleThreadExecutor();
        try {
            pausing.submit(() -> monitor.begin(PAUSE_A)).get();
            pausing.submit(() -> monitor.end(PAUSE_A, null, true)).get();
        } finally {
            pausing.shutdown();
        }
        monitor.actionOf(generation).accept(EVENT);
        assertThat(monitor.s2Violations).hasSize(1);
    }

    private static final Scenario CANCEL_A_THEN_SUBSCRIBE_A_AND_SUBSCRIBE_B = new Scenario(false, 0, 0,
            List.of(List.of(call(Kind.CANCEL, "a"), call(Kind.SUBSCRIBE, "a")), List.of(call(Kind.SUBSCRIBE, "b"))));

    @Example
    void an_id_is_known_at_the_end_when_every_cancel_of_it_returned_before_its_subscribe_was_made() {
        Timing cancelReturnedFirst = timing(new long[]{1, 3}, new long[]{2, 4}, new long[]{5}, new long[]{6});
        assertThat(InterleavingRunner.subscribedAfterEveryCancel(CANCEL_A_THEN_SUBSCRIBE_A_AND_SUBSCRIBE_B, List.of(List.of("ok", "ok"), List.of("ok")), cancelReturnedFirst))
                .containsExactly("a", "b");
    }

    @Example
    void an_id_is_not_required_when_a_cancel_of_it_overlapped_its_subscribe() {
        Timing overlapping = timing(new long[]{1}, new long[]{4}, new long[]{2}, new long[]{3});
        assertThat(InterleavingRunner.subscribedAfterEveryCancel(SUBSCRIBE_THEN_CANCEL, BOTH_OK, overlapping)).isEmpty();
    }

    @Example
    void an_id_is_not_required_when_a_cancel_of_it_never_returned() {
        Timing cancelHung = timing(new long[]{1}, new long[]{2}, new long[]{3}, new long[]{Long.MAX_VALUE});
        assertThat(InterleavingRunner.subscribedAfterEveryCancel(SUBSCRIBE_THEN_CANCEL, List.of(List.of("ok"), List.of("hung")), cancelHung)).isEmpty();
    }

    @Example
    void an_id_is_not_required_when_no_subscribe_of_it_returned_ok() {
        Timing serial = timing(new long[]{1}, new long[]{2}, new long[]{3}, new long[]{4});
        assertThat(InterleavingRunner.subscribedAfterEveryCancel(SUBSCRIBE_THEN_CANCEL, List.of(List.of("IllegalStateException"), List.of("ok")), serial)).isEmpty();
    }
}
