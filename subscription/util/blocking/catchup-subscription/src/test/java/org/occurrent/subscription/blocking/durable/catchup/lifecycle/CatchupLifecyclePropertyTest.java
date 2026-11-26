package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import net.jqwik.api.Disabled;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.Observed;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.State;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.Step;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.InterleavingRunner.Run;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CatchupLifecyclePropertyTest {

    @Provide
    Arbitrary<Scenario> perIdScenarios() {
        return LifecycleScripts.perIdScenarios();
    }

    @Provide
    Arbitrary<Scenario> mixedScenarios() {
        return LifecycleScripts.mixedScenarios();
    }

    @Provide
    Arbitrary<Scenario> perIdScenariosWithoutPause() {
        return LifecycleScripts.perIdScenariosWithoutPause();
    }

    @Provide
    Arbitrary<Scenario> mixedScenariosWithoutPause() {
        return LifecycleScripts.mixedScenariosWithoutPause();
    }

    @Provide
    Arbitrary<Scenario> mixedScenariosWithoutAnIdCancelledAndSubscribedMoreThanOnce() {
        return LifecycleScripts.withoutAnIdCancelledAndSubscribedMoreThanOnce(LifecycleScripts.mixedScenarios());
    }

    @Provide
    Arbitrary<Scenario> subscribeAndStopScenarios() {
        return LifecycleScripts.subscribeAndStopScenarios();
    }

    @Provide
    Arbitrary<Scenario> subscribeCancelAndStopScenarios() {
        return LifecycleScripts.subscribeCancelAndStopScenarios();
    }

    @Provide
    Arbitrary<Scenario> mixedScenariosWithoutPauseAndWithoutAStopTogetherWithAStartOrAResumeOrAStartTrueAfterEffect() {
        return LifecycleScripts.withoutAStartTrueThatThrowsAfterItTookEffect(LifecycleScripts.withoutAStopTogetherWithAStartOrAResume(LifecycleScripts.mixedScenariosWithoutPause()));
    }

    @Provide
    Arbitrary<Scenario> perIdScenariosWithoutAnIdPausedMoreThanOnce() {
        return LifecycleScripts.withoutAnIdPausedMoreThanOnce(LifecycleScripts.perIdScenarios());
    }

    @Provide
    Arbitrary<Scenario> mixedScenariosWithoutAnIdPausedMoreThanOnceOrAPauseTogetherWithAStartTrueOrAStopTogetherWithAStartOrAResume() {
        return LifecycleScripts.withoutAStopTogetherWithAStartOrAResume(LifecycleScripts.withoutAPauseTogetherWithAStartTrue(LifecycleScripts.withoutAnIdPausedMoreThanOnce(LifecycleScripts.mixedScenarios())));
    }

    @Provide
    Arbitrary<List<LifecycleCall>> serialScripts() {
        return LifecycleScripts.serialScripts();
    }

    @Provide
    Arbitrary<List<LifecycleCall>> serialScriptsWithoutStartTrueAfterEffect() {
        return LifecycleScripts.serialScriptsWithoutStartTrueAfterEffect();
    }

    @Property(tries = 400)
    void a_serial_script_without_a_start_true_that_throws_after_it_took_effect_gives_what_the_reference_model_gives(@ForAll boolean composite, @ForAll("serialScriptsWithoutStartTrueAfterEffect") List<LifecycleCall> script) {
        assertSerialScriptGivesWhatTheReferenceModelGives(composite, script);
    }

    @Disabled("#1520: a start(true) that throws after the live model started leaves the replays parked, where the reference model, like a start(true) that returns, runs them again. Shrunk composite [stop, subscribe(a), subscribe(a), start(true)!AFTER_EFFECT], seed 4956376150096056713")
    @Property(tries = 400)
    void a_serial_script_gives_what_the_reference_model_gives(@ForAll boolean composite, @ForAll("serialScripts") List<LifecycleCall> script) {
        assertSerialScriptGivesWhatTheReferenceModelGives(composite, script);
    }

    private static void assertSerialScriptGivesWhatTheReferenceModelGives(boolean composite, List<LifecycleCall> script) {
        Run run = new Run(new Scenario(composite, 0, 0, List.of(script)));
        try {
            State state = CatchupLifecycleSpec.initial();
            for (LifecycleCall call : script) {
                String outcome = InterleavingRunner.execute(run, call);
                InterleavingRunner.quiesce(run);
                Step step = CatchupLifecycleSpec.apply(state, call);
                state = CatchupLifecycleSpec.settle(step.next());
                Observed expected = CatchupLifecycleSpec.observe(state, InterleavingRunner.IDS);
                assertThat(outcome).as("outcome of %s after %s", call, script).isEqualTo(step.outcome());
                assertThat(InterleavingRunner.observe(run)).as("state after %s in %s", call, script).isEqualTo(expected);
            }
            assertThat(run.notes).as("notes of %s", script).isEmpty();
        } finally {
            run.model.shutdown();
        }
    }

    @Property(tries = 200)
    void s1_at_most_one_action_is_in_flight_per_id(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s1Violations).as(run.toString()).isEmpty();
        assertThat(run.notes).as(run.toString()).isEmpty();
    }

    @Property(tries = 200)
    void s1_at_most_one_action_is_in_flight_per_id_when_no_id_is_cancelled_and_subscribed_more_than_once(@ForAll("mixedScenariosWithoutAnIdCancelledAndSubscribedMoreThanOnce") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s1Violations).as(run.toString()).isEmpty();
        assertThat(run.notes).as(run.toString()).isEmpty();
    }

    @Property(tries = 200)
    void s2_without_pause_no_action_starts_after_a_cancel_or_a_stop_has_returned(@ForAll("mixedScenariosWithoutPause") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s2Violations).as(run.toString()).isEmpty();
    }

    @Property(tries = 3000)
    void s2_with_only_subscribe_and_stop_calls_no_action_starts_after_a_stop_has_returned(@ForAll("subscribeAndStopScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s2Violations).as(run.toString()).isEmpty();
    }

    @Property(tries = 3000)
    void s2_with_only_subscribe_cancel_and_stop_calls_no_action_starts_after_a_cancel_or_a_stop_has_returned(@ForAll("subscribeCancelAndStopScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s2Violations).as(run.toString()).isEmpty();
    }

    @Disabled("#1256: pause(a) returns ok during a subscribe-started replay and the replay delivers afterwards. Shrunk plain [[subscribe(a)], [subscribe(a), pause(a)]], seed 2650826291686904161")
    @Property(tries = 200)
    void s2_no_action_starts_after_a_cancel_a_pause_or_a_stop_has_returned(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s2Violations).as(run.toString()).isEmpty();
    }

    @Property(tries = 200)
    void per_id_calls_without_pause_end_equal_to_some_serial_order(@ForAll("perIdScenariosWithoutPause") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Property(tries = 200)
    void mixed_calls_without_pause_nor_a_stop_together_with_a_start_or_a_resume_nor_a_start_true_that_throws_after_it_took_effect_end_equal_to_some_serial_order(@ForAll("mixedScenariosWithoutPauseAndWithoutAStopTogetherWithAStartOrAResumeOrAStartTrueAfterEffect") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Disabled("#1516: resume(b) after stop() returns ok while the model stays stopped and b parked, when the subscribe(b) it resumes overlapped the stop. Shrunk plain [[subscribe(b)], [stop, resume(b)]], seed 3464570091306833055")
    @Property(tries = 200)
    void mixed_calls_without_pause_end_equal_to_some_serial_order(@ForAll("mixedScenariosWithoutPause") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Property(tries = 200)
    void per_id_calls_with_no_id_paused_more_than_once_end_equal_to_some_serial_order(@ForAll("perIdScenariosWithoutAnIdPausedMoreThanOnce") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Disabled("#1524: a second pause(a) during a subscribe-started replay returns ok, where the pause contract throws SubscriptionNotRunningException. Shrunk plain [[subscribe(a)], [subscribe(a), pause(a), pause(a)]], seed -8472353142428832504")
    @Property(tries = 200)
    void per_id_calls_end_equal_to_some_serial_order(@ForAll("perIdScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Property(tries = 200)
    void mixed_calls_with_no_id_paused_more_than_once_nor_a_pause_together_with_a_start_true_nor_a_stop_together_with_a_start_or_a_resume_end_equal_to_some_serial_order(@ForAll("mixedScenariosWithoutAnIdPausedMoreThanOnceOrAPauseTogetherWithAStartTrueOrAStopTogetherWithAStartOrAResume") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Disabled("#1524: same cause as the per-id property. Shrunk plain [[subscribe(a)], [subscribe(a), pause(a), pause(a)]], seed -8546245661977250639. Also #1256: a pause during a replay takes effect at the handover, so a start(true) after it finds nothing to run again and a stays paused. Shrunk plain [[subscribe(a)], [pause(a), start(true)]], seed 1398733071185951526")
    @Property(tries = 200)
    void mixed_calls_end_equal_to_some_serial_order(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Property(tries = 150)
    void every_subscription_still_subscribed_gets_the_whole_history_and_a_later_live_event_once_after_a_recovery(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, true);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(run.receivedAfterRecovery.values()).as(run.toString()).allSatisfy(received -> assertThat(isHistoryPassesThenOneLiveEvent(received)).as("received %s", received).isTrue());
    }

    /** Without a checkpoint store a replay that runs again starts from the beginning, so history may repeat as passes that each start at 1, the last one whole, then one L */
    private static boolean isHistoryPassesThenOneLiveEvent(List<String> received) {
        if (received.isEmpty() || !received.get(received.size() - 1).equals("L")) {
            return false;
        }
        List<String> history = received.subList(0, received.size() - 1);
        List<String> pass = new java.util.ArrayList<>();
        for (String event : history) {
            if (event.equals("1")) {
                if (!pass.isEmpty() && !pass.equals(List.of("1", "2")) && !List.of("1").equals(pass)) {
                    return false;
                }
                pass.clear();
                pass.add("1");
            } else if (event.equals("2") && pass.equals(List.of("1"))) {
                pass.add("2");
            } else {
                return false;
            }
        }
        return pass.equals(List.of("1", "2"));
    }
}
