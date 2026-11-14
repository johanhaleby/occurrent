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
    Arbitrary<List<LifecycleCall>> serialScripts() {
        return LifecycleScripts.serialScripts();
    }

    @Property(tries = 400)
    void a_serial_script_gives_what_the_reference_model_gives(@ForAll boolean composite, @ForAll("serialScripts") List<LifecycleCall> script) {
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

    @Disabled("#1515: b's action started after cancel(b) returned, although subscribe(b) had returned before the cancel began. Shrunk composite start=300us [[subscribe(a), resume(a), cancel(b)], [subscribe(b), subscribe(a), subscribe(a)], [resume(a)]], seed 3712755869129851291")
    @Property(tries = 200)
    void s2_without_pause_no_action_starts_after_a_cancel_or_a_stop_has_returned(@ForAll("mixedScenariosWithoutPause") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s2Violations).as(run.toString()).isEmpty();
    }

    @Disabled("#1256: pause(a) returns ok during a subscribe-started replay and the replay delivers afterwards. Shrunk plain [[subscribe(a)], [subscribe(a), pause(a)]], seed -566630320945063148")
    @Property(tries = 200)
    void s2_no_action_starts_after_a_cancel_a_pause_or_a_stop_has_returned(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s2Violations).as(run.toString()).isEmpty();
    }

    @Property(tries = 200)
    void per_id_calls_without_pause_end_equal_to_some_serial_order(@ForAll("perIdScenariosWithoutPause") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Disabled("#1516: resume(b) after stop() returns ok while the model stays stopped and b parked, when the subscribe(b) it resumes overlapped the stop. Shrunk plain [[subscribe(b)], [stop, resume(b)]], seed 3464570091306833055")
    @Property(tries = 200)
    void mixed_calls_without_pause_end_equal_to_some_serial_order(@ForAll("mixedScenariosWithoutPause") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Disabled("#1256: a second pause(a) during a subscribe-started replay returns ok, where the pause contract throws SubscriptionNotRunningException. Shrunk plain [[pause(b)], [subscribe(b), pause(b)]], seed 2619096700851834011")
    @Property(tries = 200)
    void per_id_calls_end_equal_to_some_serial_order(@ForAll("perIdScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Disabled("#1256: same cause as the per-id property. Shrunk plain [[subscribe(a)], [subscribe(a), pause(a), pause(a)]], seed -8546245661977250639")
    @Property(tries = 200)
    void mixed_calls_end_equal_to_some_serial_order(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }

    @Property(tries = 150)
    void every_subscription_still_subscribed_gets_the_whole_history_and_a_later_live_event_once_after_a_recovery(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, true);
        assertThat(run.receivedAfterRecovery.values()).as(run.toString()).allSatisfy(received -> assertThat(isHistoryPassesThenOneLiveEvent(received)).as("received %s", received).isTrue());
        assertThat(run.notes).as(run.toString()).isEmpty();
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
