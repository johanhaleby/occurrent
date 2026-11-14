package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import net.jqwik.api.*;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.InterleavingRunner.Run;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;

import static org.assertj.core.api.Assertions.assertThat;

class TmpProbeTest {
    @Provide Arbitrary<Scenario> perIdScenarios() { return LifecycleScripts.perIdScenarios(); }
    @Provide Arbitrary<Scenario> mixedScenarios() { return LifecycleScripts.mixedScenarios(); }
    @Provide Arbitrary<Scenario> mixedScenariosWithoutPause() { return LifecycleScripts.mixedScenariosWithoutPause(); }

    @Property(tries = 200)
    void s1(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s1Violations).as(run.toString()).isEmpty();
        assertThat(run.notes).as(run.toString()).isEmpty();
    }
    @Property(tries = 200)
    void s2NoPause(@ForAll("mixedScenariosWithoutPause") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s2Violations).as(run.toString()).isEmpty();
    }
    @Property(tries = 200)
    void s2Pause(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.monitor.s2Violations).as(run.toString()).isEmpty();
    }
    @Property(tries = 200)
    void mixedNoPause(@ForAll("mixedScenariosWithoutPause") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }
    @Property(tries = 200)
    void perIdPause(@ForAll("perIdScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }
    @Property(tries = 200)
    void mixedPause(@ForAll("mixedScenarios") Scenario scenario) {
        Run run = InterleavingRunner.run(scenario, false);
        assertThat(run.notes).as(run.toString()).isEmpty();
        assertThat(CatchupLifecycleSpec.accepts(scenario, run.outcomes, run.timing(), run.observed, InterleavingRunner.IDS)).as(run.toString()).isTrue();
    }
}
