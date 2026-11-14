package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import net.jqwik.api.Example;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.InterleavingRunner.Run;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Kind;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class TmpS2Probe {
    @Example
    void gatedReplayThenStop() throws Exception {
        for (boolean composite : new boolean[]{false, true}) {
            Run run = new Run(new Scenario(composite, 0, 0, List.of(List.of())));
            try {
                run.history.gate = new CountDownLatch(1);
                System.out.println("PROBE sub=" + InterleavingRunner.execute(run, LifecycleCall.of(Kind.SUBSCRIBE, "a")));
                System.out.println("PROBE queried=" + run.history.queried.await(5, TimeUnit.SECONDS));
                System.out.println("PROBE stop=" + InterleavingRunner.execute(run, LifecycleCall.of(Kind.STOP, null)));
                run.history.gate.countDown();
                InterleavingRunner.quiesce(run);
                System.out.println("PROBE composite=" + composite + " s2=" + run.monitor.s2Violations + " liveSubs=" + run.live.subscriptions.keySet() + " notes=" + run.notes + " observed=" + InterleavingRunner.observe(run));
            } finally {
                run.history.gate.countDown();
                run.model.shutdown();
            }
        }
    }

    @Example
    void settledSubscribeThenStopThenRogue() throws Exception {
        Run run = new Run(new Scenario(false, 0, 0, List.of(List.of())));
        try {
            System.out.println("PROBE2 sub=" + InterleavingRunner.execute(run, LifecycleCall.of(Kind.SUBSCRIBE, "a")));
            InterleavingRunner.quiesce(run);
            System.out.println("PROBE2 stop=" + InterleavingRunner.execute(run, LifecycleCall.of(Kind.STOP, null)));
            var f = run.live.subscriptions.get("a");
            System.out.println("PROBE2 live sub present=" + (f != null));
            if (f != null) f.rogue(GatedHistory.cloudEvent("R"));
            InterleavingRunner.quiesce(run);
            System.out.println("PROBE2 s2=" + run.monitor.s2Violations + " received=" + run.monitor.currentGeneration("a"));
        } finally {
            run.model.shutdown();
        }
    }
}
