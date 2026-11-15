package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.Observed;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.CatchupLifecycleSpec.Timing;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.ControllableLiveModel.InjectedStartFailure;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.StartFailure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * Starts the scripts of a scenario together on real threads, waits until nothing runs in the background and reads what
 * the model reads as.
 */
final class InterleavingRunner {

    static final Set<String> IDS = new TreeSet<>(LifecycleScripts.IDS);

    static final class Run {
        final Scenario scenario;
        final ControllableLiveModel live = new ControllableLiveModel();
        final GatedHistory history = new GatedHistory(cloudEvent("1"), cloudEvent("2"));
        final ExclusiveAction monitor = new ExclusiveAction(IDS);
        final SubscriptionModel model;
        final List<List<String>> outcomes = new ArrayList<>();
        final List<String> notes = new CopyOnWriteArrayList<>();
        final AtomicLong stamps = new AtomicLong();
        final List<long[]> invoked = new ArrayList<>();
        final List<long[]> returned = new ArrayList<>();
        @Nullable Observed observed;
        final Map<String, List<String>> receivedAfterRecovery = new TreeMap<>();
        final Set<String> knownAtTheEnd = new TreeSet<>();

        Run(Scenario scenario) {
            this.scenario = scenario;
            history.delayMicros = scenario.historyDelayMicros();
            live.startDelayMicros = scenario.startDelayMicros();
            model = scenario.composite() ? new CatchupSubscriptionModel(live, history)
                    : new StreamCatchupSubscriptionModel(live, history, new CatchupSubscriptionModelConfig(1000));
        }

        Timing timing() {
            return new Timing(invoked, returned);
        }

        @Override
        public String toString() {
            return scenario + " outcomes=" + outcomes + " observed=" + observed + " notes=" + notes;
        }
    }

    private InterleavingRunner() {
    }

    static Run run(Scenario scenario, boolean recover) {
        Run run = new Run(scenario);
        try {
            runThreads(run);
            quiesce(run);
            run.observed = observe(run);
            run.knownAtTheEnd.addAll(subscribedAfterEveryCancel(run.scenario, run.outcomes, run.timing()));
            for (String id : run.knownAtTheEnd) {
                if (staysNeitherRunningNorPaused(run, id)) {
                    run.notes.add(id + " was subscribed after every cancel of it returned, but the model neither runs nor holds it paused");
                }
            }
            if (recover) {
                recover(run);
            }
        } finally {
            try {
                run.model.shutdown();
            } catch (RuntimeException e) {
                run.notes.add("shutdown threw " + e);
            }
        }
        return run;
    }

    /** Both reads are false for a moment while a subscribe-started catch-up hands over, so only an id that stays so counts as lost */
    private static boolean staysNeitherRunningNorPaused(Run run, String id) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        do {
            if (run.model.isRunning(id) || run.model.isPaused(id)) {
                return false;
            }
            LockSupport.parkNanos(5_000_000);
        } while (System.nanoTime() < deadline);
        return true;
    }

    /**
     * The ids that must be known at the end. An id counts when some subscribe of it returned ok and every cancel of it in the
     * scenario returned before that subscribe was made. A cancel that never returned has Long.MAX_VALUE, so it never qualifies.
     */
    static Set<String> subscribedAfterEveryCancel(Scenario scenario, List<List<String>> outcomes, Timing timing) {
        Set<String> ids = new TreeSet<>();
        for (int t = 0; t < scenario.threads().size(); t++) {
            List<LifecycleCall> script = scenario.threads().get(t);
            for (int i = 0; i < script.size(); i++) {
                LifecycleCall subscribe = script.get(i);
                if (subscribe.kind() == LifecycleCall.Kind.SUBSCRIBE && "ok".equals(outcomes.get(t).get(i))
                        && everyCancelReturnedBefore(scenario, timing, subscribe.id(), timing.invoked().get(t)[i])) {
                    ids.add(subscribe.id());
                }
            }
        }
        return ids;
    }

    private static boolean everyCancelReturnedBefore(Scenario scenario, Timing timing, String id, long subscribeInvokedAt) {
        for (int t = 0; t < scenario.threads().size(); t++) {
            List<LifecycleCall> script = scenario.threads().get(t);
            for (int i = 0; i < script.size(); i++) {
                LifecycleCall call = script.get(i);
                if (call.kind() == LifecycleCall.Kind.CANCEL && id.equals(call.id()) && timing.returned().get(t)[i] >= subscribeInvokedAt) {
                    return false;
                }
            }
        }
        return true;
    }

    private static void runThreads(Run run) {
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        List<AtomicReferenceArray<String>> slots = new ArrayList<>();
        List<AtomicLongArray> invokedAt = new ArrayList<>();
        List<AtomicLongArray> returnedAt = new ArrayList<>();
        for (List<LifecycleCall> script : run.scenario.threads()) {
            AtomicReferenceArray<String> slot = new AtomicReferenceArray<>(script.size());
            slots.add(slot);
            AtomicLongArray invoked = neverStamped(script.size());
            AtomicLongArray returned = neverStamped(script.size());
            invokedAt.add(invoked);
            returnedAt.add(returned);
            threads.add(Thread.ofPlatform().start(() -> {
                await(go);
                for (int i = 0; i < script.size(); i++) {
                    invoked.set(i, run.stamps.incrementAndGet());
                    String outcome = execute(run, script.get(i));
                    returned.set(i, run.stamps.incrementAndGet());
                    slot.set(i, outcome);
                }
            }));
        }
        go.countDown();
        for (int t = 0; t < threads.size(); t++) {
            Thread thread = threads.get(t);
            try {
                thread.join(20_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            List<LifecycleCall> script = run.scenario.threads().get(t);
            List<String> outcomes = new ArrayList<>();
            int firstHung = -1;
            for (int i = 0; i < script.size(); i++) {
                String outcome = slots.get(t).get(i);
                if (outcome == null && firstHung < 0) {
                    firstHung = i;
                }
                outcomes.add(outcome == null ? "hung" : outcome);
            }
            if (firstHung >= 0) {
                run.notes.add("thread " + t + " did not return from " + script.get(firstHung));
                thread.interrupt();
            }
            run.outcomes.add(outcomes);
            run.invoked.add(toArray(invokedAt.get(t)));
            run.returned.add(toArray(returnedAt.get(t)));
        }
    }

    /** A call that never started or never returned has no stamp, so it orders nothing */
    private static AtomicLongArray neverStamped(int size) {
        AtomicLongArray array = new AtomicLongArray(size);
        for (int i = 0; i < size; i++) {
            array.set(i, Long.MAX_VALUE);
        }
        return array;
    }

    private static long[] toArray(AtomicLongArray array) {
        long[] result = new long[array.length()];
        for (int i = 0; i < result.length; i++) {
            result[i] = array.get(i);
        }
        return result;
    }

    static String execute(Run run, LifecycleCall call) {
        ExclusiveAction.Generation made = call.kind() == LifecycleCall.Kind.SUBSCRIBE ? run.monitor.newGeneration(call.id()) : null;
        run.monitor.begin(call);
        String outcome = "ok";
        run.live.failStartOnThisThread.set(call.failure());
        try {
            invoke(run, call, made);
        } catch (Throwable e) {
            outcome = e.getClass().getSimpleName();
            if (!(e instanceof RuntimeException)) {
                run.notes.add(call + " threw " + e);
            } else if (e instanceof IllegalStateException && !(e instanceof InjectedStartFailure)) {
                run.notes.add(call + " threw " + e);
            }
        } finally {
            run.live.failStartOnThisThread.set(StartFailure.NONE);
        }
        run.monitor.end(call, made, outcome.equals("ok"));
        return outcome;
    }

    private static void invoke(Run run, LifecycleCall call, ExclusiveAction.@Nullable Generation made) {
        SubscriptionModel model = run.model;
        switch (call.kind()) {
            case SUBSCRIBE -> model.subscribe(call.id(), StartAtTime.beginningOfTime(), run.monitor.actionOf(made));
            case CANCEL -> model.cancelSubscription(call.id());
            case PAUSE -> model.pauseSubscription(call.id());
            case RESUME -> model.resumeSubscription(call.id());
            case START_FALSE -> model.start(false);
            case START_TRUE -> model.start(true);
            case STOP -> model.stop();
        }
    }

    static void quiesce(Run run) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        String last = null;
        int stable = 0;
        while (System.nanoTime() < deadline) {
            boolean idle = run.history.readsInFlight.get() == 0 && run.monitor.idle() && IDS.stream().noneMatch(id -> ((ReplayAwareSubscriptions) run.model).isCatchingUp(id));
            String snapshot = observe(run) + " live=" + run.live.running + IDS.stream().map(run.live::isRunning).toList();
            if (idle && snapshot.equals(last)) {
                if (++stable >= 3) {
                    return;
                }
            } else {
                stable = 0;
            }
            last = snapshot;
            LockSupport.parkNanos(3_000_000);
        }
        run.notes.add("did not quiesce");
    }

    static Observed observe(Run run) {
        Map<String, Boolean> running = new TreeMap<>();
        Map<String, Boolean> paused = new TreeMap<>();
        for (String id : IDS) {
            running.put(id, run.model.isRunning(id));
            paused.put(id, run.model.isPaused(id));
        }
        return new Observed(run.model.isRunning(), running, paused);
    }

    /** start(true), then a resume of whatever reads paused, then one live event */
    private static void recover(Run run) {
        try {
            run.model.start(true);
        } catch (RuntimeException e) {
            run.notes.add("recovery start(true) threw " + e);
        }
        for (String id : run.knownAtTheEnd) {
            if (run.model.isPaused(id)) {
                try {
                    run.model.resumeSubscription(id);
                } catch (RuntimeException e) {
                    run.notes.add("recovery resume(" + id + ") threw " + e);
                }
            }
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline && !run.knownAtTheEnd.stream().allMatch(id -> run.live.isRunning(id))) {
            LockSupport.parkNanos(5_000_000);
        }
        for (String id : run.knownAtTheEnd) {
            if (!run.live.isRunning(id)) {
                run.notes.add(id + " does not run in the live model after the recovery, isPaused=" + run.model.isPaused(id));
            }
        }
        run.live.publish(cloudEvent("L"));
        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1500);
        while (System.nanoTime() < deadline && !run.knownAtTheEnd.stream().allMatch(id -> currentReceived(run, id).contains("L"))) {
            LockSupport.parkNanos(5_000_000);
        }
        quiesce(run);
        for (String id : run.knownAtTheEnd) {
            run.receivedAfterRecovery.put(id, currentReceived(run, id));
        }
    }

    private static List<String> currentReceived(Run run, String id) {
        ExclusiveAction.Generation generation = run.monitor.currentGeneration(id);
        return generation == null ? List.of("no generation") : List.copyOf(generation.received);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
