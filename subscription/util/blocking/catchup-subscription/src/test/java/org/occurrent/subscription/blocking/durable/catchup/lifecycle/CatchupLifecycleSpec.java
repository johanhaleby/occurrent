package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.StartFailure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The serial reference model of the lifecycle calls of one catch-up model over a live model that holds a subscription
 * paused until it is resumed. A run is accepted when some merge of the thread scripts, in which every thread keeps its
 * program order, gives each call the outcome it had and the end state the run ended in. The replay finishing and going
 * live is a step of its own that can fall anywhere after the call that started the replay.
 */
final class CatchupLifecycleSpec {

    enum Phase {
        /** A replay reads history for a subscription that is not live yet */
        REPLAYING,
        /** A replay waits to run again, since the model was stopped or a start failed to run it */
        PARKED,
        /** The live model runs the subscription */
        RUNNING,
        /** The live model holds the subscription paused */
        PAUSED
    }

    record State(boolean running, Map<String, Phase> subscriptions) {
        State {
            subscriptions = new TreeMap<>(subscriptions);
        }

        State with(String id, Phase phase) {
            Map<String, Phase> copy = new TreeMap<>(subscriptions);
            copy.put(id, phase);
            return new State(running, copy);
        }

        State without(String id) {
            Map<String, Phase> copy = new TreeMap<>(subscriptions);
            copy.remove(id);
            return new State(running, copy);
        }

        State running(boolean running) {
            return new State(running, subscriptions);
        }

        State mapPhases(java.util.function.UnaryOperator<Phase> change) {
            Map<String, Phase> copy = new TreeMap<>();
            subscriptions.forEach((id, phase) -> copy.put(id, change.apply(phase)));
            return new State(running, copy);
        }
    }

    record Step(String outcome, State next) {
    }

    /** What a model reads as once nothing runs in the background */
    record Observed(boolean modelRunning, Map<String, Boolean> isRunning, Map<String, Boolean> isPaused) {
    }

    static final String OK = "ok";
    static final String DUPLICATE = "DuplicateSubscriptionIdException";
    static final String UNKNOWN = "UnknownSubscriptionException";
    static final String NOT_RUNNING = "SubscriptionNotRunningException";
    static final String ALREADY_RUNNING = "SubscriptionAlreadyRunningException";
    static final String INJECTED = "InjectedStartFailure";

    private CatchupLifecycleSpec() {
    }

    static State initial() {
        return new State(true, Map.of());
    }

    static Step apply(State state, LifecycleCall call) {
        String id = call.id();
        Phase phase = id == null ? null : state.subscriptions().get(id);
        return switch (call.kind()) {
            case SUBSCRIBE -> phase != null ? new Step(DUPLICATE, state) : new Step(OK, state.with(id, state.running() ? Phase.REPLAYING : Phase.PARKED));
            case CANCEL -> new Step(OK, state.without(id));
            case PAUSE -> {
                if (phase == null) {
                    yield new Step(UNKNOWN, state);
                }
                yield switch (phase) {
                    case RUNNING -> new Step(OK, state.with(id, Phase.PAUSED));
                    // The spec does not tell held paused from parked, since every modelled call gives the same result for both
                    case REPLAYING -> new Step(OK, state.with(id, Phase.PARKED));
                    case PARKED, PAUSED -> new Step(NOT_RUNNING, state);
                };
            }
            case RESUME -> resume(state, id, phase, call.failure());
            case STOP -> new Step(OK, state.running(false).mapPhases(p -> p == Phase.REPLAYING ? Phase.PARKED : p == Phase.RUNNING ? Phase.PAUSED : p));
            case START_FALSE -> start(state, false, call.failure());
            case START_TRUE -> start(state, true, call.failure());
        };
    }

    private static Step resume(State state, String id, Phase phase, StartFailure failure) {
        if (phase == null) {
            return new Step(UNKNOWN, state);
        }
        return switch (phase) {
            case RUNNING -> new Step(ALREADY_RUNNING, state);
            case REPLAYING -> new Step(OK, state);
            case PARKED, PAUSED -> {
                State started = state;
                if (!state.running()) {
                    if (failure == StartFailure.BEFORE_EFFECT) {
                        yield new Step(INJECTED, state);
                    }
                    started = state.running(true);
                    if (failure == StartFailure.AFTER_EFFECT) {
                        yield new Step(INJECTED, started);
                    }
                }
                yield new Step(OK, started.with(id, phase == Phase.PARKED ? Phase.REPLAYING : Phase.RUNNING));
            }
        };
    }

    private static Step start(State state, boolean resumeAutomatically, StartFailure failure) {
        if (failure == StartFailure.BEFORE_EFFECT) {
            return new Step(INJECTED, state);
        }
        State started = state.running(true);
        if (failure == StartFailure.AFTER_EFFECT) {
            // The exception reaches the caller, and everything else is as after a start(..) that returned
            return new Step(INJECTED, resumeAutomatically ? started.mapPhases(p -> p == Phase.PARKED ? Phase.REPLAYING : p == Phase.PAUSED ? Phase.RUNNING : p) : started);
        }
        return new Step(OK, resumeAutomatically ? started.mapPhases(p -> p == Phase.PARKED ? Phase.REPLAYING : p == Phase.PAUSED ? Phase.RUNNING : p) : started);
    }

    /** The states one replay finishing can lead to */
    static List<State> replaysFinishing(State state) {
        List<State> result = new ArrayList<>();
        state.subscriptions().forEach((id, phase) -> {
            if (phase == Phase.REPLAYING) {
                result.add(state.with(id, Phase.RUNNING));
            }
        });
        return result;
    }

    static State settle(State state) {
        return state.mapPhases(p -> p == Phase.REPLAYING ? Phase.RUNNING : p);
    }

    static Observed observe(State state, Set<String> ids) {
        Map<String, Boolean> running = new TreeMap<>();
        Map<String, Boolean> paused = new TreeMap<>();
        for (String id : ids) {
            Phase phase = state.subscriptions().get(id);
            running.put(id, phase == Phase.RUNNING);
            paused.put(id, phase == Phase.PAUSED || phase == Phase.PARKED);
        }
        return new Observed(state.running(), running, paused);
    }

    /**
     * When each call was made and when it returned, on one shared counter. invoked.get(t)[i] and returned.get(t)[i] belong to
     * the i-th call of thread t. A call that never returned has Long.MAX_VALUE.
     */
    record Timing(List<long[]> invoked, List<long[]> returned) {
    }

    /**
     * Whether some merge of the thread scripts explains the outcomes and the end state. outcomes.get(t).get(i) is what the
     * i-th call of thread t gave. A merge keeps each thread's order and puts a call that returned before another call was
     * made ahead of that call.
     */
    static boolean accepts(Scenario scenario, List<List<String>> outcomes, Timing timing, Observed observed, Set<String> ids) {
        return accepts(scenario.threads(), outcomes, timing, observed, ids, initial(), new int[scenario.threads().size()], new HashSet<>());
    }

    private static boolean accepts(List<List<LifecycleCall>> threads, List<List<String>> outcomes, Timing timing, Observed observed, Set<String> ids, State state, int[] positions,
                                   Set<String> failed) {
        // The real-time order depends on the positions only, so the key stays complete
        String key = java.util.Arrays.toString(positions) + state;
        if (failed.contains(key)) {
            return false;
        }
        boolean done = true;
        for (int t = 0; t < threads.size(); t++) {
            if (positions[t] < threads.get(t).size()) {
                done = false;
                if (!mayGoNext(timing, positions, t)) {
                    continue;
                }
                LifecycleCall call = threads.get(t).get(positions[t]);
                Step step = apply(state, call);
                if (step.outcome().equals(outcomes.get(t).get(positions[t]))) {
                    positions[t]++;
                    boolean accepted = accepts(threads, outcomes, timing, observed, ids, step.next(), positions, failed);
                    positions[t]--;
                    if (accepted) {
                        return true;
                    }
                }
            }
        }
        for (State next : replaysFinishing(state)) {
            if (accepts(threads, outcomes, timing, observed, ids, next, positions, failed)) {
                return true;
            }
        }
        if (done && observe(settle(state), ids).equals(observed)) {
            return true;
        }
        failed.add(key);
        return false;
    }

    /** False when the first call another thread has not applied yet returned before this thread's next call was made */
    private static boolean mayGoNext(Timing timing, int[] positions, int t) {
        long madeAt = timing.invoked().get(t)[positions[t]];
        for (int u = 0; u < positions.length; u++) {
            if (u != t && positions[u] < timing.returned().get(u).length && timing.returned().get(u)[positions[u]] < madeAt) {
                return false;
            }
        }
        return true;
    }
}
