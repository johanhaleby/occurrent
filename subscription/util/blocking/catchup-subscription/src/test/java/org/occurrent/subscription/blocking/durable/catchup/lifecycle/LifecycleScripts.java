package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Tuple;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Kind;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Scenario;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.StartFailure;

import java.util.List;

/**
 * Scenarios of 2 to 3 threads with 1 to 3 calls each over the ids a and b. A scenario shrinks toward fewer threads, fewer
 * calls and the first id, with no injected failure.
 */
final class LifecycleScripts {

    static final List<String> IDS = List.of("a", "b");

    private LifecycleScripts() {
    }

    static Arbitrary<Scenario> perIdScenarios() {
        return scenarios(perIdCalls());
    }

    static Arbitrary<Scenario> mixedScenarios() {
        return scenarios(mixedCalls());
    }

    static Arbitrary<Scenario> perIdScenariosWithoutPause() {
        return scenarios(perIdCalls().filter(call -> call.kind() != Kind.PAUSE));
    }

    static Arbitrary<Scenario> mixedScenariosWithoutPause() {
        return scenarios(mixedCalls().filter(call -> call.kind() != Kind.PAUSE));
    }

    static Arbitrary<List<LifecycleCall>> serialScripts() {
        return mixedCalls().list().ofMinSize(1).ofMaxSize(6);
    }

    static Arbitrary<List<LifecycleCall>> serialScriptsWithoutStartTrueAfterEffect() {
        return mixedCalls().filter(call -> !(call.kind() == Kind.START_TRUE && call.failure() == StartFailure.AFTER_EFFECT)).list().ofMinSize(1).ofMaxSize(6);
    }

    private static Arbitrary<Scenario> scenarios(Arbitrary<LifecycleCall> calls) {
        Arbitrary<List<List<LifecycleCall>>> threads = calls.list().ofMinSize(1).ofMaxSize(3).list().ofMinSize(2).ofMaxSize(3);
        return Combinators.combine(Arbitraries.of(false, true), Arbitraries.of(0, 300), Arbitraries.of(0, 300), threads).as(Scenario::new);
    }

    private static Arbitrary<String> id() {
        return Arbitraries.of(IDS);
    }

    private static Arbitrary<LifecycleCall> perIdCalls() {
        return Arbitraries.frequencyOf(
                Tuple.of(3, id().map(id -> LifecycleCall.of(Kind.SUBSCRIBE, id))),
                Tuple.of(2, id().map(id -> LifecycleCall.of(Kind.CANCEL, id))),
                Tuple.of(2, id().map(id -> LifecycleCall.of(Kind.PAUSE, id))),
                Tuple.of(2, id().map(id -> LifecycleCall.of(Kind.RESUME, id))));
    }

    private static Arbitrary<LifecycleCall> mixedCalls() {
        Arbitrary<StartFailure> failure = Arbitraries.frequencyOf(
                Tuple.of(4, Arbitraries.just(StartFailure.NONE)),
                Tuple.of(1, Arbitraries.just(StartFailure.BEFORE_EFFECT)),
                Tuple.of(1, Arbitraries.just(StartFailure.AFTER_EFFECT)));
        return Arbitraries.frequencyOf(
                Tuple.of(3, id().map(id -> LifecycleCall.of(Kind.SUBSCRIBE, id))),
                Tuple.of(2, id().map(id -> LifecycleCall.of(Kind.CANCEL, id))),
                Tuple.of(2, id().map(id -> LifecycleCall.of(Kind.PAUSE, id))),
                Tuple.of(2, Combinators.combine(id(), failure).as((id, f) -> new LifecycleCall(Kind.RESUME, id, f))),
                Tuple.of(1, failure.map(f -> new LifecycleCall(Kind.START_FALSE, null, f))),
                Tuple.of(1, failure.map(f -> new LifecycleCall(Kind.START_TRUE, null, f))),
                Tuple.of(1, Arbitraries.just(LifecycleCall.of(Kind.STOP, null))));
    }
}
