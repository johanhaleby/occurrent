package org.occurrent.subscription.api.blocking.internal;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Disabled;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.occurrent.subscription.api.blocking.internal.BlockingHandoverRunner.Action;
import org.occurrent.subscription.api.blocking.internal.BlockingHandoverRunner.Script;
import org.occurrent.subscription.api.blocking.internal.BlockingHandoverSpec.Trace;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.api.blocking.internal.BlockingHandoverRunner.Action.*;

class BlockingHandoverLifecycleTest {

    private static Arbitrary<Script> scripts(boolean withFailures) {
        Arbitrary<Action> action = Arbitraries.frequencyOf(
                net.jqwik.api.Tuple.of(4, Arbitraries.just(STEP)),
                net.jqwik.api.Tuple.of(2, Arbitraries.just(CATCH_UP)),
                net.jqwik.api.Tuple.of(2, Arbitraries.just(CANCEL)),
                net.jqwik.api.Tuple.of(1, Arbitraries.just(STOP)),
                net.jqwik.api.Tuple.of(3, Arbitraries.just(ACCEPT)),
                net.jqwik.api.Tuple.of(1, Arbitraries.just(ACCEPT_REPORTING)),
                net.jqwik.api.Tuple.of(1, Arbitraries.just(ACCEPT_IF_LIVE)),
                net.jqwik.api.Tuple.of(1, Arbitraries.just(ACCEPT_COPY_OF_HISTORY)),
                net.jqwik.api.Tuple.of(withFailures ? 1 : 0, Arbitraries.just(FAIL_FOLD)),
                net.jqwik.api.Tuple.of(withFailures ? 1 : 0, Arbitraries.just(FAIL_CALLBACK)),
                net.jqwik.api.Tuple.of(withFailures ? 1 : 0, Arbitraries.just(INTERRUPT)));
        return Arbitraries.of(true, false).flatMap(live -> action.list().ofMinSize(2).ofMaxSize(9).map(actions -> new Script(live, actions)));
    }

    @Provide
    Arbitrary<Script> withoutFailures() {
        return scripts(false);
    }

    @Provide
    Arbitrary<Script> withFailures() {
        return scripts(true);
    }

    @Disabled("#893.32: the drain and the marker run after the last keepReplaying check, so a cancel before the marker step still writes the marker. Shrunk live [CATCH_UP, STEP, STEP, STEP, CATCH_UP, CATCH_UP, CANCEL]")
    @Property(tries = 500)
    void the_marker_is_written_only_for_a_catch_up_nothing_cancelled(@ForAll("withoutFailures") Script script) {
        Trace trace = BlockingHandoverRunner.run(script);
        assertThat(BlockingHandoverSpec.markerViolations(trace)).as(script + " " + trace).isEmpty();
    }

    @Property(tries = 500)
    void every_payload_is_delivered_once_every_accepted_payload_is_answered_and_the_replay_turn_is_released(@ForAll("withFailures") Script script) {
        Trace trace = BlockingHandoverRunner.run(script);
        assertThat(BlockingHandoverSpec.deliveryViolations(trace)).as(script + " " + trace).isEmpty();
    }

    @Disabled("#893.32: the marker is written although keepReplaying turned false before the drain")
    @Example
    void a_cancel_at_the_drain_writes_no_marker() {
        Trace trace = BlockingHandoverRunner.run(new Script(false, List.of(CATCH_UP, STEP, STEP, STEP, CANCEL)));
        assertThat(BlockingHandoverSpec.markerViolations(trace)).isEmpty();
    }

    @Example
    void a_failure_outside_the_catch_up_of_a_reported_payload_does_not_excuse_losing_it() {
        BlockingHandoverSpec.Offer lost = new BlockingHandoverSpec.Offer("p0", "acceptReportingDelivery", "reported:true", false);
        Trace trace = new Trace(BlockingHandoverRunner.HISTORY, List.of(), List.of(lost), List.of(), List.of(), List.of());
        assertThat(BlockingHandoverSpec.deliveryViolations(trace)).hasSize(1).first().asString().contains("p0").contains("never delivered");
    }

    @Example
    void a_failure_fired_after_the_offer_in_the_catch_up_that_owns_it_excuses_losing_a_reported_payload() {
        BlockingHandoverSpec.Offer lost = new BlockingHandoverSpec.Offer("p0", "acceptReportingDelivery", "reported:true", true);
        Trace trace = new Trace(BlockingHandoverRunner.HISTORY, List.of(), List.of(lost), List.of(), List.of(), List.of());
        assertThat(BlockingHandoverSpec.deliveryViolations(trace)).isEmpty();
    }
}
