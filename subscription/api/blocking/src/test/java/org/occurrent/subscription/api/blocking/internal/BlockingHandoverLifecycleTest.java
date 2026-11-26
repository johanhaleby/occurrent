package org.occurrent.subscription.api.blocking.internal;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
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

    @Property(tries = 500)
    void the_marker_is_written_only_for_a_catch_up_nothing_cancelled(@ForAll("withoutFailures") Script script) {
        Trace trace = BlockingHandoverRunner.run(script);
        assertThat(BlockingHandoverSpec.markerViolations(trace)).as(script + " " + trace).isEmpty();
        assertThat(trace.notes()).as(script + " " + trace).isEmpty();
    }

    @Property(tries = 500)
    void a_catch_up_that_was_cancelled_before_it_returned_does_not_return_true(@ForAll("withoutFailures") Script script) {
        Trace trace = BlockingHandoverRunner.run(script);
        assertThat(BlockingHandoverSpec.stoppedCatchUpViolations(trace)).as(script + " " + trace).isEmpty();
    }

    @Example
    void a_cancel_at_the_drain_does_not_let_the_catch_up_return_true() {
        Trace trace = BlockingHandoverRunner.run(new Script(false, List.of(CATCH_UP, STEP, STEP, STEP, CANCEL)));
        assertThat(BlockingHandoverSpec.stoppedCatchUpViolations(trace)).isEmpty();
    }

    @Example
    void a_catch_up_cancelled_before_it_returned_that_returned_true_is_a_violation() {
        Trace trace = new Trace(BlockingHandoverRunner.HISTORY, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(new BlockingHandoverSpec.CatchUp("true", true)));
        assertThat(BlockingHandoverSpec.stoppedCatchUpViolations(trace)).hasSize(1);
    }

    @Example
    void an_applied_copy_of_the_history_that_delivered_nothing_is_a_violation() {
        BlockingHandoverSpec.Offer applied = new BlockingHandoverSpec.Offer("h1", "accept", "applied", false);
        Trace trace = new Trace(BlockingHandoverRunner.HISTORY, List.of(), List.of(applied), List.of(), List.of(), List.of(), List.of());
        assertThat(BlockingHandoverSpec.deliveryViolations(trace)).hasSize(1).first().asString().contains("h1").contains("never delivered");
    }

    @Property(tries = 500)
    void every_payload_is_delivered_once_every_accepted_payload_is_answered_and_the_replay_turn_is_released(@ForAll("withFailures") Script script) {
        Trace trace = BlockingHandoverRunner.run(script);
        assertThat(BlockingHandoverSpec.deliveryViolations(trace)).as(script + " " + trace).isEmpty();
    }

    @Example
    void a_cancel_at_the_drain_writes_no_marker() {
        Trace trace = BlockingHandoverRunner.run(new Script(false, List.of(CATCH_UP, STEP, STEP, STEP, CANCEL)));
        assertThat(BlockingHandoverSpec.markerViolations(trace)).isEmpty();
        assertThat(trace.notes()).isEmpty();
    }

    @Property(tries = 500)
    void a_payload_offered_after_a_drain_started_is_delivered_after_every_payload_offered_before_it_and_no_two_drains_overlap(@ForAll("withFailures") Script script) {
        Trace trace = BlockingHandoverRunner.run(script);
        assertThat(BlockingHandoverSpec.orderViolations(trace)).as(script + " " + trace).isEmpty();
    }

    @Example
    void a_payload_fed_while_the_drain_delivers_is_delivered_behind_the_one_buffered_before_the_catch_up() {
        Trace trace = BlockingHandoverRunner.run(new Script(false, List.of(ACCEPT_REPORTING, HOLD_DRAIN, CATCH_UP, STEP, STEP, STEP, STEP, ACCEPT_REPORTING)));
        assertThat(trace.drainStarts()).as("the drain had started when the second payload was fed").hasSize(2);
        assertThat(BlockingHandoverSpec.orderViolations(trace)).as(trace.toString()).isEmpty();
        assertThat(trace.delivered()).containsSubsequence("p0", "p1");
    }

    @Example
    void a_payload_that_reaches_deliver_ahead_of_one_offered_before_the_drain_is_a_violation() {
        BlockingHandoverSpec.Offer older = new BlockingHandoverSpec.Offer("p0", "acceptReportingDelivery", "reported:true", false, 1);
        BlockingHandoverSpec.Offer newer = new BlockingHandoverSpec.Offer("p1", "acceptReportingDelivery", "reported:true", false, 3);
        Trace trace = new Trace(BlockingHandoverRunner.HISTORY, List.of("p1", "p0"), List.of(older, newer), List.of(), List.of(), List.of(), List.of(), List.of(2L), List.of());
        assertThat(BlockingHandoverSpec.orderViolations(trace)).hasSize(1).first().asString().contains("p1").contains("p0");
    }

    @Example
    void two_catch_ups_delivering_at_once_is_a_violation() {
        Trace trace = new Trace(BlockingHandoverRunner.HISTORY, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of("two catch-ups were delivering at once"));
        assertThat(BlockingHandoverSpec.orderViolations(trace)).hasSize(1);
    }

    @Example
    void a_failure_outside_the_catch_up_of_a_reported_payload_does_not_excuse_losing_it() {
        BlockingHandoverSpec.Offer lost = new BlockingHandoverSpec.Offer("p0", "acceptReportingDelivery", "reported:true", false);
        Trace trace = new Trace(BlockingHandoverRunner.HISTORY, List.of(), List.of(lost), List.of(), List.of(), List.of(), List.of());
        assertThat(BlockingHandoverSpec.deliveryViolations(trace)).hasSize(1).first().asString().contains("p0").contains("never delivered");
    }

    @Example
    void a_failure_fired_after_the_offer_in_the_catch_up_that_owns_it_excuses_losing_a_reported_payload() {
        BlockingHandoverSpec.Offer lost = new BlockingHandoverSpec.Offer("p0", "acceptReportingDelivery", "reported:true", true);
        Trace trace = new Trace(BlockingHandoverRunner.HISTORY, List.of(), List.of(lost), List.of(), List.of(), List.of(), List.of());
        assertThat(BlockingHandoverSpec.deliveryViolations(trace)).isEmpty();
    }

    @Example
    void a_fold_failure_in_the_catch_up_that_drains_a_payload_buffered_while_a_stopped_replay_was_open_counts_as_after_its_offer() {
        Trace trace = BlockingHandoverRunner.run(new Script(false, List.of(CATCH_UP, CANCEL, ACCEPT_REPORTING, FAIL_FOLD)));
        assertThat(trace.offers()).hasSize(1).first().extracting(BlockingHandoverSpec.Offer::failureFiredAfterInItsCatchUp).isEqualTo(true);
    }

    @Example
    void a_recording_callback_that_throws_changes_only_the_answer_of_its_own_payload_and_the_catch_up_writes_its_marker() {
        Trace trace = BlockingHandoverRunner.run(new Script(false, List.of(ACCEPT_REPORTING, ACCEPT_COPY_OF_HISTORY, FAIL_CALLBACK, CATCH_UP)));

        assertThat(trace.catchUps().get(0).result()).as("the catch-up that goes on past a throwing callback, which wrote its marker").isEqualTo("true");
        assertThat(trace.offers()).extracting(BlockingHandoverSpec.Offer::payload, BlockingHandoverSpec.Offer::answer)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("p0", "reported:true"), org.assertj.core.groups.Tuple.tuple("h1", "refused:PreDispatchRefusalException"));
        assertThat(trace.delivered()).as("every other payload is delivered once").containsExactlyInAnyOrder("h1", "h2", "p0");
        assertThat(trace.offers()).as("a callback failure excuses no lost payload").noneMatch(BlockingHandoverSpec.Offer::failureFiredAfterInItsCatchUp);
        assertThat(BlockingHandoverSpec.deliveryViolations(trace)).as(trace.toString()).isEmpty();
        assertThat(BlockingHandoverSpec.markerViolations(trace)).isEmpty();
    }

    @Example
    void a_failure_fired_in_a_catch_up_that_ended_before_the_offer_does_not_count() {
        Trace trace = BlockingHandoverRunner.run(new Script(false, List.of(CATCH_UP, INTERRUPT, ACCEPT_REPORTING)));
        assertThat(trace.offers()).hasSize(1).first().extracting(BlockingHandoverSpec.Offer::failureFiredAfterInItsCatchUp).isEqualTo(false);
    }
}
