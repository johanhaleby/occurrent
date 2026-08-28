/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.subscription.RoutingOutcome.Disposition;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.RoutingOutcome.DEFERRED;
import static org.occurrent.subscription.RoutingOutcome.DELIVERED;
import static org.occurrent.subscription.RoutingOutcome.FILTERED;
import static org.occurrent.subscription.RoutingOutcome.NOT_DELIVERABLE;
import static org.occurrent.subscription.RoutingOutcome.REFUSED;
import static org.occurrent.subscription.RoutingOutcome.UNAVAILABLE;

/**
 * {@link RoutingOutcome#disposition()} is the one place the six outcomes are sorted into the four responses a
 * broker bridge has, so these tests are what the four shipped bridges rely on instead of each sorting the
 * constants themselves.
 */
class RoutingOutcomeTest {

    /**
     * The one that would lose an event if it were wrong. Acknowledging an event on any of the other four tells the
     * broker it was consumed when nothing consumed it.
     */
    @Test
    void delivered_and_filtered_are_the_outcomes_a_caller_may_acknowledge() {
        assertThat(Arrays.stream(RoutingOutcome.values()).filter(RoutingOutcome::mayAcknowledge))
                .containsExactlyInAnyOrder(DELIVERED, FILTERED);
    }

    @Test
    void deferred_and_unavailable_are_held_rather_than_sent_through_a_failure_policy() {
        assertThat(DEFERRED.disposition()).isEqualTo(Disposition.HOLD);
        assertThat(UNAVAILABLE.disposition()).isEqualTo(Disposition.HOLD);
    }

    @Test
    void not_deliverable_is_the_outcome_a_failure_policy_decides() {
        assertThat(Arrays.stream(RoutingOutcome.values()).filter(outcome -> outcome.disposition() == Disposition.FAIL))
                .containsExactly(NOT_DELIVERABLE);
    }

    @Test
    void refused_is_the_outcome_a_bridge_stops_on() {
        assertThat(Arrays.stream(RoutingOutcome.values()).filter(outcome -> outcome.disposition() == Disposition.STOP))
                .containsExactly(REFUSED);
    }

    /**
     * {@link RoutingOutcome#mayAcknowledge()} is a second way to ask a question {@link RoutingOutcome#disposition()}
     * already answers, so the two are checked against each other for all six rather than only where a bridge
     * happens to call one of them.
     */
    @ParameterizedTest
    @EnumSource(RoutingOutcome.class)
    void mayAcknowledge_answers_the_same_as_disposition(RoutingOutcome outcome) {
        assertThat(outcome.mayAcknowledge()).isEqualTo(outcome.disposition() == Disposition.ACKNOWLEDGE);
    }

    /**
     * Every outcome has a disposition, and every disposition is reachable from one. The first half is what lets a
     * bridge decide on {@code disposition()} alone, the second is what makes an exhaustive switch over
     * {@link Disposition} at a bridge cover only branches that can actually run.
     */
    @ParameterizedTest
    @EnumSource(RoutingOutcome.class)
    void every_outcome_has_a_disposition(RoutingOutcome outcome) {
        assertThat(outcome.disposition()).isNotNull();
    }

    @Test
    void every_disposition_is_reachable_from_some_outcome() {
        assertThat(Arrays.stream(RoutingOutcome.values()).map(RoutingOutcome::disposition))
                .containsAll(Arrays.asList(Disposition.values()));
    }
}
