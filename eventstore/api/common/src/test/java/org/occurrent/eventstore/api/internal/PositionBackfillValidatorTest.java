/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.eventstore.api.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class PositionBackfillValidatorTest {

    private static final String COLLECTION = "events";

    @ParameterizedTest
    @MethodSource("everyMessage")
    void every_message_names_the_collection_and_the_runbook(String message) {
        assertThat(message).contains("'" + COLLECTION + "'").contains("doc/runbooks/position-backfill.md");
    }

    @ParameterizedTest
    @MethodSource("everyMessage")
    void every_message_that_names_the_backfill_names_the_repair_first(String message) {
        // An event whose position the pre-0.34.0 updateEvent dropped reaches all of these looking like history that
        // predates position. Backfilling it gives it a position it never had and nothing undoes that, so a message
        // naming the backfill without naming the repair recommends the one remedy that cannot be taken back. This is
        // parameterized rather than written out per message so a fourth message cannot be added without the caveat.
        assertThat(message)
                .contains("updateEvent")
                .contains("doc/runbooks/update-event-repair.md")
                .contains("cannot be undone");
    }

    @Test
    void the_resolver_message_says_position_was_turned_off_rather_than_that_history_is_missing_it() {
        String warning = PositionBackfillValidator.positionDisabledByUnpositionedEventsMessage(COLLECTION);

        // This message is the only one a store on this path logs, and what it has to convey is that the store made a
        // decision, not that a setting is unsatisfied. The other two describe a store that kept position on.
        assertThat(warning)
                .contains("Position will NOT be used for this store")
                .contains("withStreamPosition()")
                .doesNotContain("will not start");
    }

    private static Stream<String> everyMessage() {
        return Stream.of(
                PositionBackfillValidator.unpositionedEventsExist(COLLECTION).getMessage(),
                PositionBackfillValidator.unpositionedEventsMessage(COLLECTION),
                PositionBackfillValidator.positionDisabledByUnpositionedEventsMessage(COLLECTION));
    }

    @Test
    void the_failure_message_explains_that_the_store_will_not_start() {
        String failure = PositionBackfillValidator.unpositionedEventsExist(COLLECTION).getMessage();

        assertThat(failure)
                .contains("configured to require backfilled positions")
                .contains("will not start")
                .contains("turn off requireBackfilledPosition");
    }

    @Test
    void the_warning_message_explains_what_is_silently_skipped_instead_of_claiming_a_requirement() {
        String warning = PositionBackfillValidator.unpositionedEventsMessage(COLLECTION);

        // The store starts on this path, so telling the reader it requires backfilled positions would be false and
        // would advise turning off a setting that is already off.
        assertThat(warning)
                .doesNotContain("configured to require backfilled positions")
                .doesNotContain("will not start");
        assertThat(warning)
                .contains("position-ordered reads and position-based catch-up skip")
                .contains("set requireBackfilledPosition(true) to fail");
    }
}
