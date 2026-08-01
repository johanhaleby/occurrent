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

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class PositionBackfillValidatorTest {

    private static final String COLLECTION = "events";

    @Test
    void both_messages_name_the_collection_and_the_runbook() {
        String failure = PositionBackfillValidator.unpositionedEventsExist(COLLECTION).getMessage();
        String warning = PositionBackfillValidator.unpositionedEventsMessage(COLLECTION);

        assertThat(failure).contains("'" + COLLECTION + "'").contains("doc/runbooks/position-backfill.md");
        assertThat(warning).contains("'" + COLLECTION + "'").contains("doc/runbooks/position-backfill.md");
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
