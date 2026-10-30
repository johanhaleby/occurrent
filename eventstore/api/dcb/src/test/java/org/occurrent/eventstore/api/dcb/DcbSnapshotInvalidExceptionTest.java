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

package org.occurrent.eventstore.api.dcb;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbSnapshotInvalidExceptionTest {

    private static final DcbCriteria CRITERIA = DcbCriteria.tags(Tag.parse("name:1"));
    private static final DcbMarkerVersions AFTER = DcbMarkerVersions.of(Map.of("tag:name:1", new MarkerVersion("epoch-1", 3, 0)));

    @Test
    void an_event_below_the_folded_position_names_the_position_in_the_message() {
        DcbSnapshotInvalidException exception = DcbSnapshotInvalidException.eventBelowFoldedPosition(CRITERIA, AFTER, 42);

        assertThat(exception).hasMessageContaining("below position 42");
    }

    @Test
    void an_event_below_the_folded_position_keeps_the_criteria_and_the_versions_of_the_refused_read() {
        DcbSnapshotInvalidException exception = DcbSnapshotInvalidException.eventBelowFoldedPosition(CRITERIA, AFTER, 42);

        assertThat(exception.criteria()).isEqualTo(CRITERIA);
        assertThat(exception.afterVersions()).isEqualTo(AFTER);
    }

    @Test
    void a_refusal_is_not_marked_as_before_the_append_until_an_application_service_marks_it() {
        DcbSnapshotInvalidException exception = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused");

        assertThat(exception.isBeforeAppend()).isFalse();
    }

    @Test
    void marking_a_refusal_as_before_the_append_returns_a_marked_copy_and_leaves_the_original_unmarked() {
        DcbSnapshotInvalidException original = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused");

        DcbSnapshotInvalidException marked = original.beforeAppend();

        assertAll(
                () -> assertThat(marked).isNotSameAs(original),
                () -> assertThat(marked.isBeforeAppend()).isTrue(),
                () -> assertThat(original.isBeforeAppend()).isFalse()
        );
    }

    @Test
    void a_marked_refusal_keeps_the_criteria_the_versions_and_the_message_and_has_the_original_as_its_cause() {
        DcbSnapshotInvalidException original = DcbSnapshotInvalidException.eventBelowFoldedPosition(CRITERIA, AFTER, 42);

        DcbSnapshotInvalidException marked = original.beforeAppend();

        assertAll(
                () -> assertThat(marked.criteria()).isEqualTo(CRITERIA),
                () -> assertThat(marked.afterVersions()).isEqualTo(AFTER),
                () -> assertThat(marked).hasMessage(original.getMessage()),
                () -> assertThat(marked).hasCause(original)
        );
    }
}
