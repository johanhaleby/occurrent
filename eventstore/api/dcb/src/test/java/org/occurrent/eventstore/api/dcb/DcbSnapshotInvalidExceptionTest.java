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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    void a_refusal_is_not_marked_for_any_execute_until_an_application_service_marks_it() {
        DcbSnapshotInvalidException exception = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused");

        assertThat(exception.isBeforeAppendOf(new Object())).isFalse();
    }

    @Test
    void marking_a_refusal_as_before_the_append_of_an_execute_returns_a_marked_copy_and_leaves_the_original_unmarked() {
        DcbSnapshotInvalidException original = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused");
        Object execute = new Object();

        DcbSnapshotInvalidException marked = original.beforeAppend(execute);

        assertAll(
                () -> assertThat(marked).isNotSameAs(original),
                () -> assertThat(marked.isBeforeAppendOf(execute)).isTrue(),
                () -> assertThat(original.isBeforeAppendOf(execute)).isFalse()
        );
    }

    @Test
    void a_marked_refusal_is_not_marked_for_another_execute() {
        Object execute = new Object();

        DcbSnapshotInvalidException marked = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused").beforeAppend(execute);

        assertThat(marked.isBeforeAppendOf(new Object())).isFalse();
    }

    @Test
    void a_marked_refusal_is_not_marked_for_another_execute_that_is_equal_to_the_one_it_is_marked_for() {
        List<String> execute = new ArrayList<>(List.of("same"));
        List<String> equalToExecute = new ArrayList<>(List.of("same"));

        DcbSnapshotInvalidException marked = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused").beforeAppend(execute);

        assertAll(
                () -> assertThat(equalToExecute).as("the other execute is equal").isEqualTo(execute),
                () -> assertThat(marked.isBeforeAppendOf(execute)).as("marked for the instance it was marked with").isTrue(),
                () -> assertThat(marked.isBeforeAppendOf(equalToExecute)).as("marked for an equal instance").isFalse()
        );
    }

    @Test
    void a_refusal_cannot_be_marked_for_a_null_execute() {
        DcbSnapshotInvalidException original = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused");

        assertThatThrownBy(() -> original.beforeAppend(null)).isInstanceOf(NullPointerException.class).hasMessage("execute cannot be null");
    }

    @Test
    void an_unmarked_refusal_cannot_be_asked_whether_it_is_marked_for_a_null_execute() {
        DcbSnapshotInvalidException unmarked = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused");

        assertThatThrownBy(() -> unmarked.isBeforeAppendOf(null)).isInstanceOf(NullPointerException.class).hasMessage("execute cannot be null");
    }

    @Test
    void a_marked_refusal_cannot_be_asked_whether_it_is_marked_for_a_null_execute() {
        DcbSnapshotInvalidException marked = new DcbSnapshotInvalidException(CRITERIA, AFTER, "refused").beforeAppend(new Object());

        assertThatThrownBy(() -> marked.isBeforeAppendOf(null)).isInstanceOf(NullPointerException.class).hasMessage("execute cannot be null");
    }

    @Test
    void a_marked_refusal_keeps_the_criteria_the_versions_and_the_message_and_has_the_original_as_its_cause() {
        DcbSnapshotInvalidException original = DcbSnapshotInvalidException.eventBelowFoldedPosition(CRITERIA, AFTER, 42);

        DcbSnapshotInvalidException marked = original.beforeAppend(new Object());

        assertAll(
                () -> assertThat(marked.criteria()).isEqualTo(CRITERIA),
                () -> assertThat(marked.afterVersions()).isEqualTo(AFTER),
                () -> assertThat(marked).hasMessage(original.getMessage()),
                () -> assertThat(marked).hasCause(original)
        );
    }
}
