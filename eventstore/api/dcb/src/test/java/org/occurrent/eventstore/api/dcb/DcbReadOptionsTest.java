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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("DcbReadOptions and marker versions")
class DcbReadOptionsTest {

    private static final DcbMarkerVersions SAVED = DcbMarkerVersions.of(Map.of("tag:a", new MarkerVersion("epoch-1", 3, 0)));

    @Nested
    @DisplayName("a read that does not ask for versions")
    class AReadThatDoesNotAskForVersions {

        @Test
        void asks_for_no_versions_and_resumes_after_none_when_it_reads_from_the_beginning() {
            DcbReadOptions options = DcbReadOptions.fromBeginning();

            assertThat(options.versionsRequested()).isFalse();
            assertThat(options.afterVersions()).isEmpty();
        }

        @Test
        void asks_for_no_versions_when_it_is_bound_by_a_position() {
            assertThat(DcbReadOptions.afterPosition(5).versionsRequested()).isFalse();
            assertThat(DcbReadOptions.upToPosition(5).versionsRequested()).isFalse();
            assertThat(DcbReadOptions.between(1, 5).versionsRequested()).isFalse();
        }

        @Test
        void asks_for_no_versions_when_it_is_built_without_the_version_arguments() {
            DcbReadOptions options = new DcbReadOptions(PositionRange.fromBeginning(), DcbReadOptions.Direction.BACKWARD, 2, OptionalInt.of(3));

            assertThat(options.versionsRequested()).isFalse();
            assertThat(options.afterVersions()).isEmpty();
        }
    }

    @Nested
    @DisplayName("a read that asks for versions")
    class AReadThatAsksForVersions {

        @Test
        void asks_for_versions_and_resumes_after_none() {
            DcbReadOptions options = DcbReadOptions.fromBeginning().withVersions();

            assertThat(options.versionsRequested()).isTrue();
            assertThat(options.afterVersions()).isEmpty();
        }

        @Test
        void can_be_bound_by_a_position_although_a_store_then_reports_no_versions() {
            DcbReadOptions options = DcbReadOptions.afterPosition(5).withVersions();

            assertThat(options.versionsRequested()).isTrue();
            assertThat(options.afterPosition()).hasValue(5);
        }

        @Test
        void keeps_asking_when_it_reads_backwards() {
            assertThat(DcbReadOptions.fromBeginning().withVersions().backwards().versionsRequested()).isTrue();
        }

        @Test
        void keeps_asking_when_it_reads_forwards_again() {
            assertThat(DcbReadOptions.fromBeginning().withVersions().backwards().forwards().versionsRequested()).isTrue();
        }

        @Test
        void keeps_asking_when_it_skips() {
            assertThat(DcbReadOptions.fromBeginning().withVersions().skip(2).versionsRequested()).isTrue();
        }

        @Test
        void keeps_asking_when_it_is_limited() {
            assertThat(DcbReadOptions.fromBeginning().withVersions().limit(2).versionsRequested()).isTrue();
        }

        @Test
        void keeps_asking_when_it_was_changed_before_it_asked() {
            DcbReadOptions options = DcbReadOptions.fromBeginning().backwards().skip(1).limit(2).withVersions();

            assertThat(options.versionsRequested()).isTrue();
            assertThat(options.direction()).isEqualTo(DcbReadOptions.Direction.BACKWARD);
            assertThat(options.skip()).isEqualTo(1);
            assertThat(options.limit()).hasValue(2);
        }
    }

    @Nested
    @DisplayName("a read after marker versions")
    class AReadAfterMarkerVersions {

        @Test
        void asks_for_versions_and_resumes_after_the_versions_it_was_given() {
            DcbReadOptions options = DcbReadOptions.afterVersions(SAVED);

            assertThat(options.versionsRequested()).isTrue();
            assertThat(options.afterVersions()).contains(SAVED);
        }

        @Test
        void reads_forwards_from_the_beginning_with_no_skip_and_no_limit() {
            DcbReadOptions options = DcbReadOptions.afterVersions(SAVED);

            assertThat(options.direction()).isEqualTo(DcbReadOptions.Direction.FORWARD);
            assertThat(options.skip()).isZero();
            assertThat(options.limit()).isEmpty();
            assertThat(options.afterPosition()).isEmpty();
            assertThat(options.upToPosition()).isEmpty();
        }

        @Test
        void rejects_null_versions() {
            assertThatThrownBy(() -> DcbReadOptions.afterVersions(null))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("Marker versions cannot be null");
        }

        @Test
        void keeps_its_versions_and_keeps_asking_when_it_reads_backwards() {
            assertResumesAfterSaved(DcbReadOptions.afterVersions(SAVED).backwards());
        }

        @Test
        void keeps_its_versions_and_keeps_asking_when_it_reads_forwards_again() {
            assertResumesAfterSaved(DcbReadOptions.afterVersions(SAVED).backwards().forwards());
        }

        @Test
        void keeps_its_versions_and_keeps_asking_when_it_skips() {
            assertResumesAfterSaved(DcbReadOptions.afterVersions(SAVED).skip(2));
        }

        @Test
        void keeps_its_versions_and_keeps_asking_when_it_is_limited() {
            assertResumesAfterSaved(DcbReadOptions.afterVersions(SAVED).limit(2));
        }

        @Test
        void keeps_its_versions_and_keeps_asking_when_it_asks_for_versions_again() {
            assertResumesAfterSaved(DcbReadOptions.afterVersions(SAVED).withVersions());
        }

        @Test
        void rejects_an_after_position_bound() {
            assertThatThrownBy(() -> new DcbReadOptions(PositionRange.afterPosition(5), DcbReadOptions.Direction.FORWARD, 0, OptionalInt.empty(), true, Optional.of(SAVED)))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Reading after marker versions cannot be combined with a position bound");
        }

        @Test
        void rejects_an_up_to_position_bound() {
            assertThatThrownBy(() -> new DcbReadOptions(PositionRange.upToPosition(5), DcbReadOptions.Direction.FORWARD, 0, OptionalInt.empty(), true, Optional.of(SAVED)))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Reading after marker versions cannot be combined with a position bound");
        }

        @Test
        void rejects_a_position_range_with_both_bounds() {
            assertThatThrownBy(() -> new DcbReadOptions(PositionRange.between(1, 5), DcbReadOptions.Direction.FORWARD, 0, OptionalInt.empty(), true, Optional.of(SAVED)))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Reading after marker versions cannot be combined with a position bound");
        }

        @Test
        void rejects_an_after_position_bound_of_zero() {
            assertThatThrownBy(() -> new DcbReadOptions(PositionRange.afterPosition(0), DcbReadOptions.Direction.FORWARD, 0, OptionalInt.empty(), true, Optional.of(SAVED)))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Reading after marker versions cannot be combined with a position bound");
        }

        @Test
        void rejects_not_asking_for_versions() {
            assertThatThrownBy(() -> new DcbReadOptions(PositionRange.fromBeginning(), DcbReadOptions.Direction.FORWARD, 0, OptionalInt.empty(), false, Optional.of(SAVED)))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Reading after marker versions requires the read to ask for versions");
        }

        @Test
        void rejects_null_after_versions_in_the_canonical_constructor() {
            assertThatThrownBy(() -> new DcbReadOptions(PositionRange.fromBeginning(), DcbReadOptions.Direction.FORWARD, 0, OptionalInt.empty(), true, null))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("After versions cannot be null");
        }

        private void assertResumesAfterSaved(DcbReadOptions options) {
            assertThat(options.versionsRequested()).isTrue();
            assertThat(options.afterVersions()).contains(SAVED);
        }
    }
}
