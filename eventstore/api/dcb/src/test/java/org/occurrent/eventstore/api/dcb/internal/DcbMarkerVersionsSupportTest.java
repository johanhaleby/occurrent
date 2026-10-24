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

package org.occurrent.eventstore.api.dcb.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport.ObservedMarker;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("DcbMarkerVersionsSupport")
class DcbMarkerVersionsSupportTest {

    private static final Tag A = Tag.of("account", "a");
    private static final Tag B = Tag.of("account", "b");
    private static final String KEY_A = "tag:account:a";
    private static final String KEY_B = "tag:account:b";
    private static final String EPOCH = "epoch-1";

    @Nested
    @DisplayName("the marker keys of criteria")
    class TheMarkerKeysOfCriteria {

        @Test
        void is_a_single_key_for_criteria_that_match_every_event() {
            assertThat(DcbMarkerVersionsSupport.queryMarkerKeys(DcbCriteria.all())).containsExactly("all");
        }

        @Test
        void is_one_key_per_tag_and_one_per_type_and_never_a_key_that_combines_a_tag_and_a_type() {
            DcbCriteria criteria = DcbCriteria.types("Deposited", "Withdrawn").tags(A, B);

            assertThat(DcbMarkerVersionsSupport.queryMarkerKeys(criteria))
                    .containsExactly(KEY_A, KEY_B, "type:Deposited", "type:Withdrawn");
        }

        @Test
        void is_the_union_of_the_keys_of_every_item_without_repeating_a_key() {
            DcbCriteria criteria = DcbCriteria.anyOf(DcbCriteria.tags(A), DcbCriteria.tags(A, B), DcbCriteria.type("Deposited"));

            assertThat(DcbMarkerVersionsSupport.queryMarkerKeys(criteria)).containsExactly(KEY_A, KEY_B, "type:Deposited");
        }

        @Test
        void ignores_the_excluded_types_of_an_item() {
            DcbCriteria criteria = DcbCriteria.tags(A).excludingTypes("Closed");

            assertThat(DcbMarkerVersionsSupport.queryMarkerKeys(criteria)).containsExactly(KEY_A);
        }
    }

    @Nested
    @DisplayName("the selection keys of an item")
    class TheSelectionKeysOfAnItem {

        @Test
        void is_the_tag_key_of_an_item_with_one_tag() {
            assertThat(DcbMarkerVersionsSupport.selectionKeys(DcbCriteria.tags(A))).containsExactly(KEY_A);
        }

        @Test
        void is_only_the_first_tag_key_in_canonical_order_of_an_item_with_several_tags() {
            assertThat(DcbMarkerVersionsSupport.selectionKeys(DcbCriteria.tags(B, A))).containsExactly(KEY_A);
            assertThat(DcbMarkerVersionsSupport.selectionKeys(DcbCriteria.tags(Tag.of("zebra", "1"), Tag.of("alpha", "9"), Tag.of("mid", "5"))))
                    .containsExactly("tag:alpha:9");
        }

        @Test
        void is_the_tag_key_and_no_type_key_of_an_item_with_both_tags_and_types() {
            assertThat(DcbMarkerVersionsSupport.selectionKeys(DcbCriteria.types("Deposited").tags(B))).containsExactly(KEY_B);
        }

        @Test
        void is_one_type_key_per_type_in_ascending_order_of_an_item_without_tags() {
            assertThat(DcbMarkerVersionsSupport.selectionKeys(DcbCriteria.types("Withdrawn", "Deposited")))
                    .containsExactly("type:Deposited", "type:Withdrawn");
        }

        @Test
        void is_one_type_key_of_an_item_with_a_single_type() {
            assertThat(DcbMarkerVersionsSupport.selectionKeys(DcbCriteria.type("Deposited"))).containsExactly("type:Deposited");
        }

        @Test
        void ignores_the_excluded_types_of_an_item() {
            assertThat(DcbMarkerVersionsSupport.selectionKeys(DcbCriteria.tags(A).excludingTypes("Closed"))).containsExactly(KEY_A);
            assertThat(DcbMarkerVersionsSupport.selectionKeys(DcbCriteria.type("Deposited").excludingTypes("Closed"))).containsExactly("type:Deposited");
        }
    }

    @Nested
    @DisplayName("whether a read may report versions")
    class WhetherAReadMayReportVersions {

        @Test
        void is_yes_for_a_full_forward_read_that_asks_for_versions() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.fromBeginning().withVersions())).isTrue();
        }

        @Test
        void is_yes_for_a_read_after_versions() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.afterVersions(versions(KEY_A, 3, 0)))).isTrue();
        }

        @Test
        void is_yes_for_a_read_that_asks_after_going_backwards_and_forwards_again() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.fromBeginning().backwards().forwards().withVersions())).isTrue();
        }

        @Test
        void is_no_for_a_read_that_does_not_ask() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.fromBeginning())).isFalse();
        }

        @Test
        void is_no_for_a_read_with_a_limit() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.fromBeginning().withVersions().limit(10))).isFalse();
        }

        @Test
        void is_no_for_a_read_with_a_skip() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.fromBeginning().withVersions().skip(1))).isFalse();
        }

        @Test
        void is_no_for_a_backward_read() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.fromBeginning().withVersions().backwards())).isFalse();
        }

        @Test
        void is_no_for_a_read_with_a_lower_position_bound_because_an_event_that_commits_late_below_it_would_never_be_read() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.afterPosition(5).withVersions())).isFalse();
        }

        @Test
        void is_no_for_a_read_with_a_lower_position_bound_of_zero() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.afterPosition(0).withVersions())).isFalse();
        }

        @Test
        void is_no_for_a_read_with_an_upper_position_bound() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.upToPosition(5).withVersions())).isFalse();
        }

        @Test
        void is_no_for_a_read_between_two_positions() {
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(DcbReadOptions.between(1, 5).withVersions())).isFalse();
        }

        @Test
        void is_no_for_a_read_after_versions_that_is_limited_skips_or_goes_backwards() {
            DcbReadOptions afterVersions = DcbReadOptions.afterVersions(versions(KEY_A, 3, 0));

            assertThat(DcbMarkerVersionsSupport.mayReportVersions(afterVersions.limit(1))).isFalse();
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(afterVersions.skip(1))).isFalse();
            assertThat(DcbMarkerVersionsSupport.mayReportVersions(afterVersions.backwards())).isFalse();
        }

        @Test
        void is_no_for_options_that_ask_with_a_position_range_and_every_other_default() {
            DcbReadOptions options = new DcbReadOptions(PositionRange.afterPosition(2), DcbReadOptions.Direction.FORWARD, 0, OptionalInt.empty(), true, Optional.empty());

            assertThat(DcbMarkerVersionsSupport.mayReportVersions(options)).isFalse();
        }
    }

    @Nested
    @DisplayName("the versions of keys")
    class TheVersionsOfKeys {

        @Test
        void are_the_epoch_the_version_and_the_unstamped_count_observed_for_every_key() {
            Map<String, ObservedMarker> observed = Map.of(
                    KEY_A, new ObservedMarker(EPOCH, 4, 1),
                    KEY_B, new ObservedMarker("epoch-2", 9, 9));

            Optional<DcbMarkerVersions> versions = DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A, KEY_B), observed);

            assertThat(versions).isPresent();
            assertThat(versions.get().markers()).containsOnly(
                    Map.entry(KEY_A, new MarkerVersion(EPOCH, 4, 1)),
                    Map.entry(KEY_B, new MarkerVersion("epoch-2", 9, 9)));
        }

        @Test
        void do_not_include_a_marker_observed_for_a_key_that_was_not_asked_for() {
            Map<String, ObservedMarker> observed = Map.of(
                    KEY_A, new ObservedMarker(EPOCH, 4, 0),
                    KEY_B, new ObservedMarker(EPOCH, 2, 0));

            Optional<DcbMarkerVersions> versions = DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A), observed);

            assertThat(versions.orElseThrow().keys()).containsExactly(KEY_A);
        }

        @Test
        void are_absent_when_no_key_is_asked_for() {
            assertThat(DcbMarkerVersionsSupport.versionsOf(Set.of(), Map.of(KEY_A, new ObservedMarker(EPOCH, 4, 0)))).isEmpty();
        }

        @Test
        void are_absent_when_a_key_has_no_observed_marker() {
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(EPOCH, 4, 0));

            assertThat(DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A, KEY_B), observed)).isEmpty();
        }

        @Test
        void are_absent_when_a_marker_has_no_epoch() {
            Map<String, ObservedMarker> observed = Map.of(
                    KEY_A, new ObservedMarker(EPOCH, 4, 0),
                    KEY_B, new ObservedMarker(null, 4, 4));

            assertThat(DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A, KEY_B), observed)).isEmpty();
        }

        @Test
        void are_absent_when_a_marker_has_a_blank_epoch() {
            assertThat(DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A), Map.of(KEY_A, new ObservedMarker(" ", 4, 0)))).isEmpty();
        }

        @Test
        void are_absent_when_a_marker_has_a_version_below_one() {
            assertThat(DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A), Map.of(KEY_A, new ObservedMarker(EPOCH, 0, 0)))).isEmpty();
        }

        @Test
        void are_absent_when_a_marker_has_a_negative_unstamped_count() {
            assertThat(DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A), Map.of(KEY_A, new ObservedMarker(EPOCH, 4, -1)))).isEmpty();
        }

        @Test
        void are_absent_when_a_marker_has_an_unstamped_count_above_its_version() {
            assertThat(DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A), Map.of(KEY_A, new ObservedMarker(EPOCH, 4, 5)))).isEmpty();
        }

        @Test
        void are_present_for_a_marker_whose_unstamped_count_equals_its_version() {
            assertThat(DcbMarkerVersionsSupport.versionsOf(Set.of(KEY_A), Map.of(KEY_A, new ObservedMarker(EPOCH, 4, 4)))).isPresent();
        }
    }

    @Nested
    @DisplayName("the check that a read can resume after saved versions")
    class TheCheckThatAReadCanResumeAfterSavedVersions {

        private final DcbCriteria criteria = DcbCriteria.tags(A, B);

        @Test
        void accepts_saved_versions_that_cover_the_keys_of_the_criteria_and_match_what_was_observed() {
            DcbMarkerVersions saved = versions(Map.of(KEY_A, new MarkerVersion(EPOCH, 3, 1), KEY_B, new MarkerVersion(EPOCH, 5, 0)));
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(EPOCH, 3, 1), KEY_B, new ObservedMarker(EPOCH, 5, 0));

            assertThatCode(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed)).doesNotThrowAnyException();
        }

        @Test
        void accepts_a_marker_that_has_moved_past_the_saved_version() {
            DcbMarkerVersions saved = versions(Map.of(KEY_A, new MarkerVersion(EPOCH, 3, 1), KEY_B, new MarkerVersion(EPOCH, 5, 0)));
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(EPOCH, 40, 1), KEY_B, new ObservedMarker(EPOCH, 50, 0));

            assertThatCode(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed)).doesNotThrowAnyException();
        }

        @Test
        void names_the_criteria_and_the_saved_versions_in_the_exception_it_throws() {
            DcbMarkerVersions saved = versions(KEY_A, 3, 0);

            assertThatExceptionOfType(DcbSnapshotInvalidException.class)
                    .isThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, Map.of()))
                    .satisfies(e -> {
                        assertThat(e.criteria()).isEqualTo(criteria);
                        assertThat(e.afterVersions()).isEqualTo(saved);
                    });
        }

        @Test
        void refuses_criteria_that_match_every_event_whatever_was_saved_and_observed() {
            DcbMarkerVersions saved = versions(KEY_A, 3, 0);

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(DcbCriteria.all(), saved, Map.of(KEY_A, new ObservedMarker(EPOCH, 3, 0))))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("match every event");
        }

        @Test
        void refuses_a_missing_saved_key_before_an_extra_one() {
            DcbMarkerVersions saved = versions(KEY_B, 3, 0, "tag:account:extra", 3, 0);

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, Map.of()))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("no entry for marker key " + KEY_A);
        }

        @Test
        void refuses_the_first_missing_saved_key_in_ascending_order() {
            DcbMarkerVersions saved = versions("tag:account:extra", 3, 0);

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, Map.of()))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("no entry for marker key " + KEY_A);
        }

        @Test
        void refuses_an_extra_saved_key() {
            DcbMarkerVersions saved = versions(KEY_A, 3, 0, KEY_B, 3, 0, "tag:account:extra", 3, 0);
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(EPOCH, 3, 0), KEY_B, new ObservedMarker(EPOCH, 3, 0));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("an entry for marker key tag:account:extra");
        }

        @Test
        void refuses_the_saved_keys_before_it_looks_at_any_observed_marker() {
            DcbMarkerVersions saved = versions(KEY_A, 3, 0, "tag:account:extra", 3, 0);
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker("another-epoch", 1, 1));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("no entry for marker key " + KEY_B);
        }

        @Test
        void refuses_a_key_that_has_no_observed_marker() {
            DcbMarkerVersions saved = versions(KEY_A, 3, 0, KEY_B, 3, 0);
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(EPOCH, 3, 0));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("There is no marker for marker key " + KEY_B);
        }

        @Test
        void refuses_a_marker_with_no_epoch_before_it_compares_anything_else_about_it() {
            DcbMarkerVersions saved = versions(KEY_A, 9, 0, KEY_B, 3, 0);
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(null, 1, 1), KEY_B, new ObservedMarker(EPOCH, 3, 0));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("The marker for marker key " + KEY_A + " has no epoch");
        }

        @Test
        void refuses_a_different_epoch_before_it_compares_versions() {
            DcbMarkerVersions saved = versions(KEY_A, 9, 0, KEY_B, 3, 0);
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker("another-epoch", 1, 1), KEY_B, new ObservedMarker(EPOCH, 3, 0));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("epoch of the marker for marker key " + KEY_A + " differs");
        }

        @Test
        void refuses_a_saved_version_above_the_observed_one_before_it_compares_unstamped_counts() {
            DcbMarkerVersions saved = versions(Map.of(KEY_A, new MarkerVersion(EPOCH, 9, 3), KEY_B, new MarkerVersion(EPOCH, 3, 0)));
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(EPOCH, 8, 0), KEY_B, new ObservedMarker(EPOCH, 3, 0));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("saved version of marker key " + KEY_A + " is above");
        }

        @Test
        void refuses_a_changed_unstamped_count() {
            DcbMarkerVersions saved = versions(Map.of(KEY_A, new MarkerVersion(EPOCH, 3, 0), KEY_B, new MarkerVersion(EPOCH, 3, 0)));
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(EPOCH, 4, 1), KEY_B, new ObservedMarker(EPOCH, 3, 0));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("marker key " + KEY_A + " was incremented by a writer that wrote no marker versions");
        }

        @Test
        void refuses_a_lower_unstamped_count_too() {
            DcbMarkerVersions saved = versions(Map.of(KEY_A, new MarkerVersion(EPOCH, 3, 2), KEY_B, new MarkerVersion(EPOCH, 3, 0)));
            Map<String, ObservedMarker> observed = Map.of(KEY_A, new ObservedMarker(EPOCH, 3, 0), KEY_B, new ObservedMarker(EPOCH, 3, 0));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("marker key " + KEY_A + " was incremented by a writer that wrote no marker versions");
        }

        @Test
        void reports_the_first_key_in_ascending_order_that_fails_whichever_check_it_fails() {
            DcbMarkerVersions saved = versions(Map.of(KEY_A, new MarkerVersion(EPOCH, 3, 0), KEY_B, new MarkerVersion(EPOCH, 3, 0)));
            Map<String, ObservedMarker> observed = new HashMap<>();
            observed.put(KEY_B, new ObservedMarker("another-epoch", 3, 0));
            observed.put(KEY_A, new ObservedMarker(EPOCH, 4, 1));

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("marker key " + KEY_A + " was incremented by a writer that wrote no marker versions");
        }

        @Test
        void checks_the_keys_of_every_item_of_criteria_that_has_several() {
            DcbCriteria several = DcbCriteria.anyOf(DcbCriteria.tags(A), DcbCriteria.type("Deposited"));
            DcbMarkerVersions saved = versions(KEY_A, 3, 0);

            assertThatThrownBy(() -> DcbMarkerVersionsSupport.requireResumable(several, saved, Map.of(KEY_A, new ObservedMarker(EPOCH, 3, 0))))
                    .isExactlyInstanceOf(DcbSnapshotInvalidException.class)
                    .hasMessageContaining("no entry for marker key type:Deposited");
        }
    }

    private static DcbMarkerVersions versions(String key, long version, long unstamped) {
        return versions(Map.of(key, new MarkerVersion(EPOCH, version, unstamped)));
    }

    private static DcbMarkerVersions versions(String firstKey, long firstVersion, long firstUnstamped, String secondKey, long secondVersion, long secondUnstamped) {
        return versions(Map.of(firstKey, new MarkerVersion(EPOCH, firstVersion, firstUnstamped), secondKey, new MarkerVersion(EPOCH, secondVersion, secondUnstamped)));
    }

    private static DcbMarkerVersions versions(String firstKey, long firstVersion, long firstUnstamped, String secondKey, long secondVersion, long secondUnstamped,
                                              String thirdKey, long thirdVersion, long thirdUnstamped) {
        return versions(Map.of(
                firstKey, new MarkerVersion(EPOCH, firstVersion, firstUnstamped),
                secondKey, new MarkerVersion(EPOCH, secondVersion, secondUnstamped),
                thirdKey, new MarkerVersion(EPOCH, thirdVersion, thirdUnstamped)));
    }

    private static DcbMarkerVersions versions(Map<String, MarkerVersion> markers) {
        return DcbMarkerVersions.of(markers);
    }
}
