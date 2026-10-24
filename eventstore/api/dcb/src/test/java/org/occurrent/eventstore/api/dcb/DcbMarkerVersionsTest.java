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
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("DcbMarkerVersions")
class DcbMarkerVersionsTest {

    private static final MarkerVersion FIRST = new MarkerVersion("epoch-1", 3, 0);
    private static final MarkerVersion SECOND = new MarkerVersion("epoch-2", 7, 2);

    @Nested
    @DisplayName("a marker version")
    class AMarkerVersion {

        @Test
        void keeps_the_epoch_the_version_and_the_unstamped_count_it_was_given() {
            MarkerVersion markerVersion = new MarkerVersion("epoch-1", 5, 2);

            assertThat(markerVersion.epoch()).isEqualTo("epoch-1");
            assertThat(markerVersion.version()).isEqualTo(5);
            assertThat(markerVersion.unstamped()).isEqualTo(2);
        }

        @Test
        void accepts_an_unstamped_count_of_zero_and_one_equal_to_the_version() {
            assertThat(new MarkerVersion("epoch-1", 4, 0).unstamped()).isZero();
            assertThat(new MarkerVersion("epoch-1", 4, 4).unstamped()).isEqualTo(4);
        }

        @Test
        void accepts_the_lowest_version_which_is_one() {
            assertThat(new MarkerVersion("epoch-1", 1, 0).version()).isEqualTo(1);
        }

        @Test
        void rejects_a_null_epoch() {
            assertThatThrownBy(() -> new MarkerVersion(null, 1, 0))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("Epoch cannot be null");
        }

        @Test
        void rejects_an_empty_epoch() {
            assertThatThrownBy(() -> new MarkerVersion("", 1, 0))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Epoch cannot be blank");
        }

        @Test
        void rejects_a_blank_epoch() {
            assertThatThrownBy(() -> new MarkerVersion("  ", 1, 0))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Epoch cannot be blank");
        }

        @Test
        void rejects_a_version_of_zero() {
            assertThatThrownBy(() -> new MarkerVersion("epoch-1", 0, 0))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Version must be greater than zero");
        }

        @Test
        void rejects_a_negative_version() {
            assertThatThrownBy(() -> new MarkerVersion("epoch-1", -1, 0))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Version must be greater than zero");
        }

        @Test
        void rejects_a_negative_unstamped_count() {
            assertThatThrownBy(() -> new MarkerVersion("epoch-1", 3, -1))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Unstamped count cannot be negative");
        }

        @Test
        void rejects_an_unstamped_count_above_the_version() {
            assertThatThrownBy(() -> new MarkerVersion("epoch-1", 3, 4))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Unstamped count cannot be greater than the version");
        }
    }

    @Nested
    @DisplayName("the versions of a boundary")
    class TheVersionsOfABoundary {

        @Test
        void holds_the_marker_version_of_every_key_it_was_given() {
            DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:a", FIRST, "type:T", SECOND));

            assertThat(versions.markers()).containsOnly(Map.entry("tag:a", FIRST), Map.entry("type:T", SECOND));
        }

        @Test
        void lists_its_keys_in_ascending_order_whatever_order_they_were_given_in() {
            Map<String, MarkerVersion> given = new LinkedHashMap<>();
            given.put("type:T", SECOND);
            given.put("tag:b", FIRST);
            given.put("tag:a", FIRST);

            DcbMarkerVersions versions = new DcbMarkerVersions(given);

            assertThat(versions.keys()).containsExactly("tag:a", "tag:b", "type:T");
            assertThat(versions.markers().keySet()).containsExactly("tag:a", "tag:b", "type:T");
        }

        @Test
        void is_equal_to_versions_with_the_same_entries_given_in_another_order() {
            Map<String, MarkerVersion> forwards = new LinkedHashMap<>();
            forwards.put("tag:a", FIRST);
            forwards.put("tag:b", SECOND);
            Map<String, MarkerVersion> backwards = new LinkedHashMap<>();
            backwards.put("tag:b", SECOND);
            backwards.put("tag:a", FIRST);

            assertThat(new DcbMarkerVersions(forwards)).isEqualTo(new DcbMarkerVersions(backwards));
            assertThat(new DcbMarkerVersions(forwards)).hasSameHashCodeAs(new DcbMarkerVersions(backwards));
        }

        @Test
        void is_different_from_versions_that_differ_in_one_entry() {
            DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:a", FIRST));

            assertThat(versions).isNotEqualTo(DcbMarkerVersions.of(Map.of("tag:a", SECOND)));
            assertThat(versions).isNotEqualTo(DcbMarkerVersions.of(Map.of("tag:b", FIRST)));
        }

        @Test
        void cannot_be_changed_through_the_map_it_exposes() {
            DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:a", FIRST));

            assertThatThrownBy(() -> versions.markers().put("tag:b", SECOND)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> versions.markers().remove("tag:a")).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> versions.markers().clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> versions.keys().remove("tag:a")).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void is_not_changed_by_a_later_change_to_the_map_it_was_created_from() {
            Map<String, MarkerVersion> given = new HashMap<>();
            given.put("tag:a", FIRST);
            DcbMarkerVersions versions = new DcbMarkerVersions(given);

            given.put("tag:b", SECOND);
            given.remove("tag:a");

            assertThat(versions.keys()).containsExactly("tag:a");
            assertThat(versions.get("tag:a")).contains(FIRST);
        }

        @Test
        void returns_the_version_of_a_key_it_covers() {
            DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:a", FIRST, "tag:b", SECOND));

            assertThat(versions.get("tag:b")).contains(SECOND);
        }

        @Test
        void returns_nothing_for_a_key_it_does_not_cover() {
            DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:a", FIRST));

            assertThat(versions.get("tag:z")).isEmpty();
        }

        @Test
        void rejects_looking_up_a_null_key() {
            DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:a", FIRST));

            assertThatThrownBy(() -> versions.get(null))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("Marker key cannot be null");
        }

        @Test
        void rejects_a_null_map() {
            assertThatThrownBy(() -> new DcbMarkerVersions(null))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("Markers cannot be null");
        }

        @Test
        void rejects_an_empty_map() {
            assertThatThrownBy(() -> new DcbMarkerVersions(Map.of()))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Markers cannot be empty");
        }

        @Test
        void rejects_an_empty_key() {
            assertThatThrownBy(() -> DcbMarkerVersions.of(Map.of("", FIRST)))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Marker key cannot be blank");
        }

        @Test
        void rejects_a_blank_key() {
            assertThatThrownBy(() -> DcbMarkerVersions.of(Map.of("   ", FIRST)))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Marker key cannot be blank");
        }

        @Test
        void rejects_a_null_key() {
            Map<String, MarkerVersion> given = new HashMap<>();
            given.put(null, FIRST);

            assertThatThrownBy(() -> new DcbMarkerVersions(given))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("Marker key cannot be null");
        }

        @Test
        void rejects_a_null_marker_version() {
            Map<String, MarkerVersion> given = new HashMap<>();
            given.put("tag:a", null);

            assertThatThrownBy(() -> new DcbMarkerVersions(given))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("Marker version cannot be null");
        }
    }
}
