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

package org.occurrent.dsl.snapshot;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.snapshot.internal.SnapshotMarkerVersionsFields;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotMarkerVersionsFieldsTest {

    private static final DcbCriteria CRITERIA = DcbCriteria.tags(Tag.parse("account:a"));
    private static final String CRITERIA_STRING = DcbMarkerVersions.of(CRITERIA, Map.of("tag:account:a", new MarkerVersion("epoch-1", 1, 0))).criteria();

    @Test
    void round_trips_marker_versions_including_a_key_with_a_dot_and_an_unstamped_count() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(CRITERIA, Map.of(
                "tag:account:a.b", new MarkerVersion("epoch-1", 3, 1),
                "type:Deposited", new MarkerVersion("epoch-2", 8, 0)));

        DcbMarkerVersions read = SnapshotMarkerVersionsFields.fromFields(SnapshotMarkerVersionsFields.toFields(versions));

        assertThat(read).isEqualTo(versions);
    }

    @Test
    void stores_each_marker_as_a_field_with_the_key_as_a_value_not_as_a_name() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(CRITERIA, Map.of("tag:account:a.b", new MarkerVersion("epoch-1", 3, 1)));

        Map<String, Object> fields = SnapshotMarkerVersionsFields.toFields(versions);

        assertThat(fields).containsOnlyKeys("c", "m");
        assertThat(fields.get("m")).isEqualTo(List.of(Map.of("k", "tag:account:a.b", "e", "epoch-1", "v", 3L, "u", 1L)));
    }

    @Test
    void stores_the_criteria_the_versions_were_read_with() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(CRITERIA, Map.of("tag:account:a", new MarkerVersion("epoch-1", 3, 0)));

        Map<String, Object> fields = SnapshotMarkerVersionsFields.toFields(versions);

        assertThat(fields.get("c")).isEqualTo(versions.criteria());
    }

    @Test
    void round_trips_the_criteria_so_that_the_versions_are_still_read_with_them() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(CRITERIA, Map.of("tag:account:a", new MarkerVersion("epoch-1", 3, 0)));

        DcbMarkerVersions read = SnapshotMarkerVersionsFields.fromFields(SnapshotMarkerVersionsFields.toFields(versions));

        assertThat(read.criteria()).isEqualTo(versions.criteria());
        assertThat(read.readWith(CRITERIA)).isTrue();
        assertThat(read.readWith(DcbCriteria.tags(Tag.parse("account:b")))).isFalse();
    }

    @Test
    void the_list_of_markers_that_earlier_0_35_snapshots_stored_without_criteria_is_refused() {
        List<Map<String, Object>> bareList = List.of(field("tag:account:a", "epoch-1", 3L, 0L));

        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(bareList))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a map with the criteria");
    }

    @Test
    void a_map_without_criteria_is_refused() {
        Map<String, Object> noCriteria = new LinkedHashMap<>();
        noCriteria.put("m", List.of(field("tag:account:a", "epoch-1", 3L, 0L)));

        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(noCriteria))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Criteria are missing");
    }

    @Test
    void a_map_without_markers_is_refused() {
        Map<String, Object> noMarkers = new LinkedHashMap<>();
        noMarkers.put("c", "all");

        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(noMarkers))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Marker versions are missing");
    }

    @Test
    void reads_a_number_of_any_width_as_a_version() {
        DcbMarkerVersions read = SnapshotMarkerVersionsFields.fromFields(stored(field("tag:a", "epoch-1", 3, 1)));

        assertThat(read.get("tag:a")).contains(new MarkerVersion("epoch-1", 3, 1));
    }

    @Test
    void a_missing_field_is_refused() {
        Map<String, Object> noEpoch = field("tag:a", "epoch-1", 3L, 0L);
        noEpoch.remove("e");

        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(stored(noEpoch)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Epoch is missing for marker key tag:a");
    }

    @Test
    void a_field_of_the_wrong_type_is_refused() {
        Map<String, Object> wrongType = field("tag:a", "epoch-1", 3L, 0L);
        wrongType.put("v", "three");

        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(stored(wrongType))).isInstanceOf(ClassCastException.class);
    }

    @Test
    void a_marker_that_is_not_a_map_is_refused() {
        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(stored("tag:a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A marker version must be a map, was tag:a");
    }

    @Test
    void a_key_stored_twice_is_refused() {
        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(stored(field("tag:a", "epoch-1", 3L, 0L), field("tag:a", "epoch-1", 4L, 0L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Marker key tag:a is stored more than once");
    }

    @Test
    void values_a_marker_version_does_not_accept_are_refused() {
        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(stored(field("tag:a", "epoch-1", 0L, 0L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A marker without a version cannot have an epoch");
    }

    @Test
    void round_trips_a_type_marker_with_absent_stripes_and_stores_no_epoch_for_the_absent_ones() {
        Map<String, MarkerVersion> markers = new java.util.TreeMap<>();
        markers.put(DcbMarkerVersionsSupport.stripeKey("D", 0), new MarkerVersion("epoch-1", 1, 0));
        for (int stripe = 1; stripe < DcbMarkerVersionsSupport.STRIPES; stripe++) {
            markers.put(DcbMarkerVersionsSupport.stripeKey("D", stripe), MarkerVersion.absent());
        }
        DcbCriteria criteria = DcbCriteria.types("D");
        DcbMarkerVersions versions = DcbMarkerVersions.of(criteria, markers);

        Map<String, Object> fields = SnapshotMarkerVersionsFields.toFields(versions);
        assertThatCode(() -> SnapshotMarkerVersionsFields.fromFields(fields)).doesNotThrowAnyException();
        DcbMarkerVersions read = SnapshotMarkerVersionsFields.fromFields(fields);

        assertThat(read).isEqualTo(versions);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> stored = (List<Map<String, Object>>) fields.get("m");
        assertThat(stored).filteredOn(m -> !"epoch-1".equals(m.get("e"))).hasSize(DcbMarkerVersionsSupport.STRIPES - 1).allSatisfy(m -> assertThat(m).doesNotContainKey("e"));
    }

    @Test
    void no_markers_are_refused() {
        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(stored()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Markers cannot be empty");
    }

    private static Map<String, Object> stored(Object... markers) {
        Map<String, Object> stored = new LinkedHashMap<>();
        stored.put("c", CRITERIA_STRING);
        stored.put("m", List.of(markers));
        return stored;
    }

    private static Map<String, Object> field(String key, String epoch, Number version, Number unstamped) {
        Map<String, Object> field = new LinkedHashMap<>();
        field.put("k", key);
        field.put("e", epoch);
        field.put("v", version);
        field.put("u", unstamped);
        return field;
    }
}
