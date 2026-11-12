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
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotMarkerVersionsFieldsTest {

    @Test
    void round_trips_marker_versions_including_a_key_with_a_dot_and_an_unstamped_count() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of(
                "tag:account:a.b", new MarkerVersion("epoch-1", 3, 1),
                "type:Deposited", new MarkerVersion("epoch-2", 8, 0)));

        DcbMarkerVersions read = SnapshotMarkerVersionsFields.fromFields(SnapshotMarkerVersionsFields.toFields(versions));

        assertThat(read).isEqualTo(versions);
    }

    @Test
    void stores_each_marker_as_a_field_with_the_key_as_a_value_not_as_a_name() {
        DcbMarkerVersions versions = DcbMarkerVersions.of(Map.of("tag:account:a.b", new MarkerVersion("epoch-1", 3, 1)));

        List<Map<String, Object>> fields = SnapshotMarkerVersionsFields.toFields(versions);

        assertThat(fields).containsExactly(Map.of("k", "tag:account:a.b", "e", "epoch-1", "v", 3L, "u", 1L));
    }

    @Test
    void reads_a_number_of_any_width_as_a_version() {
        DcbMarkerVersions read = SnapshotMarkerVersionsFields.fromFields(List.of(field("tag:a", "epoch-1", 3, 1)));

        assertThat(read.get("tag:a")).contains(new MarkerVersion("epoch-1", 3, 1));
    }

    @Test
    void a_missing_field_is_refused() {
        Map<String, Object> noEpoch = field("tag:a", "epoch-1", 3L, 0L);
        noEpoch.remove("e");

        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(List.of(noEpoch)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Epoch is missing for marker key tag:a");
    }

    @Test
    void a_field_of_the_wrong_type_is_refused() {
        Map<String, Object> wrongType = field("tag:a", "epoch-1", 3L, 0L);
        wrongType.put("v", "three");

        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(List.of(wrongType))).isInstanceOf(ClassCastException.class);
    }

    @Test
    void a_marker_that_is_not_a_map_is_refused() {
        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(List.of("tag:a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A marker version must be a map, was tag:a");
    }

    @Test
    void a_key_stored_twice_is_refused() {
        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(List.of(field("tag:a", "epoch-1", 3L, 0L), field("tag:a", "epoch-1", 4L, 0L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Marker key tag:a is stored more than once");
    }

    @Test
    void values_a_marker_version_does_not_accept_are_refused() {
        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(List.of(field("tag:a", "epoch-1", 0L, 0L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Version must be greater than zero");
    }

    @Test
    void no_markers_are_refused() {
        assertThatThrownBy(() -> SnapshotMarkerVersionsFields.fromFields(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Markers cannot be empty");
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
