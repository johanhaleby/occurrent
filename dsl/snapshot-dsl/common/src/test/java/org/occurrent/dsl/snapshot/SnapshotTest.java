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
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotTest {

    private static final DcbMarkerVersions VERSIONS = DcbMarkerVersions.of(Map.of("tag:a", new MarkerVersion("epoch-1", 3, 0)));

    @Test
    void a_snapshot_without_marker_versions_keeps_its_version() {
        Snapshot<String> snapshot = new Snapshot<>("state", 7, 2);

        assertThat(snapshot.version()).isEqualTo(7);
        assertThat(snapshot.markerVersions()).isEmpty();
    }

    @Test
    void a_snapshot_at_marker_versions_has_version_zero() {
        Snapshot<String> snapshot = Snapshot.atMarkerVersions("state", VERSIONS, 2);

        assertThat(snapshot.version()).isZero();
        assertThat(snapshot.markerVersions()).contains(VERSIONS);
        assertThat(snapshot.schemaVersion()).isEqualTo(2);
    }

    @Test
    void marker_versions_and_a_version_cannot_be_combined() {
        assertThatThrownBy(() -> new Snapshot<>("state", 7, 2, Optional.of(VERSIONS)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("version must be 0 for a snapshot with marker versions");
    }

    @Test
    void a_negative_version_is_refused() {
        assertThatThrownBy(() -> new Snapshot<>("state", -1, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("version cannot be negative");
    }

    @Test
    void a_negative_schema_version_is_refused() {
        assertThatThrownBy(() -> Snapshot.atMarkerVersions("state", VERSIONS, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaVersion cannot be negative");
    }

    @Test
    void null_marker_versions_are_refused() {
        assertThatThrownBy(() -> Snapshot.atMarkerVersions("state", null, 2))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("markerVersions cannot be null");
        assertThatThrownBy(() -> new Snapshot<>("state", 0, 2, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("markerVersions cannot be null");
    }
}
