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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.occurrent.eventstore.api.dcb.Tag;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SnapshotSupportTest {

    private static final DcbCriteria CRITERIA = DcbCriteria.tags(Tag.parse("name:1"));

    @Test
    void resolve_base_uses_the_snapshot_when_the_schema_matches() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(loaded, 1, () -> 0);

        assertThat(base.state()).isEqualTo(42);
        assertThat(base.version()).isEqualTo(7);
    }

    @Test
    void resolve_base_falls_back_to_initial_state_on_a_schema_mismatch() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(loaded, 2, () -> 0);

        assertThat(base.state()).isEqualTo(0);
        assertThat(base.version()).isEqualTo(0);
    }

    @Test
    void resolve_base_falls_back_to_initial_state_when_no_snapshot_exists() {
        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(Optional.empty(), 1, () -> 5);

        assertThat(base.state()).isEqualTo(5);
        assertThat(base.version()).isEqualTo(0);
    }

    @Test
    void is_redelivery_is_true_only_when_the_snapshot_already_covers_the_delivered_version_at_a_matching_schema() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        assertThat(SnapshotSupport.isRedelivery(loaded, 1, 7)).isTrue();  // already folded up to 7
        assertThat(SnapshotSupport.isRedelivery(loaded, 1, 6)).isTrue();  // an older redelivery
        assertThat(SnapshotSupport.isRedelivery(loaded, 1, 8)).isFalse(); // a new event to fold
        assertThat(SnapshotSupport.isRedelivery(loaded, 2, 7)).isFalse(); // schema mismatch, rebuild instead of skip
        assertThat(SnapshotSupport.isRedelivery(Optional.empty(), 1, 7)).isFalse();
    }

    @Test
    void resolve_base_demotes_to_initial_state_when_the_snapshot_version_is_beyond_the_observed_head() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        // The stream was reset to head 3, but the snapshot still claims version 7, so it is stale and must be discarded.
        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(loaded, 1, 3, () -> 0);

        assertThat(base.state()).isEqualTo(0);
        assertThat(base.version()).isEqualTo(0);
    }

    @Test
    void resolve_base_keeps_the_snapshot_when_its_version_equals_the_observed_head() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(loaded, 1, 7, () -> 0);

        assertThat(base.state()).isEqualTo(42);
        assertThat(base.version()).isEqualTo(7);
    }

    @Test
    void resolve_base_with_an_unbounded_head_keeps_a_high_version_snapshot() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, Long.MAX_VALUE, 1));

        // The 3-arg overload trusts the snapshot unconditionally, so even the maximum version is kept.
        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(loaded, 1, () -> 0);

        assertThat(base.state()).isEqualTo(42);
        assertThat(base.version()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void is_redelivery_is_false_when_the_head_is_below_the_snapshot_version_because_that_is_a_reset() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        // Delivered version 5 sits below the snapshot version 7 (looks like a redelivery), but the head is only 3, so the
        // stream was reset and the snapshot is stale: not a redelivery, the caller must rebuild.
        assertThat(SnapshotSupport.isRedelivery(loaded, 1, 5, 3)).isFalse();
    }

    @Test
    void is_redelivery_is_true_when_the_snapshot_still_covers_the_delivered_version_within_the_head() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        // The head is at 7, the snapshot is at 7, and version 5 was already folded: a genuine redelivery to skip.
        assertThat(SnapshotSupport.isRedelivery(loaded, 1, 5, 7)).isTrue();
    }

    @Test
    void resolve_dcb_base_uses_the_state_and_marker_versions_of_the_snapshot_when_the_schema_matches() {
        DcbMarkerVersions versions = versions("tag:a", 4);
        Optional<Snapshot<Integer>> loaded = Optional.of(Snapshot.atMarkerVersions(42, versions, 17, 1));

        SnapshotSupport.DcbBase<Integer> base = SnapshotSupport.resolveDcbBase(loaded, 1, () -> 0);

        assertThat(base.state()).isEqualTo(42);
        assertThat(base.markerVersions()).contains(versions);
    }

    @Test
    void resolve_dcb_base_carries_the_highest_folded_position_of_the_snapshot() {
        Optional<Snapshot<Integer>> loaded = Optional.of(Snapshot.atMarkerVersions(42, versions("tag:a", 4), 17, 1));

        SnapshotSupport.DcbBase<Integer> base = SnapshotSupport.resolveDcbBase(loaded, 1, () -> 0);

        assertThat(base.highestFoldedPosition()).isEqualTo(17);
    }

    @Test
    void resolve_dcb_base_ignores_a_snapshot_saved_at_a_position_since_it_has_no_marker_versions() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        SnapshotSupport.DcbBase<Integer> base = SnapshotSupport.resolveDcbBase(loaded, 1, () -> 0);

        assertThat(base.state()).isEqualTo(0);
        assertThat(base.markerVersions()).isEmpty();
    }

    @Test
    void resolve_dcb_base_falls_back_to_initial_state_on_a_schema_mismatch() {
        Optional<Snapshot<Integer>> loaded = Optional.of(Snapshot.atMarkerVersions(42, versions("tag:a", 4), 17, 1));

        SnapshotSupport.DcbBase<Integer> base = SnapshotSupport.resolveDcbBase(loaded, 2, () -> 0);

        assertThat(base.state()).isEqualTo(0);
        assertThat(base.markerVersions()).isEmpty();
        assertThat(base.highestFoldedPosition()).isZero();
    }

    @Test
    void resolve_dcb_base_falls_back_to_initial_state_when_no_snapshot_exists() {
        SnapshotSupport.DcbBase<Integer> base = SnapshotSupport.resolveDcbBase(Optional.empty(), 1, () -> 5);

        assertThat(base.state()).isEqualTo(5);
        assertThat(base.markerVersions()).isEmpty();
        assertThat(base.highestFoldedPosition()).isZero();
    }

    @Test
    void refuse_event_below_folded_position_refuses_a_read_that_returned_an_event_below_the_highest_folded_position() {
        SnapshotSupport.DcbBase<Integer> base = new SnapshotSupport.DcbBase<>(10, Optional.of(versions("tag:a", 4)), 42);

        assertThatThrownBy(() -> SnapshotSupport.refuseEventBelowFoldedPosition(CRITERIA, base, readOf(CRITERIA, List.of(eventAt(41)), 42)))
                .isInstanceOf(DcbSnapshotInvalidException.class)
                .hasMessageContaining("below position 42");
    }

    @Test
    void refuse_event_below_folded_position_accepts_a_read_whose_events_are_all_above_the_highest_folded_position() {
        SnapshotSupport.DcbBase<Integer> base = new SnapshotSupport.DcbBase<>(10, Optional.of(versions("tag:a", 4)), 42);

        assertThatCode(() -> SnapshotSupport.refuseEventBelowFoldedPosition(CRITERIA, base, readOf(CRITERIA, List.of(eventAt(43)), 43)))
                .doesNotThrowAnyException();
    }

    @Test
    void refuse_event_below_folded_position_accepts_a_read_without_events() {
        SnapshotSupport.DcbBase<Integer> base = new SnapshotSupport.DcbBase<>(10, Optional.of(versions("tag:a", 4)), 42);

        assertThatCode(() -> SnapshotSupport.refuseEventBelowFoldedPosition(CRITERIA, base, readOf(CRITERIA, List.of(), 42)))
                .doesNotThrowAnyException();
    }

    @Test
    void refuse_event_below_folded_position_accepts_any_event_when_the_read_is_from_the_initial_state() {
        SnapshotSupport.DcbBase<Integer> base = SnapshotSupport.DcbBase.initial(() -> 0);

        assertThatCode(() -> SnapshotSupport.refuseEventBelowFoldedPosition(CRITERIA, base, readOf(CRITERIA, List.of(eventAt(1)), 1)))
                .doesNotThrowAnyException();
    }

    private static DcbEventStream readOf(DcbCriteria criteria, List<CloudEvent> events, long lastSequencePosition) {
        return new DcbEventStream(events, lastSequencePosition, DcbConsistencyToken.of(criteria, lastSequencePosition));
    }

    private static CloudEvent eventAt(long position) {
        return OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("type").build(), position);
    }

    private static DcbMarkerVersions versions(String key, long version) {
        return DcbMarkerVersions.of(Map.of(key, new MarkerVersion("epoch-1", version, 0)));
    }
}
