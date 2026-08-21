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

package org.occurrent.dsl.projection.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.api.AppendId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins multi-event partial visibility as intended semantics, not an accident
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 10): {@link RecordingMaterializedView} records the append id after <em>each</em> handled event returns,
 * not after the whole append has been handled. So a waiter can observe {@code true} once the projection has applied
 * the first of a multi-event append's events, before the rest have been applied. This test asserts exactly that
 * window exists, so a later change that accidentally moves recording to "after the last event of the append" (which
 * decision 10 explicitly rejects, since a filtered subscriber may never see an append's last event at all) fails
 * loudly here instead of silently changing the guarantee callers observe.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RecordingMaterializedViewPartialVisibilityTest {

    private static final String PROJECTION_ID = "orderStatus";

    @Test
    void a_waiter_can_observe_true_after_the_first_of_a_multi_event_appends_events_is_applied_before_the_rest_are() {
        List<String> appliedInOrder = new ArrayList<>();
        MaterializedView<String> delegate = new MaterializedView<>() {
            @Override
            public void update(String event) {
                appliedInOrder.add(event);
            }
        };
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();

        RecordingMaterializedView<String> recording = new RecordingMaterializedView<>(delegate, PROJECTION_ID, store);

        // First event of a two-event append.
        recording.update(metadataWithAppendId(appendId), "event-1");

        // Pinned: the append is already visible to a membership waiter, even though its second event has not been
        // applied yet.
        assertThat(store.hasApplied(PROJECTION_ID, appendId)).as("membership is visible after the first handled event, not the last").isTrue();
        assertThat(appliedInOrder).as("but the read model has only applied the first event so far").containsExactly("event-1");

        // Second (and last) event of the same append.
        recording.update(metadataWithAppendId(appendId), "event-2");

        assertThat(appliedInOrder).containsExactly("event-1", "event-2");
        assertThat(store.hasApplied(PROJECTION_ID, appendId)).isTrue();
    }

    private static EventMetadata metadataWithAppendId(AppendId appendId) {
        return new EventMetadata(Map.of(OccurrentCloudEventExtension.APPEND_ID, appendId.toString()));
    }
}
