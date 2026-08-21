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

package org.occurrent.springboot.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.springboot.common.AppliedAppendRecordingRegistry;
import org.occurrent.dsl.projection.blocking.Projections;
import org.occurrent.dsl.projection.blocking.RecordingMaterializedView;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.api.AppendId;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pull feed drives its own replay, so nothing watches it for catch-up boundaries. The clear that replay owes can
 * still fail, and the feed can then go quiet with no delivery left to retry it, which is what the scheduled poll is
 * for. Without it a membership the rebuild discarded survives and a wait for it answers true about a read model that
 * no longer holds it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PullFeedClearRetryTest {

    private static final String PROJECTION_ID = "orders";

    @Test
    void a_clear_a_feed_replay_left_failing_is_retried_by_the_poll_with_no_further_deliveries() {
        FailingOnceClearStore store = new FailingOnceClearStore();
        AppendId beforeTheRebuild = AppendId.mint();
        store.delegate.recordApplied(PROJECTION_ID, beforeTheRebuild);

        MaterializedView<String> view = event -> {
        };
        RecordingMaterializedView<String> recording = Projections.recordingAppliedAppends(view, PROJECTION_ID, store);
        AppliedAppendRecordingRegistry registry =
                new AppliedAppendRecordingRegistry(Duration.ofMillis(200), Duration.ofSeconds(5), 2.0);
        registry.register(PROJECTION_ID, (BooleanSupplier) recording::pollForClear);

        // The feed replays and completes it. Its own clear attempt fails, and nothing is delivered afterwards.
        recording.replayStarted();
        recording.replayCompleted();
        assertThat(store.clearAttempts).isEqualTo(1);
        assertThat(store.hasApplied(PROJECTION_ID, beforeTheRebuild))
                .as("the clear failed, so what the rebuild discards is still recorded")
                .isTrue();

        registry.tick(PROJECTION_ID);

        assertThat(store.clearAttempts).isEqualTo(2);
        assertThat(store.hasApplied(PROJECTION_ID, beforeTheRebuild)).isFalse();
    }

    private static final class FailingOnceClearStore implements AppliedAppendStore {
        private final AppliedAppendStore delegate = AppliedAppendStore.inMemory();
        private int clearAttempts = 0;

        @Override
        public void recordApplied(String projectionId, AppendId appendId) {
            delegate.recordApplied(projectionId, appendId);
        }

        @Override
        public boolean hasApplied(String projectionId, AppendId appendId) {
            return delegate.hasApplied(projectionId, appendId);
        }

        @Override
        public void clear(String projectionId) {
            clearAttempts++;
            if (clearAttempts == 1) {
                throw new RuntimeException("the store was briefly unavailable");
            }
            delegate.clear(projectionId);
        }
    }
}
