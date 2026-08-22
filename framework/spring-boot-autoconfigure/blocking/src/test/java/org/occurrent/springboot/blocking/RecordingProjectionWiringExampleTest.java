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
import org.occurrent.dsl.projection.blocking.Projections;
import org.occurrent.dsl.projection.blocking.RecordingMaterializedView;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wiring the {@code recordingAppliedAppends} javadoc tells a caller composing a projection outside Spring to
 * write, compiled and run here so the instruction cannot drift away from what the types allow. A Copilot review of
 * this branch caught exactly that, an instruction that read fine and did not compile.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RecordingProjectionWiringExampleTest {

    record MyEvent(String id) {
    }

    // A model that has catch-ups and records what registered for them, which is all findIn and listenForCatchup
    // need from it here.
    static final class RegisteringModel implements ReplayAwareSubscriptions {
        final List<CatchupListener> registered = new ArrayList<>();

        @Override
        public boolean isCatchingUp(String subscriptionId) {
            return false;
        }

        @Override
        public boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
            registered.add(listener);
            return true;
        }
    }

    @Test
    void the_documented_wiring_compiles_and_registers_the_recording_view_on_the_model() {
        String projectionId = "orders";
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        MaterializedView<MyEvent> view = event -> {
        };
        RegisteringModel subscriptionModel = new RegisteringModel();

        RecordingMaterializedView<MyEvent> recording = Projections.recordingAppliedAppends(view, projectionId, store);
        ReplayAwareSubscriptions.findIn(subscriptionModel)
                .ifPresent(model -> model.listenForCatchup(projectionId, recording));

        assertThat(subscriptionModel.registered).containsExactly(recording);
    }
}
