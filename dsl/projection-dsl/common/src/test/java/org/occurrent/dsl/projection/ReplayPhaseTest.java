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

package org.occurrent.dsl.projection;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReplayPhaseTest {

    @Test
    void neverReplays_answers_false() {
        assertThat(ReplayPhase.neverReplays().currentPhase()).isEqualTo(CatchupPhase.LIVE);
    }

    @Test
    void neverReplays_returns_the_same_instance_every_call() {
        // A caller distinguishes the known never-replays case from an unresolved one by comparing with ==
        // (ProjectionAnnotationRegistrar.resolveEventStorePhase on the reactor stack), so a fresh lambda per call
        // would break that comparison silently.
        assertThat(ReplayPhase.neverReplays()).isSameAs(ReplayPhase.neverReplays());
    }
}
