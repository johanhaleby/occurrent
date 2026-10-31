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

package org.occurrent.tck.eventstore.blocking;

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.BeforeEach;
import org.occurrent.tck.latecommit.LateCommits;

/**
 * What {@link DcbLateCommitConformance} builds on. It touches {@link EventStoreFixture#lateCommits()} in setup, so a
 * fixture that never answered fails before the first assertion rather than halfway through a test.
 */
@NullMarked
abstract class LateCommitConformance extends EventStoreConformance {

    @BeforeEach
    final void touchTheLateCommits() {
        fixture().lateCommits();
    }

    final LateCommits lateCommits() {
        return fixture().lateCommits();
    }
}
