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
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;

/**
 * The position a stream catch-up reader may resume from, read through one place so the stream late-commit suite asks the
 * same question of every store. A fixture cannot override it, which is what stops a store from picking a weaker answer
 * for itself.
 * <p>
 * A DCB caller resumes from marker versions rather than from a position, so no DCB suite uses this class.
 */
@NullMarked
final class ResumePositions {

    private ResumePositions() {
    }

    /**
     * The position a catch-up reader resumes from.
     */
    static long after(PositionOrderedReader reader) {
        return reader.currentPosition();
    }
}
