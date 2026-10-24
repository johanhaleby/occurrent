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
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbEventStream;

/**
 * The position a caller may resume from, read through one place so the late-commit suites ask the same question of
 * every store. A fixture cannot override it, which is what stops a store from picking a weaker answer for itself.
 * <p>
 * Today it answers with what callers use now, the position an append or a read reports. The layer that changes what a
 * caller may resume from repoints these methods and nothing else in the suites.
 */
@NullMarked
final class ResumePositions {

    private ResumePositions() {
    }

    /**
     * The position a decider resumes from after its own append.
     */
    static long after(DcbAppendResult append) {
        return append.lastSequencePosition();
    }

    /**
     * The position a view resumes from after a read.
     */
    static long after(DcbEventStream read) {
        return read.lastSequencePosition();
    }

    /**
     * The position a catch-up reader resumes from.
     */
    static long after(PositionOrderedReader reader) {
        return reader.currentPosition();
    }
}
