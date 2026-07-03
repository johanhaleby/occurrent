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

package org.occurrent.eventstore.mongodb.migration.positionbackfill;

import org.jspecify.annotations.NullMarked;

/**
 * The outcome of a {@link PositionBackfill#run()} call.
 *
 * @param eventsPositioned The number of events that had a {@code position} assigned during this call. A re-run
 *                          after completion reports {@code 0} (idempotent no-op).
 * @param seededCounterTo  The value the position counter was seeded to (or found already at) before backfilling
 *                          began.
 * @param completed        {@code true} when every un-positioned event in the collection now has a position.
 */
@NullMarked
public record PositionBackfillResult(long eventsPositioned, long seededCounterTo, boolean completed) {
}
