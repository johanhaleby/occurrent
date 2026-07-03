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

/**
 * The persisted checkpoint document fields for a backfill run. One checkpoint document is kept per event store
 * collection so a crashed or interrupted backfill resumes from the last successfully processed {@code _id} instead
 * of restarting from the beginning.
 */
final class PositionBackfillCheckpoint {

    static final String CHECKPOINT_DOCUMENT_ID = "positionBackfill";
    static final String FIELD_LAST_PROCESSED_ID = "lastProcessedId";
    static final String FIELD_PROCESSED_COUNT = "processedCount";
    static final String FIELD_COMPLETED = "completed";

    private PositionBackfillCheckpoint() {
    }
}
