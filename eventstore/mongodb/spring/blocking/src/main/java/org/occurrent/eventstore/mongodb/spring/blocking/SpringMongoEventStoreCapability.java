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

package org.occurrent.eventstore.mongodb.spring.blocking;

/**
 * Selects which event-store APIs and MongoDB support structures a {@link SpringMongoEventStore} should enable.
 * <p>
 * Capabilities control indexes, support collections, and fail-fast guards. They do not change the stored CloudEvent format.
 */
public enum SpringMongoEventStoreCapability {
    /**
     * Enables stream-based event-store reads, writes, queries, and operations.
     */
    STREAM,

    /**
     * Enables Dynamic Consistency Boundary reads and appends.
     */
    DCB
}
