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

package org.occurrent.example.projection.globalposition;

import org.occurrent.domain.DomainEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A minimal read model that is rebuilt by replaying domain events in global position order, regardless of which
 * event stream (person) each event belongs to.
 * <p>
 * This is the "projection" side of the global position example: instead of catching up per stream, the projection
 * consumes a single, unified, monotonically increasing sequence that interleaves events from every stream in the
 * exact order they were written to the event store.
 */
public class NameProjection {

    private final Map<String, String> currentNameByPersonId = new ConcurrentHashMap<>();
    private final List<String> appliedInOrder = new ArrayList<>();

    public void apply(DomainEvent event) {
        currentNameByPersonId.put(event.userId(), event.name());
        appliedInOrder.add(event.eventId());
    }

    public String currentNameOf(String personId) {
        return currentNameByPersonId.get(personId);
    }

    /**
     * @return the ids of the events applied to this projection, in the order they were applied. Useful for asserting
     * that a rebuild from the global position replayed events in write order across streams.
     */
    public List<String> appliedEventIdsInOrder() {
        return List.copyOf(appliedInOrder);
    }
}
