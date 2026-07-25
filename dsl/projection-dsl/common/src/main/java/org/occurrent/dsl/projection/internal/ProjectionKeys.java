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

package org.occurrent.dsl.projection.internal;

import org.jspecify.annotations.NullMarked;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.Projection;

/**
 * Tells a legitimate skip apart from a key that silently lost its metadata. Shared by the blocking and reactor
 * materializers so both stacks fail the same way with the same wording.
 */
@NullMarked
public final class ProjectionKeys {

    private ProjectionKeys() {
    }

    /**
     * Call this when a view-instance id resolved to {@code null}, before treating that as "skip this event".
     * <p>
     * A {@code null} id normally means the event maps to no instance and is skipped, which is a documented feature. But
     * a projection keyed on metadata that was folded with {@link EventMetadata#empty()} also produces {@code null},
     * because {@link EventMetadata#getPosition()} and {@link EventMetadata#get(String)} return {@code null} on empty
     * metadata rather than throwing the way the stream accessors do. Left alone, that drops every such event with no
     * error anywhere, which is the failure this guards.
     * <p>
     * The two are distinguished by the conjunction: the key was declared metadata-aware
     * ({@link Projection#metadataKeyed()}) and the metadata it was handed carries nothing. A projection that declares
     * the metadata-aware overload but ignores the metadata still returns a real id, so it never reaches here.
     */
    public static void failIfKeyNeededMetadata(boolean metadataKeyed, EventMetadata metadata) {
        if (metadataKeyed && metadata.getData().isEmpty()) {
            throw new IllegalStateException("Could not resolve the view-instance id: this projection is keyed by event metadata (id(BiFunction)) but the event was folded with empty metadata, so the key resolved to null and the event would have been skipped silently. Supply the metadata alongside the event on whichever sink you feed: accept(metadata, event) on a CatchupProjectionFeed or DomainEventFeed, update(metadata, event) on a MaterializedView, or apply(metadata, event) on the BiFunction from Projections.domainEventFeed(...). Or key with the event-only id(Function) if the key does not need metadata.");
        }
    }
}
