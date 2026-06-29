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

package org.occurrent.eventstore.mongodb.dcb.internal;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbQueryItem;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The driver-agnostic DCB marker model and storage contract shared by the MongoDB event stores. It derives the conflict
 * markers a query and an append touch, names the support collections and documents, and validates and canonicalizes DCB
 * events. Keeping this in one place is what guarantees the two MongoDB stores write the same on-disk contract, which the
 * optimistic-concurrency check depends on.
 */
@NullMarked
public final class DcbMarkerModel {

    public static final String DCB_POSITION_DOCUMENT_ID = "dcb";
    public static final String DCB_COUNTER_POSITION = "position";
    public static final String CHECKPOINT_LAST_POSITION = "lastPosition";
    public static final String CHECKPOINT_VERSION = "version";

    private static final String MARKER_ID_PREFIX = "marker:";
    private static final String POSITION_COLLECTION_SUFFIX = "_dcb_position";
    private static final String CHECKPOINT_COLLECTION_SUFFIX = "_dcb_checkpoints";

    private DcbMarkerModel() {
    }

    public static String positionCollectionName(String eventStoreCollectionName) {
        return eventStoreCollectionName + POSITION_COLLECTION_SUFFIX;
    }

    public static String checkpointCollectionName(String eventStoreCollectionName) {
        return eventStoreCollectionName + CHECKPOINT_COLLECTION_SUFFIX;
    }

    public static String markerId(String key) {
        return MARKER_ID_PREFIX + key;
    }

    public static Set<String> queryMarkerKeys(DcbQuery query) {
        if (query instanceof DcbQuery.MatchAll) {
            return Set.of("all");
        }
        // Decompose into one key per attribute (a key per tag, a key per type) and NEVER combine them into a single
        // "type:X+tag:t" key. Skew-safety (ADR 0021) depends on this: a conflicting event shares a marker via whichever
        // single attribute made it match, and a combined key would share nothing with an event that carries only one
        // of the attributes through a different query.
        TreeSet<String> keys = new TreeSet<>();
        for (DcbQueryItem item : dcbQueryItems(query)) {
            item.tags().forEach(tag -> keys.add("tag:" + tag));
            item.types().forEach(type -> keys.add("type:" + type));
        }
        return keys;
    }

    // A query here is either a single DcbQueryItem alternative or an Items list (MatchAll is handled by the callers).
    public static List<DcbQueryItem> dcbQueryItems(DcbQuery query) {
        if (query instanceof DcbQueryItem item) {
            return List.of(item);
        }
        return ((DcbQuery.Items) query).items();
    }

    public static Set<String> eventMarkerKeys(List<CloudEvent> events) {
        TreeSet<String> keys = new TreeSet<>();
        for (CloudEvent event : events) {
            DcbCloudEvents.getTags(event).forEach(tag -> keys.add("tag:" + tag));
            keys.add("type:" + event.getType());
        }
        return keys;
    }

    public static Set<String> boundaryTagsOf(List<CloudEvent> events) {
        return events.stream().flatMap(event -> DcbCloudEvents.getTags(event).stream()).collect(Collectors.toCollection(TreeSet::new));
    }

    public static List<CloudEvent> validateDcbEvents(List<CloudEvent> events) {
        requireNonNull(events, "Events cannot be null");
        List<CloudEvent> copy = List.copyOf(events);
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("Events cannot be empty");
        }
        return copy.stream()
                .map(event -> DcbCloudEvents.withTags(event, DcbCloudEvents.getTags(event)))
                .toList();
    }
}
