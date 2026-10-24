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
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;

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

    public static final String POSITION_DOCUMENT_ID = "dcb";
    public static final String COUNTER_POSITION = "position";
    public static final String CHECKPOINT_LAST_POSITION = "lastPosition";
    public static final String CHECKPOINT_VERSION = "version";
    /**
     * The marker field that counts the increments whose events carry {@link #EVENT_MARKER_VERSIONS}. Every increment a
     * 0.35 writer makes raises it together with {@link #CHECKPOINT_VERSION}, so {@code version - s} counts the
     * increments of writers that wrote none (ADR 148).
     */
    public static final String CHECKPOINT_STAMPED = "s";
    /**
     * The marker field holding the marker's epoch, an {@code ObjectId} set when the marker is created, or by the first
     * 0.35 increment of a marker created before it.
     */
    public static final String CHECKPOINT_EPOCH = "epoch";
    /**
     * The event field holding one element per marker key of the event, with the marker's key, epoch and the version
     * the event's append gave it.
     */
    public static final String EVENT_MARKER_VERSIONS = "mv";
    public static final String EVENT_MARKER_VERSIONS_KEY = "k";
    public static final String EVENT_MARKER_VERSIONS_EPOCH = "e";
    public static final String EVENT_MARKER_VERSIONS_VERSION = "v";

    private static final String MARKER_ID_PREFIX = "marker:";
    private static final String POSITION_COLLECTION_SUFFIX = "_position";
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

    /**
     * The marker key of a marker document id, the inverse of {@link #markerId(String)}.
     */
    public static String markerKey(String markerId) {
        if (!markerId.startsWith(MARKER_ID_PREFIX)) {
            throw new IllegalArgumentException("Not a marker id: " + markerId);
        }
        return markerId.substring(MARKER_ID_PREFIX.length());
    }

    // Decompose into one key per attribute (a key per tag, a key per type) and NEVER combine them into a single
    // "type:X+tag:t" key. Skew-safety (ADR 0021) depends on this: a conflicting event shares a marker via whichever
    // single attribute made it match, and a combined key would share nothing with an event that carries only one
    // of the attributes through a different criteria. The derivation is shared with the in-memory store.
    public static Set<String> queryMarkerKeys(DcbCriteria criteria) {
        return DcbMarkerVersionsSupport.queryMarkerKeys(criteria);
    }

    // MatchAll has no per-item markers. A DcbCriterion is one alternative, and Items contains several.
    public static List<DcbCriterion> dcbQueryItems(DcbCriteria criteria) {
        return DcbMarkerVersionsSupport.items(criteria);
    }

    public static Set<String> eventMarkerKeys(List<CloudEvent> events) {
        return DcbMarkerVersionsSupport.eventMarkerKeys(events);
    }

    public static Set<Tag> tagsOf(List<CloudEvent> events) {
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
