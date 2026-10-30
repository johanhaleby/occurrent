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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel.*;

/**
 * The marker-version storage contract the MongoDB event stores share (ADR 148): how a marker document's epoch, version
 * and {@code s} counter turn into reported versions, how an event's {@code mv} array is built, which marker versions a
 * read after versions selects by, and the transaction options of the read that reports them.
 */
@NullMarked
public final class DcbMarkerVersionsDocuments {

    private static final String ID = "_id";


    /**
     * How long the read-only transaction that reports marker versions may take to commit, unless configured otherwise.
     */
    public static final Duration DEFAULT_MARKER_VERSIONS_COMMIT_TIMEOUT = Duration.ofSeconds(5);

    private DcbMarkerVersionsDocuments() {
    }

    /**
     * Returns {@code commitTimeout} when it is at least one millisecond and fits in a {@code long} of milliseconds.
     *
     * @throws IllegalArgumentException otherwise
     */
    public static Duration requireValidCommitTimeout(Duration commitTimeout) {
        Objects.requireNonNull(commitTimeout, "Marker versions commit timeout cannot be null");
        if (commitTimeout.compareTo(Duration.ofMillis(1)) < 0) {
            throw new IllegalArgumentException("Marker versions commit timeout must be at least 1 millisecond, was " + commitTimeout);
        }
        try {
            commitTimeout.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Marker versions commit timeout is too large, was " + commitTimeout, e);
        }
        return commitTimeout;
    }

    /**
     * The element of {@code mv} a read after versions selects an event by: an element with this key, this epoch and a
     * version above this one.
     *
     * @param key     the marker key
     * @param epoch   the marker's epoch as stored, so the filter compares the same BSON type
     * @param version the saved version
     */
    public record Selection(String key, Object epoch, long version) {
    }

    /**
     * Indexes marker documents by their marker key.
     */
    public static Map<String, Document> markersByKey(Iterable<Document> markerDocuments) {
        Map<String, Document> markers = new HashMap<>();
        for (Document marker : markerDocuments) {
            Object id = marker.get(ID);
            if (id instanceof String markerId) {
                markers.put(markerKey(markerId), marker);
            }
        }
        return markers;
    }

    /**
     * The epoch of a marker document as an opaque string, {@code null} when it has none a 0.35 writer would set.
     */
    public static @Nullable String epochOf(Document marker) {
        return marker.get(CHECKPOINT_EPOCH) instanceof ObjectId epoch ? epoch.toHexString() : null;
    }

    /**
     * What a point-in-time read observed of each of {@code keys}, with {@link MarkerObservation#absent()} for a key that
     * has no marker. A missing {@code s} counts as 0, which is what a marker no 0.35 writer incremented has.
     */
    public static Map<String, MarkerObservation> observe(Set<String> keys, Map<String, Document> markersByKey) {
        Map<String, MarkerObservation> observed = new HashMap<>();
        for (String key : keys) {
            Document marker = markersByKey.get(key);
            if (marker == null) {
                observed.put(key, MarkerObservation.absent());
            } else {
                long version = longOf(marker, CHECKPOINT_VERSION);
                long stamped = longOf(marker, CHECKPOINT_STAMPED);
                observed.put(key, new MarkerObservation(epochOf(marker), version, version - stamped));
            }
        }
        return observed;
    }

    /**
     * The consistency token of a read with {@code criteria} over the marker documents it loaded. It holds what the read
     * observed of each marker key of {@code criteria}, and the sum of their versions as its version (ADR 149).
     */
    public static DcbConsistencyToken consistencyToken(DcbCriteria criteria, Map<String, Document> markersByKey) {
        Map<String, MarkerObservation> observed = observe(DcbMarkerVersionsSupport.queryMarkerKeys(criteria), markersByKey);
        return DcbConsistencyToken.of(criteria, observed.values().stream().mapToLong(MarkerObservation::version).sum(), observed);
    }

    /**
     * Writes {@code mv} on each event document, one element per marker key of its event with the marker's epoch and
     * the version this append's transaction gave it.
     *
     * @throws IllegalStateException if a marker the append incremented is missing from {@code markersByKey}
     */
    public static void stampMarkerVersions(List<Document> eventDocuments, List<CloudEvent> events, Map<String, Document> markersByKey) {
        if (eventDocuments.size() != events.size()) {
            throw new IllegalStateException("Expected one document per event");
        }
        for (int i = 0; i < events.size(); i++) {
            List<Document> elements = new ArrayList<>();
            for (String key : DcbMarkerVersionsSupport.eventMarkerKeys(events.get(i))) {
                Document marker = markersByKey.get(key);
                if (marker == null) {
                    throw new IllegalStateException("The append incremented marker " + key + " but could not read it back");
                }
                elements.add(new Document(EVENT_MARKER_VERSIONS_KEY, key)
                        .append(EVENT_MARKER_VERSIONS_EPOCH, marker.get(CHECKPOINT_EPOCH))
                        .append(EVENT_MARKER_VERSIONS_VERSION, longOf(marker, CHECKPOINT_VERSION)));
            }
            eventDocuments.get(i).put(EVENT_MARKER_VERSIONS, elements);
        }
    }

    /**
     * The elements of {@code mv} that select the events of {@code item} appended after {@code saved}. Call it only
     * after {@link DcbMarkerVersionsSupport#requireResumable} accepted {@code saved}, so every selection key has a
     * marker with the saved epoch.
     */
    public static List<Selection> selections(DcbCriterion item, Map<String, Document> markersByKey, DcbMarkerVersions saved) {
        List<Selection> selections = new ArrayList<>();
        for (String key : DcbMarkerVersionsSupport.selectionKeys(item)) {
            Document marker = Objects.requireNonNull(markersByKey.get(key), () -> "No marker for " + key);
            long version = saved.get(key).orElseThrow(() -> new IllegalStateException("No saved version for " + key)).version();
            selections.add(new Selection(key, Objects.requireNonNull(marker.get(CHECKPOINT_EPOCH), () -> "No epoch for " + key), version));
        }
        return selections;
    }

    /**
     * The options of the read-only transaction that reports marker versions. They never come from the options a user
     * configured for writes. Read concern snapshot gives one point-in-time view, and the majority commit makes sure
     * that view is majority-committed. A transaction reads from the primary whatever the collection's read preference.
     * {@code timeout} keeps a client-wide {@code timeoutMS} from replacing the bound. The driver then sends the bound as
     * the commit's {@code maxTimeMS} and drops the write concern's {@code wtimeout}.
     */
    public static TransactionOptions readOnlySnapshotTransactionOptions(Duration commitTimeout) {
        long millis = commitTimeout.toMillis();
        return TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.SNAPSHOT)
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(millis, TimeUnit.MILLISECONDS))
                .maxCommitTime(millis, TimeUnit.MILLISECONDS)
                .timeout(millis, TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * The keys of the index a read after marker versions selects events by, one {@code mv} element with a key, an
     * epoch and a version above the saved one.
     */
    public static Document markerVersionsIndex() {
        return new Document(EVENT_MARKER_VERSIONS + "." + EVENT_MARKER_VERSIONS_KEY, 1)
                .append(EVENT_MARKER_VERSIONS + "." + EVENT_MARKER_VERSIONS_EPOCH, 1)
                .append(EVENT_MARKER_VERSIONS + "." + EVENT_MARKER_VERSIONS_VERSION, 1);
    }

    /**
     * The elements of {@code mv} that select the events the check has to look for. Call it only for an item the check
     * returned, so every selection key has a marker with an epoch in {@code markersByKey}.
     */
    public static List<Selection> selections(DcbAppendConditionCheck.ItemQuery item, Map<String, Document> markersByKey) {
        return item.selections().stream()
                .map(selection -> new Selection(selection.key(), Objects.requireNonNull(markersByKey.get(selection.key()), () -> "No marker for " + selection.key()).get(CHECKPOINT_EPOCH), selection.version()))
                .toList();
    }

    /**
     * The filter on the marker documents of {@code keys}.
     */
    public static Bson markersFilter(Set<String> keys) {
        return Filters.in(ID, keys.stream().map(DcbMarkerModel::markerId).toList());
    }

    /**
     * The filter on the events of {@code item} appended after {@code saved}, to combine with the item's own filter.
     * Call it only after {@link DcbMarkerVersionsSupport#requireResumable} accepted {@code saved}.
     */
    public static Bson afterVersionsFilter(DcbCriterion item, Map<String, Document> markersByKey, DcbMarkerVersions saved) {
        return selectionFilter(selections(item, markersByKey, saved));
    }

    /**
     * The filter on an event with an element of {@code mv} that one of {@code selections} selects. {@code $elemMatch}
     * keeps the key, the epoch and the version on one element.
     */
    public static Bson selectionFilter(List<Selection> selections) {
        List<Bson> elements = selections.stream()
                .map(selection -> Filters.elemMatch(EVENT_MARKER_VERSIONS, Filters.and(
                        Filters.eq(EVENT_MARKER_VERSIONS_KEY, selection.key()),
                        Filters.eq(EVENT_MARKER_VERSIONS_EPOCH, selection.epoch()),
                        Filters.gt(EVENT_MARKER_VERSIONS_VERSION, selection.version()))))
                .toList();
        return elements.size() == 1 ? elements.getFirst() : Filters.or(elements);
    }

    /**
     * The update a marker upsert applies: the version and the {@code s} counter go up together, and a marker the
     * upsert inserts gets an epoch.
     */
    public static Bson markerIncrement(long lastPosition) {
        return Updates.combine(
                Updates.inc(CHECKPOINT_VERSION, 1L),
                Updates.inc(CHECKPOINT_STAMPED, 1L),
                Updates.set(CHECKPOINT_LAST_POSITION, lastPosition),
                Updates.setOnInsert(CHECKPOINT_EPOCH, new ObjectId()));
    }

    /**
     * The filter of the update that gives an existing marker without an epoch one, sent in the same unordered bulk
     * write as the marker's upsert. In either order the two leave the marker with exactly one epoch.
     */
    public static Bson markerWithoutEpoch(String key) {
        return Filters.and(Filters.eq(ID, markerId(key)), Filters.exists(CHECKPOINT_EPOCH, false));
    }

    /**
     * The update that gives a marker without an epoch one.
     */
    public static Bson setEpoch() {
        return Updates.set(CHECKPOINT_EPOCH, new ObjectId());
    }

    /**
     * The versions an append reports, present only for a condition with a consistency token over criteria other than
     * {@link org.occurrent.eventstore.api.dcb.DcbCriteria.MatchAll}, and only when every marker key of the criteria
     * has a reportable marker in {@code markersByKey}.
     */
    public static Optional<DcbMarkerVersions> appendedVersions(@Nullable DcbAppendCondition condition, Map<String, Document> markersByKey) {
        if (condition == null || condition.consistencyToken().isEmpty() || condition.criteria() instanceof DcbCriteria.MatchAll) {
            return Optional.empty();
        }
        Set<String> keys = DcbMarkerVersionsSupport.queryMarkerKeys(condition.criteria());
        return DcbMarkerVersionsSupport.versionsOf(keys, observe(keys, markersByKey));
    }


    private static long longOf(Document document, String field) {
        return document.get(field) instanceof Number number ? number.longValue() : 0;
    }
}
