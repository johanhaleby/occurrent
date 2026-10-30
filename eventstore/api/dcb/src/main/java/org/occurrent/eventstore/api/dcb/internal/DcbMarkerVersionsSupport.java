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

package org.occurrent.eventstore.api.dcb.internal;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.*;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. The marker keys and the marker-version checks every DCB event store shares, so
 * the in-memory and the MongoDB stores derive the same keys and refuse the same saved versions (ADR 148).
 */
@NullMarked
public final class DcbMarkerVersionsSupport {

    /**
     * The marker key a {@link DcbCriteria.MatchAll} append condition increments.
     */
    public static final String MATCH_ALL_MARKER_KEY = "all";

    private DcbMarkerVersionsSupport() {
    }

    /**
     * What a store observed for one marker in a point-in-time read.
     *
     * @param epoch     the marker's epoch, {@code null} when the marker has none
     * @param version   the marker's version
     * @param unstamped how many of the marker's increments wrote no marker versions
     */
    public record ObservedMarker(@Nullable String epoch, long version, long unstamped) {

        /**
         * Whether the marker can be reported, that is it has an epoch and values a {@link DcbMarkerVersions.MarkerVersion}
         * accepts.
         */
        public boolean reportable() {
            return epoch != null && !epoch.isBlank() && version >= 1 && unstamped >= 0 && unstamped <= version;
        }

        DcbMarkerVersions.MarkerVersion toMarkerVersion() {
            return new DcbMarkerVersions.MarkerVersion(requireNonNull(epoch), version, unstamped);
        }
    }

    /**
     * The marker keys of {@code criteria}, one per tag and one per type of every item, or {@link #MATCH_ALL_MARKER_KEY}
     * for {@link DcbCriteria.MatchAll}. Never combine a tag and a type into one key, ADR 21 depends on it.
     */
    public static Set<String> queryMarkerKeys(DcbCriteria criteria) {
        requireNonNull(criteria, "Criteria cannot be null");
        if (criteria instanceof DcbCriteria.MatchAll) {
            return Set.of(MATCH_ALL_MARKER_KEY);
        }
        TreeSet<String> keys = new TreeSet<>();
        for (DcbCriterion item : items(criteria)) {
            item.tags().forEach(tag -> keys.add(tagKey(tag)));
            item.types().forEach(type -> keys.add(typeKey(type)));
        }
        return Collections.unmodifiableSortedSet(keys);
    }

    /**
     * The items of {@code criteria}, empty for {@link DcbCriteria.MatchAll}.
     */
    public static List<DcbCriterion> items(DcbCriteria criteria) {
        return switch (criteria) {
            case DcbCriteria.MatchAll ignored -> List.of();
            case DcbCriterion item -> List.of(item);
            case DcbCriteria.Items items -> items.items();
        };
    }

    /**
     * The marker keys of one event, one per tag and one for its type.
     */
    public static SortedSet<String> eventMarkerKeys(CloudEvent event) {
        TreeSet<String> keys = new TreeSet<>();
        DcbCloudEvents.getTags(event).forEach(tag -> keys.add(tagKey(tag)));
        keys.add(typeKey(event.getType()));
        return keys;
    }

    /**
     * The union of the marker keys of {@code events}.
     */
    public static SortedSet<String> eventMarkerKeys(List<CloudEvent> events) {
        TreeSet<String> keys = new TreeSet<>();
        events.forEach(event -> keys.addAll(eventMarkerKeys(event)));
        return keys;
    }

    /**
     * The marker keys a read after versions selects the events of {@code item} by. An event matching the item carries
     * all its tags, so the item's first tag key in canonical order selects all of them. An item without tags has one
     * key per type instead, and an event matching it has one of them.
     */
    public static List<String> selectionKeys(DcbCriterion item) {
        if (!item.tags().isEmpty()) {
            return List.of(item.tags().stream().map(DcbMarkerVersionsSupport::tagKey).min(Comparator.naturalOrder()).orElseThrow());
        }
        return item.types().stream().map(DcbMarkerVersionsSupport::typeKey).sorted().toList();
    }

    public static String tagKey(Tag tag) {
        return "tag:" + tag.canonical();
    }

    public static String typeKey(String type) {
        return "type:" + type;
    }

    /**
     * Whether a read with {@code options} may report marker versions. A read with a limit, a skip, backward order or
     * a position bound reads only part of the boundary, so versions over it would skip events on resume. A lower
     * bound counts too, because an event that commits late below it is covered by the versions and never read.
     */
    public static boolean mayReportVersions(DcbReadOptions options) {
        return options.versionsRequested()
                && options.limit().isEmpty()
                && options.skip() == 0
                && options.direction() == DcbReadOptions.Direction.FORWARD
                && options.afterPosition().isEmpty()
                && options.upToPosition().isEmpty();
    }

    /**
     * The versions of {@code keys}, present only when every key has a reportable marker in {@code observed}.
     */
    public static Optional<DcbMarkerVersions> versionsOf(Set<String> keys, Map<String, ObservedMarker> observed) {
        if (keys.isEmpty()) {
            return Optional.empty();
        }
        Map<String, DcbMarkerVersions.MarkerVersion> versions = new TreeMap<>();
        for (String key : keys) {
            ObservedMarker marker = observed.get(key);
            if (marker == null || !marker.reportable()) {
                return Optional.empty();
            }
            versions.put(key, marker.toMarkerVersion());
        }
        return Optional.of(new DcbMarkerVersions(versions));
    }

    /**
     * Throws {@link DcbSnapshotInvalidException} unless the events after {@code saved} can be selected from the
     * markers in {@code observed}. That needs the saved keys to be the marker keys of {@code criteria}, and for each key
     * a marker with an epoch, the saved epoch, a version at or above the saved one, and the saved unstamped count.
     */
    public static void requireResumable(DcbCriteria criteria, DcbMarkerVersions saved, Map<String, ObservedMarker> observed) {
        if (criteria instanceof DcbCriteria.MatchAll) {
            throw new DcbSnapshotInvalidException(criteria, saved, "Cannot read after marker versions for criteria that match every event");
        }
        Set<String> keys = queryMarkerKeys(criteria);
        for (String key : keys) {
            if (!saved.keys().contains(key)) {
                throw new DcbSnapshotInvalidException(criteria, saved, "The saved marker versions have no entry for marker key " + key + " of the criteria");
            }
        }
        for (String key : saved.keys()) {
            if (!keys.contains(key)) {
                throw new DcbSnapshotInvalidException(criteria, saved, "The saved marker versions have an entry for marker key " + key + ", which isn't a marker key of the criteria");
            }
        }
        for (String key : keys) {
            DcbMarkerVersions.MarkerVersion savedVersion = saved.get(key).orElseThrow();
            ObservedMarker marker = observed.get(key);
            if (marker == null) {
                throw new DcbSnapshotInvalidException(criteria, saved, "There is no marker for marker key " + key);
            }
            if (marker.epoch() == null) {
                throw new DcbSnapshotInvalidException(criteria, saved, "The marker for marker key " + key + " has no epoch");
            }
            if (!marker.epoch().equals(savedVersion.epoch())) {
                throw new DcbSnapshotInvalidException(criteria, saved, "The epoch of the marker for marker key " + key + " differs from the saved epoch");
            }
            if (savedVersion.version() > marker.version()) {
                throw new DcbSnapshotInvalidException(criteria, saved, "The saved version of marker key " + key + " is above the version the store read");
            }
            if (savedVersion.unstamped() != marker.unstamped()) {
                throw new DcbSnapshotInvalidException(criteria, saved, "The marker for marker key " + key + " was incremented by a writer that wrote no marker versions");
            }
        }
    }
}
