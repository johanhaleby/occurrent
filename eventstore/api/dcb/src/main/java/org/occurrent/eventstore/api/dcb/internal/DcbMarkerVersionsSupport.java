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
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;

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

    /**
     * How many stripes the marker of an event type is split into. Every node that shares a store has to agree on it,
     * so it's a constant and not a setting.
     */
    public static final int STRIPES = 8;

    private static final String TYPE_KEY_PREFIX = "type:";
    private static final String STRIPE_KEY_PREFIX = "typestripe:";

    private DcbMarkerVersionsSupport() {
    }

    /**
     * The marker keys of {@code criteria}, or {@link #MATCH_ALL_MARKER_KEY} for {@link DcbCriteria.MatchAll}. An item
     * with at least one tag gives one key per tag and none for its types, since every event it matches has all of its
     * tags. An item without tags gives every stripe of each of its types, whatever any setting says, since an event it
     * matches increments one stripe of its type. Never combine a tag and a type into one key, ADR 21 depends on it.
     */
    public static Set<String> queryMarkerKeys(DcbCriteria criteria) {
        requireNonNull(criteria, "Criteria cannot be null");
        if (criteria instanceof DcbCriteria.MatchAll) {
            return Set.of(MATCH_ALL_MARKER_KEY);
        }
        TreeSet<String> keys = new TreeSet<>();
        for (DcbCriterion item : items(criteria)) {
            if (item.tags().isEmpty()) {
                item.types().forEach(type -> keys.addAll(stripeKeys(type)));
            } else {
                item.tags().forEach(tag -> keys.add(tagKey(tag)));
            }
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
     * The marker keys of one event, one per tag and one for its type, on {@code stripe}.
     */
    public static SortedSet<String> eventMarkerKeys(CloudEvent event, int stripe) {
        TreeSet<String> keys = new TreeSet<>();
        DcbCloudEvents.getTags(event).forEach(tag -> keys.add(tagKey(tag)));
        keys.add(stripeKey(event.getType(), stripe));
        return keys;
    }

    /**
     * The union of the marker keys of {@code events}, with each type on {@code stripe}.
     */
    public static SortedSet<String> eventMarkerKeys(List<CloudEvent> events, int stripe) {
        TreeSet<String> keys = new TreeSet<>();
        events.forEach(event -> keys.addAll(eventMarkerKeys(event, stripe)));
        return keys;
    }

    /**
     * The stripe an append increments for each type of its events when type marker writes are spread, the same for
     * every event and every attempt of the append.
     */
    public static int stripeOf(AppendId appendId) {
        return Math.floorMod(appendId.value().hashCode(), STRIPES);
    }

    /**
     * The marker keys a read after versions selects the events of {@code item} by. An event matching the item carries
     * all its tags, so the item's first tag key in canonical order selects all of them. An item without tags has
     * every stripe of each type instead, and an event matching it has one of them.
     */
    public static List<String> selectionKeys(DcbCriterion item) {
        if (!item.tags().isEmpty()) {
            return List.of(item.tags().stream().map(DcbMarkerVersionsSupport::tagKey).min(Comparator.naturalOrder()).orElseThrow());
        }
        return item.types().stream().flatMap(type -> stripeKeys(type).stream()).sorted().toList();
    }

    public static String tagKey(Tag tag) {
        return "tag:" + tag.canonical();
    }

    /**
     * The marker key of stripe {@code stripe} of {@code type}, from 0 to {@link #STRIPES} - 1. Stripe 0 is the key a
     * 0.34 node uses for the type. A prefix of its own keeps the other stripes from colliding with the key of a type
     * whose name looks like a stripe.
     */
    public static String stripeKey(String type, int stripe) {
        if (stripe < 0 || stripe >= STRIPES) {
            throw new IllegalArgumentException("Stripe must be from 0 to " + (STRIPES - 1) + ", was " + stripe);
        }
        return stripe == 0 ? TYPE_KEY_PREFIX + type : STRIPE_KEY_PREFIX + stripe + ":" + type;
    }

    /**
     * Whether {@code key} is a stripe key of some type, stripe 0 included.
     */
    public static boolean isTypeKey(String key) {
        return key.startsWith(TYPE_KEY_PREFIX) || key.startsWith(STRIPE_KEY_PREFIX);
    }

    private static List<String> stripeKeys(String type) {
        List<String> keys = new ArrayList<>(STRIPES);
        for (int stripe = 0; stripe < STRIPES; stripe++) {
            keys.add(stripeKey(type, stripe));
        }
        return keys;
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
     * The versions of {@code keys}, the marker keys of {@code criteria}, present only when every key has a reportable
     * marker in {@code observed}. The exception is a type key whose marker is missing or has version 0, which is
     * reported as absent.
     */
    public static Optional<DcbMarkerVersions> versionsOf(DcbCriteria criteria, Set<String> keys, Map<String, MarkerObservation> observed) {
        requireNonNull(criteria, "Criteria cannot be null");
        if (keys.isEmpty()) {
            return Optional.empty();
        }
        Map<String, DcbMarkerVersions.MarkerVersion> versions = new TreeMap<>();
        for (String key : keys) {
            MarkerObservation marker = observed.get(key);
            if (isAbsent(marker) && isTypeKey(key)) {
                versions.put(key, DcbMarkerVersions.MarkerVersion.absent());
                continue;
            }
            if (marker == null || !reportable(marker)) {
                return Optional.empty();
            }
            versions.put(key, new DcbMarkerVersions.MarkerVersion(requireNonNull(marker.epoch()), marker.version(), marker.unstamped()));
        }
        return Optional.of(DcbMarkerVersions.of(criteria, versions));
    }

    /**
     * Throws {@link DcbSnapshotInvalidException} unless the events after {@code saved} can be selected from the
     * markers in {@code observed}. That needs {@code saved} to be read with {@code criteria}, the saved keys to be the
     * marker keys of {@code criteria}, and for each key
     * a marker with an epoch, the saved epoch, a version at or above the saved one, and the saved unstamped count.
     * A saved entry may be absent for a type key only. A type marker that is still missing then passes, and one that
     * exists now needs an epoch and an unstamped count of 0.
     */
    public static void requireResumable(DcbCriteria criteria, DcbMarkerVersions saved, Map<String, MarkerObservation> observed) {
        if (criteria instanceof DcbCriteria.MatchAll) {
            throw new DcbSnapshotInvalidException(criteria, saved, "Cannot read after marker versions for criteria that match every event");
        }
        if (!saved.readWith(criteria)) {
            throw new DcbSnapshotInvalidException(criteria, saved, "The saved marker versions were read with other criteria, " + saved.criteria());
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
            MarkerObservation marker = observed.get(key);
            if (savedVersion.equals(DcbMarkerVersions.MarkerVersion.absent())) {
                if (!isTypeKey(key)) {
                    throw new DcbSnapshotInvalidException(criteria, saved, "The saved marker versions have an absent entry for marker key " + key + ", which isn't a type marker key");
                }
                if (isAbsent(marker)) {
                    continue;
                }
                if (marker.epoch() == null) {
                    throw new DcbSnapshotInvalidException(criteria, saved, "The marker for marker key " + key + " has no epoch");
                }
                if (marker.unstamped() != 0) {
                    throw new DcbSnapshotInvalidException(criteria, saved, "The marker for marker key " + key + " was incremented by a writer that wrote no marker versions");
                }
                continue;
            }
            if (isAbsent(marker)) {
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

    private static boolean isAbsent(@Nullable MarkerObservation marker) {
        return marker == null || marker.version() == 0;
    }

    // A marker can be reported when it has an epoch and values a DcbMarkerVersions.MarkerVersion accepts
    private static boolean reportable(MarkerObservation marker) {
        return marker.epoch() != null && !marker.epoch().isBlank() && marker.version() >= 1 && marker.unstamped() >= 0 && marker.unstamped() <= marker.version();
    }
}
