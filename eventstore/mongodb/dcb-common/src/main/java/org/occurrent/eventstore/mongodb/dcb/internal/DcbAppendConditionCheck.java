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

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The check of a conditional append with a consistency token on the MongoDB stores (ADR 149). It compares what the read
 * saw of each marker with the markers inside the append's transaction, and says whether that settles the condition or
 * the store has to look at the events.
 */
@NullMarked
public final class DcbAppendConditionCheck {

    private DcbAppendConditionCheck() {
    }

    /**
     * What the markers say about a condition.
     */
    public sealed interface Outcome {
    }

    /**
     * The markers show no event matching the criteria committed after the read.
     */
    public record Pass() implements Outcome {
    }

    /**
     * The markers can't rule out a matching event committed after the read, and the events can't be asked.
     */
    public record Refuse() implements Outcome {
    }

    /**
     * The store has to look for a matching event above these versions.
     *
     * @param items the criteria items the markers didn't settle
     */
    public record Query(List<ItemQuery> items) implements Outcome {
    }

    /**
     * The events of {@code item} to look for.
     *
     * @param item       the criteria item
     * @param selections the marker keys that select them, each with the version the read saw
     */
    public record ItemQuery(DcbCriterion item, List<KeySelection> selections) {
    }

    /**
     * An event is selected by an {@code mv} element with this key, the epoch of the marker and a version above this one.
     */
    public record KeySelection(String key, long version) {
    }

    /**
     * Compares {@code current}, the markers read inside the append's transaction, with what {@code token} saw.
     *
     * @param token    the token of the condition
     * @param criteria the criteria of the condition
     * @param current  the marker observation of every marker key of {@code criteria}
     * @throws IllegalArgumentException if the token doesn't hold exactly the marker keys of {@code criteria}, which means
     *                                  this store's read didn't create it
     */
    public static Outcome check(DcbConsistencyToken token, DcbCriteria criteria, Map<String, MarkerObservation> current) {
        Set<String> keys = DcbMarkerVersionsSupport.queryMarkerKeys(criteria);
        Map<String, MarkerObservation> read = token.markers();
        if (!read.keySet().equals(keys)) {
            throw new IllegalArgumentException("The consistency token holds the marker keys " + read.keySet() + " but the criteria have the marker keys " + keys + ", so this event store's read didn't create it");
        }
        boolean moved = false;
        for (String key : keys) {
            MarkerObservation atRead = read.get(key);
            MarkerObservation now = Objects.requireNonNull(current.get(key), () -> "No observation of marker key " + key);
            if (saw(atRead) && (now.version() < atRead.version() || !Objects.equals(atRead.epoch(), now.epoch()))) {
                return new Refuse();
            }
            moved |= moved(atRead, now);
        }
        if (!moved) {
            return new Pass();
        }
        if (criteria instanceof DcbCriteria.MatchAll) {
            return new Refuse();
        }
        List<ItemQuery> items = new ArrayList<>();
        for (DcbCriterion item : DcbMarkerVersionsSupport.items(criteria)) {
            List<String> movedKeys = DcbMarkerVersionsSupport.queryMarkerKeys(item).stream().filter(key -> moved(read.get(key), current.get(key))).toList();
            if (ruledOutByTheMarkers(item, movedKeys)) {
                continue;
            }
            for (String key : movedKeys) {
                if (!eventsSelectableAbove(read.get(key), current.get(key))) {
                    return new Refuse();
                }
            }
            List<String> selectionKeys = item.tags().isEmpty() ? movedKeys : DcbMarkerVersionsSupport.selectionKeys(item);
            items.add(new ItemQuery(item, selectionKeys.stream().map(key -> new KeySelection(key, read.get(key).version())).toList()));
        }
        return items.isEmpty() ? new Pass() : new Query(items);
    }

    private static boolean saw(MarkerObservation atRead) {
        return atRead.version() > 0;
    }

    private static boolean moved(MarkerObservation atRead, MarkerObservation now) {
        return atRead.version() != now.version();
    }

    // A matching event increments every tag marker of an item with tags, so one marker that didn't move rules it out. It
    // increments one of the type markers of an item without tags, so that item needs one that moved. The check doesn't
    // read the events for this, so it also holds for a writer that writes no mv.
    private static boolean ruledOutByTheMarkers(DcbCriterion item, List<String> movedKeys) {
        if (item.tags().isEmpty()) {
            return movedKeys.isEmpty();
        }
        return movedKeys.size() < item.tags().size();
    }

    // A marker without an epoch was last written by 0.34, so its events may have no mv. The epoch hasn't changed since the
    // read when the read saw the marker. A u that moved means a writer that writes no mv incremented the marker after the
    // read.
    private static boolean eventsSelectableAbove(MarkerObservation atRead, MarkerObservation now) {
        return now.epoch() != null && atRead.unstamped() == now.unstamped();
    }
}
