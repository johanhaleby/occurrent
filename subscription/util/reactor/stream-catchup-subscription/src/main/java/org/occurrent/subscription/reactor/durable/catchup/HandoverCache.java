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

package org.occurrent.subscription.reactor.durable.catchup;

import org.jspecify.annotations.NullMarked;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * An insertion-ordered set of replayed event ids that the inclusive live resume re-delivers at the handover seam, so
 * the pipeline can suppress those duplicates. It holds the ids delivered during the replay window (the overlap the
 * live change stream re-delivers, bounded by the write volume during the replay, not by total history), and grows to
 * cover that overlap up to {@code ceiling}. Once the set would exceed {@code ceiling} it evicts oldest-first.
 * <p>
 * Eviction is loss-safe by construction: dropping an id can only stop a re-delivered live event from being suppressed,
 * so an overlap larger than {@code ceiling} yields extra duplicate deliveries, never loss. Dedup is by id, never by
 * position, so a low-position event that commits late (after the handover advanced past its position) and is therefore
 * absent from the forward-only replay is never in this set and is always delivered by the live change stream.
 */
@NullMarked
final class HandoverCache {
    private final int ceiling;
    private final Set<String> ids;

    public HandoverCache(int ceiling) {
        if (ceiling < 1) {
            throw new IllegalArgumentException("ceiling must be at least 1, was " + ceiling);
        }
        this.ceiling = ceiling;
        this.ids = Collections.synchronizedSet(new LinkedHashSet<>());
    }

    public void add(String id) {
        synchronized (ids) {
            if (ids.add(id) && ids.size() > ceiling) {
                Iterator<String> iterator = ids.iterator();
                iterator.next();
                iterator.remove();
            }
        }
    }

    public boolean contains(String id) {
        return ids.contains(id);
    }
}
