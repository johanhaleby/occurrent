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
 * Insertion-ordered set of replayed event ids the inclusive live resume re-delivers at the handover seam, so the
 * pipeline can suppress those duplicates. Grows to cover the replay-to-live overlap (bounded by write volume during
 * replay, not total history) up to {@code ceiling}, then evicts oldest-first.
 * <p>
 * Dedup is id-based with a fixed ceiling: exceeding it evicts entries and causes duplicate delivery, never loss.
 * Never dedupes by position, so a late-committing low-position event, absent from the forward-only replay, is
 * always delivered by the live change stream.
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
