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

package org.occurrent.subscription.internal;

import org.jspecify.annotations.NullMarked;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

/**
 * A bounded, insertion-ordered set of event ids that de-duplicates the replay-to-live overlap at the handover seam of
 * the catch-up feeds. Recording the replayed ids lets the live consumer skip the events the inclusive live resume
 * re-delivers. The overlap is bounded by write volume during the replay, not by total history, since live delivery
 * resumes from a recent token.
 * <p>
 * Dedup is id-based with a fixed ceiling: it retains the most recently added ids up to {@code maxSize} and evicts
 * oldest-first past that, so exceeding {@code maxSize} causes duplicate delivery, never loss, since delivery is
 * at-least-once. Never dedupes by position, so a late-committing low-position event, absent from the forward-only
 * replay, is always delivered live.
 * <p>
 * Thread-safe. The catch-up pipelines write on the catch-up thread and read on the live thread at the handover seam.
 */
@NullMarked
public final class BoundedIdCache {
    private final int maxSize;
    private final Set<String> ids;
    private final Queue<String> order;

    public BoundedIdCache(int maxSize) {
        this.maxSize = maxSize;
        this.ids = new HashSet<>(Math.min(maxSize, 1024));
        this.order = new ArrayDeque<>();
    }

    public synchronized boolean contains(String id) {
        return ids.contains(id);
    }

    public synchronized void add(String id) {
        if (ids.add(id)) {
            order.add(id);
            if (order.size() > maxSize) {
                ids.remove(order.poll());
            }
        }
    }
}
