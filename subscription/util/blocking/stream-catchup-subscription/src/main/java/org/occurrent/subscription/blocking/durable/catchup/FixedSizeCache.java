/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.blocking.durable.catchup;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An insertion-ordered cache of delivered event ids that grows to cover the replay-to-live overlap up to {@code size},
 * evicting oldest-first once it would exceed it. Used both to dedupe an overlapping catch-up reconciliation re-read and
 * to skip, in the live consumer, events already delivered during catch-up at the handover seam. Shared by the stream
 * and DCB catch-up paths. The overlap it must cover is bounded by the write volume during the replay (the live change
 * stream resumes from a recent token, not from history), not by total history.
 * <p>
 * Eviction is loss-safe by construction: dropping an id can only stop a re-delivered live event from being suppressed,
 * so an overlap larger than {@code size} yields extra duplicate deliveries, never loss (delivery is at-least-once).
 * Dedup is by id, never by position, so a low-position event that commits late (after the handover advanced past its
 * position) is never in this cache and is always delivered by the live subscription.
 * <p>
 * Written on the catch-up (replay) thread and read on the live delivery thread at the handover seam, so every access
 * is synchronized, matching the reactor {@code HandoverCache}.
 */
@NullMarked
final class FixedSizeCache {
    private final LinkedHashMap<String, @Nullable String> cacheContent;

    public FixedSizeCache(int size) {
        cacheContent = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return this.size() > size;
            }
        };
    }

    public synchronized void put(String value) {
        cacheContent.put(value, null);
    }

    public synchronized boolean isCached(String key) {
        return cacheContent.containsKey(key);
    }
}
