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
 * Insertion-ordered cache of delivered event ids covering the replay-to-live overlap up to {@code size}, evicting
 * oldest-first past that. Dedupes the overlapping reconcile re-read and lets the live consumer skip events already
 * delivered during catch-up at the handover seam. Shared by the stream and DCB catch-up paths. The overlap is
 * bounded by write volume during replay, not total history, since the live change stream resumes from a recent
 * token.
 * <p>
 * Dedup is id-based with a fixed ceiling: exceeding {@code size} evicts entries and causes duplicate delivery,
 * never loss, since delivery is at-least-once. Never dedupes by position, so a late-committing low-position event
 * is always delivered live.
 * <p>
 * Written on the catch-up thread, read on the live thread at the handover seam, so access is synchronized, matching
 * the reactor {@code HandoverCache}.
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
