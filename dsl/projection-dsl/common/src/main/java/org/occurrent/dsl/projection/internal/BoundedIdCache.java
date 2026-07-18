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

package org.occurrent.dsl.projection.internal;

import org.jspecify.annotations.NullMarked;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

/**
 * A bounded set of event ids that de-duplicates the replay-to-live overlap in the catch-up feeds. It retains
 * the most recently added ids up to {@code maxSize}, evicting the oldest once the cap is reached. Not thread-safe: the
 * catch-up pipelines serialize all access to it.
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

    public boolean contains(String id) {
        return ids.contains(id);
    }

    public void add(String id) {
        if (ids.add(id)) {
            order.add(id);
            if (order.size() > maxSize) {
                ids.remove(order.poll());
            }
        }
    }
}
