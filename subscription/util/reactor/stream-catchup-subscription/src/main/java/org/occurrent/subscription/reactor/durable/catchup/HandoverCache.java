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
 * A bounded, insertion-ordered set of recently replayed event ids. The live change stream resumes inclusively and
 * re-delivers events near the captured token that the replay already emitted, and this cache skips those. It only
 * needs the tail the live resume can overlap, not the whole history.
 */
@NullMarked
public final class HandoverCache {
    private final int maxSize;
    private final Set<String> ids;

    public HandoverCache(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be at least 1, was " + maxSize);
        }
        this.maxSize = maxSize;
        this.ids = Collections.synchronizedSet(new LinkedHashSet<>());
    }

    public void add(String id) {
        synchronized (ids) {
            if (ids.add(id) && ids.size() > maxSize) {
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
