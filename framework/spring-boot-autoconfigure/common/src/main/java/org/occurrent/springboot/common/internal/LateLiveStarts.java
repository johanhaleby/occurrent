/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.common.internal;

import org.jspecify.annotations.Nullable;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

/**
 * Internal, not part of the public API. The start boundary each bean built after startup read as it was created,
 * kept until the annotation post-processor registers that bean's handlers.
 * <p>
 * Keyed by the instance, so two creations of one bean name at the same time, a prototype or a scoped bean built on
 * two threads, each keep the boundary they read. The instance is held weakly, since Spring calls no post-processor
 * when a bean's initialization throws, and a strong key would keep every instance whose creation failed alive until
 * the context closes.
 */
public final class LateLiveStarts {
    private final ReferenceQueue<Object> collected = new ReferenceQueue<>();
    private final Map<InstanceKey, String> liveStartByInstance = new HashMap<>();

    public synchronized void put(Object instance, String liveStart) {
        removeCollected();
        liveStartByInstance.put(new InstanceKey(instance, collected), liveStart);
    }

    public synchronized @Nullable String remove(Object instance) {
        removeCollected();
        return liveStartByInstance.remove(new InstanceKey(instance, null));
    }

    private void removeCollected() {
        Reference<?> key;
        while ((key = collected.poll()) != null) {
            liveStartByInstance.remove(key);
        }
    }

    // Equal only to a key for the same instance, by identity, since a bean's own equals and hashCode can change
    // while it is initialized. A key whose instance is collected is equal only to itself.
    private static final class InstanceKey extends WeakReference<Object> {
        private final int identityHash;

        InstanceKey(Object instance, @Nullable ReferenceQueue<Object> queue) {
            super(instance, queue);
            this.identityHash = System.identityHashCode(instance);
        }

        @Override
        public int hashCode() {
            return identityHash;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof InstanceKey key)) {
                return false;
            }
            Object instance = get();
            return instance != null && instance == key.get();
        }
    }
}
