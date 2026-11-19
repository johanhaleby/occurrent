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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Internal, not part of the public API. The start boundary a FactoryBean built after startup read for its product,
 * kept until the annotation post-processor sees that product.
 * <p>
 * The product is a different instance from the factory, so the two meet only by bean name. A singleton FactoryBean is
 * created once, so its product takes the boundary whatever thread asks for it. Two creations of a prototype or scoped
 * FactoryBean can run at the same time on two threads, and Spring builds each one's product on the thread that
 * created its factory, inside the same {@code getBean} call. So such a boundary is kept for that thread only, and each
 * product takes the boundary its own creation read.
 * <p>
 * Spring calls no post-processor when a factory's initialization or its {@code getObject} throws, so nothing removes
 * the boundary that creation read. It stays as one entry per bean name, for a singleton, or per bean name and thread
 * that is still alive, for any other scope, and the next creation under that name, on that thread, replaces it.
 */
public final class LateFactoryLiveStarts {
    private final Map<String, String> singletonLiveStarts = new ConcurrentHashMap<>();
    // Dropped with the thread, so a failed creation on a thread that has ended keeps no entry
    private final ThreadLocal<Map<String, String>> liveStartsOfThisThread = ThreadLocal.withInitial(HashMap::new);

    /**
     * @param liveStart The boundary the factory read, or null when there is none, which drops an earlier one
     */
    public void put(String beanName, boolean singleton, @Nullable String liveStart) {
        Map<String, String> liveStarts = liveStartsFor(singleton);
        if (liveStart == null) {
            liveStarts.remove(beanName);
        } else {
            liveStarts.put(beanName, liveStart);
        }
    }

    public @Nullable String remove(String beanName, boolean singleton) {
        return liveStartsFor(singleton).remove(beanName);
    }

    private Map<String, String> liveStartsFor(boolean singleton) {
        return singleton ? singletonLiveStarts : liveStartsOfThisThread.get();
    }
}
