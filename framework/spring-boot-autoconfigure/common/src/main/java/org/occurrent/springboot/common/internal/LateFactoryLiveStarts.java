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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal, not part of the public API. The start boundary each FactoryBean built after startup read as it was created,
 * for the products it builds.
 * <p>
 * Spring calls a post-processor before a factory's own initialization but none before its {@code getObject}, so the
 * boundary belongs to the creation of the factory. A product never takes a boundary read after its {@code getObject}
 * started, so a write the factory or {@code getObject} makes, or one made on another thread meanwhile, reaches it. It
 * can get events written before it was built, and which ones depends on its factory:
 * <ul>
 *     <li>A singleton or scoped factory's product takes the boundary that factory read. When the product is built in
 *     the {@code getBean} call that created the factory, that's every event written from that call on. When the factory
 *     was created in an earlier call, by a {@code getBean} of the factory itself, or the product is a later one of a
 *     factory whose products aren't cached, it's also every event written between the factory's creation and the
 *     product's.</li>
 *     <li>A prototype factory is created in the same call as its product. The product takes the boundary of the oldest
 *     factory under its bean name that was created on its thread, hasn't been claimed by a product and hasn't been
 *     garbage collected. Its own factory is one of them, so that boundary is never later than its own. It's earlier
 *     when an older one is still there, which is the factory whose {@code getObject} asked for this product under its
 *     own name, one asked for with {@code getBean} of the factory itself, or one whose creation failed and that isn't
 *     collected yet. The product then also gets every event written since that older factory was created.</li>
 * </ul>
 * A new prototype factory reads its boundary with {@link #newestUnclaimedLiveStart(String)} as the earliest the event
 * store may return, so of those factories the oldest is the earliest even when two reads of the store could go back,
 * as two MongoDB sessions can when the primary changes. A product whose factory was built before the startup scan
 * ended takes none, and starts at the boundary of the context. A prototype factory built then is recorded with no
 * boundary, so a product that takes the boundary of the oldest one starts there too.
 * <p>
 * Keyed by the factory instance and held weakly, so a factory whose creation failed, or a prototype factory whose
 * product is built, takes its boundary with it once it's collected. The boundary itself goes at the next access after
 * that.
 */
public final class LateFactoryLiveStarts {
    private final LateLiveStarts liveStartByFactory = new LateLiveStarts();
    // Oldest first, and dropped with the thread
    private final ThreadLocal<Map<String, List<WeakReference<Object>>>> unclaimedPrototypeFactoriesOfThisThread =
            new ThreadLocal<>();

    /**
     * Called when a factory under {@code beanName} is created, before its initialization.
     *
     * @param liveStart The boundary the factory read, or null when there is none
     */
    public void factoryCreated(String beanName, Object factory, boolean prototype, @Nullable String liveStart) {
        if (liveStart != null) {
            liveStartByFactory.put(factory, liveStart);
        }
        if (prototype) {
            Map<String, List<WeakReference<Object>>> unclaimedByName = unclaimedPrototypeFactoriesOfThisThread.get();
            if (unclaimedByName == null) {
                unclaimedByName = new HashMap<>();
                unclaimedPrototypeFactoriesOfThisThread.set(unclaimedByName);
            }
            List<WeakReference<Object>> unclaimed =
                    unclaimedByName.computeIfAbsent(beanName, ignored -> new ArrayList<>());
            unclaimed.removeIf(reference -> reference.get() == null);
            unclaimed.add(new WeakReference<>(factory));
        }
    }

    /**
     * @return The boundary {@code factory} read when it was created, or null when it was created before the startup
     * scan ended
     */
    public @Nullable String liveStartOf(Object factory) {
        return liveStartByFactory.get(factory);
    }

    /**
     * @return The boundary of the newest unclaimed, uncollected factory under {@code beanName} on this thread that read
     * one, for the next factory under that name to read its own from, or null when there is none
     */
    public @Nullable String newestUnclaimedLiveStart(String beanName) {
        Map<String, List<WeakReference<Object>>> unclaimedByName = unclaimedPrototypeFactoriesOfThisThread.get();
        List<WeakReference<Object>> unclaimed = unclaimedByName == null ? null : unclaimedByName.get(beanName);
        if (unclaimed == null) {
            return null;
        }
        for (int i = unclaimed.size() - 1; i >= 0; i--) {
            @Nullable Object factory = unclaimed.get(i).get();
            @Nullable String liveStart = factory == null ? null : liveStartByFactory.get(factory);
            if (liveStart != null) {
                return liveStart;
            }
        }
        return null;
    }

    /**
     * Claims the prototype factory created last under {@code beanName} on this thread, for a product that has just been
     * built on it.
     *
     * @return The boundary of the oldest unclaimed factory under that name on this thread, or null when there is none
     */
    public @Nullable String claimPrototypeFactory(String beanName) {
        Map<String, List<WeakReference<Object>>> unclaimedByName = unclaimedPrototypeFactoriesOfThisThread.get();
        List<WeakReference<Object>> unclaimed = unclaimedByName == null ? null : unclaimedByName.get(beanName);
        if (unclaimed == null) {
            return null;
        }
        @Nullable Object oldest = null;
        for (WeakReference<Object> reference : unclaimed) {
            oldest = reference.get();
            if (oldest != null) {
                break;
            }
        }
        unclaimed.removeIf(reference -> reference.get() == null);
        if (!unclaimed.isEmpty()) {
            unclaimed.remove(unclaimed.size() - 1);
        }
        if (unclaimed.isEmpty()) {
            unclaimedByName.remove(beanName);
        }
        return oldest == null ? null : liveStartByFactory.get(oldest);
    }
}
