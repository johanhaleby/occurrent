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
import java.util.List;

/**
 * Internal, not part of the public API. The FactoryBean instances the container is initializing on this thread.
 * <p>
 * Spring passes a FactoryBean's product through {@code postProcessAfterInitialization} under the factory's bean name,
 * the same callback and name it passes the factory itself through, and a product can implement {@code FactoryBean}
 * too. So the type of what arrives there doesn't say which of the two it is. The factory is the instance that went
 * through {@code postProcessBeforeInitialization} on this thread first, and a product never goes through it.
 * <p>
 * Held weakly, since Spring calls no {@code postProcessAfterInitialization} when a factory's initialization throws.
 */
public final class FactoriesInInitialization {
    private final ThreadLocal<List<WeakReference<Object>>> initializingOnThisThread = new ThreadLocal<>();

    /**
     * Called from {@code postProcessBeforeInitialization} with a {@code FactoryBean} the container is initializing.
     */
    public void initializationStarts(Object factory) {
        List<WeakReference<Object>> initializing = initializingOnThisThread.get();
        if (initializing == null) {
            initializing = new ArrayList<>();
            initializingOnThisThread.set(initializing);
        }
        initializing.removeIf(reference -> reference.refersTo(null));
        initializing.add(new WeakReference<>(factory));
    }

    /**
     * Called from {@code postProcessAfterInitialization} with what arrived there.
     *
     * @return true when {@code bean}, or the target a proxy around it wraps, is a factory whose initialization started
     * on this thread, which then ends. False for anything else, a factory's product included.
     */
    public boolean initializationEnds(Object bean) {
        @Nullable List<WeakReference<Object>> initializing = initializingOnThisThread.get();
        if (initializing == null) {
            return false;
        }
        Object target = SubscriptionAnnotations.ultimateTarget(bean);
        boolean factory = initializing.removeIf(reference -> reference.refersTo(bean) || reference.refersTo(target));
        initializing.removeIf(reference -> reference.refersTo(null));
        if (initializing.isEmpty()) {
            initializingOnThisThread.remove();
        }
        return factory;
    }
}
