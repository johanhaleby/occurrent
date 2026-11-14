/*
 *
 *  Copyright 2024 Johan Haleby
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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The handlers an annotation post-processor has registered, so a later scan of the same bean skips them. A handler is
 * keyed by its bean name and its {@link HandlerMethod#signature()}. Every scan reads
 * {@link HandlerScan#implementationClassOf}, which for a proxy is the class its target source names, so a
 * later instance behind a proxy whose target source names the bean's class keys its handlers the way a scan of the
 * plain instance did. A JDK proxy whose target source names no class is scanned as the proxy's own class, where a
 * generic interface handler has the erased parameter type, so that handler gets another signature than the class
 * method a scan of the plain instance found. Internal to the Spring Boot starters, not an API.
 */
public final class RegisteredHandlers {

    private final Set<Key> keys = ConcurrentHashMap.newKeySet();

    /**
     * @return {@code true} if {@code handler} of {@code beanName} is registered
     */
    public boolean isRegistered(String beanName, HandlerMethod handler) {
        return keys.contains(new Key(beanName, handler.signature()));
    }

    /**
     * Records {@code handler} of {@code beanName} as registered, in one atomic add, so of two concurrent calls for the
     * same handler only one gets {@code true}.
     *
     * @return {@code true} if this call recorded it, {@code false} if it was already registered
     */
    public boolean markRegistered(String beanName, HandlerMethod handler) {
        return keys.add(new Key(beanName, handler.signature()));
    }

    /**
     * Forgets {@code handler} of {@code beanName}, so a later scan registers it again.
     */
    public void forgetRegistered(String beanName, HandlerMethod handler) {
        keys.remove(new Key(beanName, handler.signature()));
    }

    private record Key(String beanName, String signature) {
    }
}
