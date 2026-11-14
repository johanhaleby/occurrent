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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The handlers an annotation post-processor has registered, so a later scan of the same bean skips them. A handler is
 * keyed by its bean name and its {@link HandlerMethod#signature()}, and each key records the methods registered under
 * it. A recorded method is skipped. A method that isn't recorded counts as the handler already registered under its
 * signature, as in 0.34.0, so a non-singleton bean whose factory returns C1 and then an unrelated C2, each with a
 * private handler of one signature, registers the handler of C1 only. The method registers when nothing is recorded
 * under its signature, or when the scan that found it also found a recorded method of that signature, which puts the
 * two in one class tree, such as a private handler of A and a handler of the same signature that its subclass B
 * declares.
 * <p>
 * Every scan reads {@link HandlerScan#implementationClassOf}, so a later instance behind a proxy that is unwrapped, or
 * behind a CGLIB proxy, keys its handlers the way a scan of the plain instance did. A JDK proxy whose target source
 * isn't a fixed singleton is not unwrapped and is scanned as the proxy's own class, where a
 * generic interface handler has the erased parameter type, so that handler gets another signature than the class
 * method a scan of the plain instance found. Internal to the Spring Boot starters, not an API.
 */
public final class RegisteredHandlers {

    private final ConcurrentMap<Key, Set<Method>> registered = new ConcurrentHashMap<>();

    /**
     * @return {@code true} if a scan that found {@code handler} of {@code beanName} skips it
     */
    public boolean isRegistered(String beanName, HandlerMethod handler) {
        Set<Method> recorded = registered.get(new Key(beanName, handler.signature()));
        return recorded != null && !registers(recorded, handler);
    }

    /**
     * Records {@code handler} of {@code beanName} as registered, in one atomic step that also decides whether it
     * registers, so of two concurrent calls for the same handler only one gets {@code true}.
     *
     * @return {@code true} if this call recorded it, {@code false} if it is recorded already or counts as the handler
     * another method is recorded as
     */
    public boolean markRegistered(String beanName, HandlerMethod handler) {
        AtomicBoolean recordedNow = new AtomicBoolean();
        registered.compute(new Key(beanName, handler.signature()), (key, recorded) -> {
            if (recorded != null && !registers(recorded, handler)) {
                return recorded;
            }
            recordedNow.set(true);
            Set<Method> methods = recorded == null ? new HashSet<>() : new HashSet<>(recorded);
            methods.add(handler.method());
            return Set.copyOf(methods);
        });
        return recordedNow.get();
    }

    /**
     * Forgets {@code handler} of {@code beanName}, so a later scan registers it again.
     */
    public void forgetRegistered(String beanName, HandlerMethod handler) {
        registered.computeIfPresent(new Key(beanName, handler.signature()), (key, recorded) -> {
            Set<Method> methods = new HashSet<>(recorded);
            methods.remove(handler.method());
            return methods.isEmpty() ? null : Set.copyOf(methods);
        });
    }

    // A key is only there while it records a method, so a recorded set is never empty
    private static boolean registers(Set<Method> recorded, HandlerMethod handler) {
        return !recorded.contains(handler.method()) && !Collections.disjoint(recorded, handler.sameSignature());
    }

    private record Key(String beanName, String signature) {
    }
}
