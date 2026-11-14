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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The handlers an annotation post-processor has registered, so a later scan of the same bean skips them. A handler is
 * keyed by its bean name and its {@link HandlerMethod#signature()}, and each key records the handlers registered under
 * it, each with the class its scan read. A handler registers unless a recorded one is the same handler. When the two
 * scans read one class, or one class extends the other, they are the same handler when an instance of the class
 * further down runs one method for both. A private handler of A and a handler of the same signature that its subclass
 * B declares are two handlers, and both register in whichever order instances of A and B come. When B is recorded and
 * a later instance is of C, which extends B and overrides the handler, C's method is skipped, since a C runs it for B's
 * handler too. Instances of unrelated classes have one handler of a signature, as in 0.34.0, so a non-singleton bean
 * whose factory returns C1 and then an unrelated C2, each with a private handler of one signature, registers the
 * handler of C1 only. The first of them to register keeps it, so when a factory returns A, B and an unrelated U, which
 * handlers register depends on whether A or B comes before U.
 * <p>
 * Every scan reads {@link HandlerScan#implementationClassOf}, so a later instance behind a proxy that is unwrapped, or
 * behind a CGLIB proxy, keys its handlers the way a scan of the plain instance did. A JDK proxy whose target source
 * isn't a fixed singleton is not unwrapped and is scanned as the proxy's own class, where a generic interface handler
 * has the erased parameter type, so that handler gets another signature than the class method a scan of the plain
 * instance found. Internal to the Spring Boot starters, not an API.
 */
public final class RegisteredHandlers {

    private final ConcurrentMap<Key, Set<HandlerMethod>> registered = new ConcurrentHashMap<>();

    /**
     * @return {@code true} if a scan that found {@code handler} of {@code beanName} skips it
     */
    public boolean isRegistered(String beanName, HandlerMethod handler) {
        Set<HandlerMethod> recorded = registered.get(new Key(beanName, handler.signature()));
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
            Set<HandlerMethod> handlers = recorded == null ? new HashSet<>() : new HashSet<>(recorded);
            handlers.add(handler);
            return Set.copyOf(handlers);
        });
        return recordedNow.get();
    }

    /**
     * Forgets {@code handler} of {@code beanName}, so a later scan registers it again.
     */
    public void forgetRegistered(String beanName, HandlerMethod handler) {
        registered.computeIfPresent(new Key(beanName, handler.signature()), (key, recorded) -> {
            Set<HandlerMethod> handlers = new HashSet<>(recorded);
            handlers.remove(handler);
            return handlers.isEmpty() ? null : Set.copyOf(handlers);
        });
    }

    private static boolean registers(Set<HandlerMethod> recorded, HandlerMethod handler) {
        return recorded.stream().noneMatch(other -> HandlerScan.isSameHandler(other, handler));
    }

    private record Key(String beanName, String signature) {
    }
}
