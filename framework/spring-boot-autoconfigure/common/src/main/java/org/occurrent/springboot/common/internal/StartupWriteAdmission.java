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

import java.lang.reflect.Method;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Internal, not part of the public API. Refuses every write through the application service until the startup scan has
 * registered every handler, and refuses the creation of a bean whose synchronous handler would register after that.
 * <p>
 * Every write it admits reaches every synchronous handler the context ever registers, or the bean that declares that
 * handler fails its creation. The starters start no asynchronous subscription before {@link #open()}, so nothing the
 * startup scan starts can write while it runs. A bean that would register a new synchronous handler once the gate is
 * open fails its creation, before its {@code @PostConstruct} method runs, or, when its creation began before the gate
 * opened, once it is initialized.
 * <p>
 * A bean counts here only when it declares a synchronous handler that isn't registered yet, so another instance of a
 * prototype whose handler registered at startup is built as usual.
 */
public final class StartupWriteAdmission implements StartupGap {

    private volatile boolean open;
    // The synchronous beans in creation, read and changed only while holding its own lock
    private final Map<Object, Refusable> inCreation = new IdentityHashMap<>();

    @Override
    public boolean admitsWrite() {
        return open;
    }

    /**
     * Called before the initialization of a bean that declares a synchronous handler that isn't registered yet.
     *
     * @throws IllegalStateException when the gate is open, which fails the bean's creation
     */
    public void synchronousBeanCreationStarts(Object bean, String beanName, Method handler) {
        synchronized (inCreation) {
            if (open) {
                throw refusal(beanName, handler);
            }
            inCreation.put(bean, new Refusable(beanName, handler));
        }
    }

    /**
     * Called once a bean passed to {@link #synchronousBeanCreationStarts} is initialized. Takes any object, so a caller
     * can pass both the instance it got and the target a proxy wraps.
     *
     * @param handlerUnregistered Whether the bean's synchronous handler is still unregistered
     * @throws IllegalStateException when the gate opened while the bean was created and its handler is still
     *                               unregistered, which fails the bean's creation
     */
    public void synchronousBeanCreationEnds(Object bean, BooleanSupplier handlerUnregistered) {
        Refusable started;
        synchronized (inCreation) {
            started = inCreation.remove(bean);
        }
        if (started != null && open && handlerUnregistered.getAsBoolean()) {
            throw refusal(started.beanName(), started.handler());
        }
    }

    /**
     * Called when a {@code FactoryBean}'s product that declares a synchronous handler that isn't registered yet has been
     * built, since the product is first seen then.
     *
     * @throws IllegalStateException when the gate is open, which fails the product's creation
     */
    public void synchronousProductBuilt(String beanName, Method handler) {
        if (open) {
            throw refusal(beanName, handler);
        }
    }

    /**
     * Called once the startup scan has registered every handler, and every write is admitted from then on.
     */
    public void open() {
        synchronized (inCreation) {
            open = true;
        }
    }

    private static IllegalStateException refusal(String beanName, Method handler) {
        return new IllegalStateException(("Bean '%s' declares the synchronous handler %s#%s and is created after the startup scan of the Spring Boot " +
                                          "starter registered the synchronous handlers. A synchronous handler has no replay and gets only the writes " +
                                          "made after it registers, so it would miss the earlier ones. Let the startup scan register it: remove @Lazy " +
                                          "from the bean, or declare its @Bean method with the annotated class as the return type rather than an " +
                                          "interface or Object. Or make the handler asynchronous, with @Subscription instead of " +
                                          "@SynchronousSubscription, or mode = ASYNC on @Projection and @Snapshot.")
                .formatted(beanName, handler.getDeclaringClass().getName(), handler.getName()));
    }

    private record Refusable(String beanName, Method handler) {
    }
}
