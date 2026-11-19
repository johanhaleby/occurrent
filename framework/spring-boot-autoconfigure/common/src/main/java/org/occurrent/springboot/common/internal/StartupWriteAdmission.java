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
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Internal, not part of the public API. Refuses every write through the application service until the startup scan has
 * registered every handler, and refuses the creation of a bean whose synchronous handler would register after that.
 * <p>
 * Every write it admits reaches every synchronous handler the context ever registers, or the bean that declares that
 * handler fails its creation. The starters start no asynchronous subscription before the gate opens, so nothing the
 * startup scan starts can write while it runs. Until the gate opens, the starter takes every bean that declares a
 * synchronous handler that isn't registered yet and registers it before opening. Once the gate is open, such a bean
 * fails its creation before its {@code @PostConstruct} method runs. A bean whose initialization began before the gate
 * opened fails once it is initialized instead, and so does a bean that another post-processor replaced with an object
 * that declares such a handler. A write such a bean made during its initialization may already be stored by then,
 * and the handler won't get it once the bean is fixed either, since it has no replay.
 * <p>
 * A registration that another thread starts while the gate is closed keeps it closed until it ends, since a handler
 * counts as registered from the moment it is claimed, before it subscribes.
 * <p>
 * Whether a handler is registered is read again once the gate is open, so another instance of a prototype whose
 * handler registered at startup is built as usual.
 */
public final class StartupWriteAdmission {

    private volatile boolean open;
    private final Object lock = new Object();
    // Registrations counted by registrationStarts that haven't ended, read and changed only while holding lock
    private int registering;

    /**
     * @return {@code false} until the gate opens, then {@code true} for good
     */
    public boolean admitsWrite() {
        return open;
    }

    /**
     * Called before the initialization of a bean that declares a synchronous handler that isn't registered yet.
     *
     * @param unregisteredHandler The bean's first synchronous handler that isn't registered, or null, asked only once
     *                            the gate is open
     * @throws IllegalStateException when the gate is open and the bean still has an unregistered synchronous handler,
     *                               which fails the bean's creation
     */
    public void refuseIfOpen(String beanName, Supplier<Method> unregisteredHandler) {
        if (open) {
            Method handler = unregisteredHandler.get();
            if (handler != null) {
                throw refusal(beanName, handler);
            }
        }
    }

    /**
     * Called before a thread other than the startup thread registers a bean's handlers.
     *
     * @return Whether the registration is counted, in which case the gate stays closed until {@link #registrationEnds()}
     */
    public boolean registrationStarts() {
        synchronized (lock) {
            if (open) {
                return false;
            }
            registering++;
            return true;
        }
    }

    /**
     * Called once a registration that {@link #registrationStarts()} counted has ended, whether or not it succeeded.
     */
    public void registrationEnds() {
        synchronized (lock) {
            registering--;
            lock.notifyAll();
        }
    }

    /**
     * Runs {@code handOver} unless the gate is open, and keeps the gate from opening until it returns.
     *
     * @return Whether {@code handOver} ran
     */
    public boolean runWhileClosed(Runnable handOver) {
        synchronized (lock) {
            if (open) {
                return false;
            }
            handOver.run();
            return true;
        }
    }

    /**
     * Waits until every counted registration has ended, then opens the gate when {@code nothingLeft} holds, read while
     * nothing can run through {@link #runWhileClosed}. Every write is admitted from then on.
     *
     * @return Whether the gate is open
     * @throws IllegalStateException when the thread is interrupted while it waits
     */
    public boolean openIfNothingLeft(BooleanSupplier nothingLeft) {
        synchronized (lock) {
            while (registering > 0) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for another thread to finish registering a handler before writes are admitted", e);
                }
            }
            if (!nothingLeft.getAsBoolean()) {
                return false;
            }
            open = true;
            return true;
        }
    }

    /**
     * Opens the gate, and every write is admitted from then on.
     */
    public void open() {
        openIfNothingLeft(() -> true);
    }

    /**
     * The exception that fails the creation of a bean whose synchronous handler would register after the gate opened,
     * thrown before the bean's initialization runs.
     */
    public static IllegalStateException refusal(String beanName, Method handler) {
        return refusal(beanName, handler, "");
    }

    /**
     * The exception that fails the creation of a bean whose synchronous handler would register after the gate opened,
     * thrown once the bean's initialization has run. That happens when the gate opened while the bean was initialized, or
     * when another post-processor replaced the bean with an object that declares the handler.
     */
    public static IllegalStateException refusalAfterInitialization(String beanName, Method handler) {
        return refusal(beanName, handler, "The bean was refused after its initialization ran, so a write it made during initialization, for example " +
                                          "from its @PostConstruct method, may already be stored, and the handler won't get that write after the bean is " +
                                          "fixed either. ");
    }

    private static IllegalStateException refusal(String beanName, Method handler, String afterInitialization) {
        return new IllegalStateException(("Bean '%s' declares the synchronous handler %s#%s and is created after the startup scan of the Spring Boot " +
                                          "starter registered the synchronous handlers. %sA synchronous handler has no replay and gets only the writes " +
                                          "made after it registers, so it would miss the earlier ones. Let the startup scan register it: remove @Lazy " +
                                          "from the bean, or declare its @Bean method with the annotated class as the return type rather than an " +
                                          "interface or Object. Or make the handler asynchronous, with @Subscription instead of " +
                                          "@SynchronousSubscription, or mode = ASYNC on @Projection and @Snapshot.")
                .formatted(beanName, handler.getDeclaringClass().getName(), handler.getName(), afterInitialization));
    }
}
