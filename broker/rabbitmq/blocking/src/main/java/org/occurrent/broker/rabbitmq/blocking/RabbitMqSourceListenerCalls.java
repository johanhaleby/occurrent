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

package org.occurrent.broker.rabbitmq.blocking;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Calls a {@link RabbitMqSourceListener} for one queue, logging and ignoring whatever it throws, so a broken listener
 * never fails a delivery. What {@link RabbitMqCloudEventSource}, {@code RabbitMqDomainEventSource} and
 * {@link RabbitMqDeliveryFailureAction} share. Public only because the two sources live in different packages. Not
 * part of the supported API.
 */
public final class RabbitMqSourceListenerCalls {

    private static final RabbitMqSourceListener NO_OP = new RabbitMqSourceListener() {
    };

    private final RabbitMqSourceListener listener;
    private final String queue;
    private final Logger log;

    /**
     * @param listener The listener to call.
     * @param queue    The queue every call reports.
     * @param log      Where a listener that throws is logged.
     */
    public RabbitMqSourceListenerCalls(RabbitMqSourceListener listener, String queue, Logger log) {
        this.listener = requireNonNull(listener, RabbitMqSourceListener.class.getSimpleName() + " cannot be null");
        this.queue = requireNonNull(queue, "queue cannot be null");
        this.log = requireNonNull(log, "log cannot be null");
    }

    /**
     * Calls a listener that does nothing.
     */
    public static RabbitMqSourceListenerCalls none() {
        return new RabbitMqSourceListenerCalls(NO_OP, "", LoggerFactory.getLogger(RabbitMqSourceListenerCalls.class));
    }

    /**
     * Calls {@link RabbitMqSourceListener#delivered(String)}.
     */
    public void delivered() {
        call("delivered", () -> listener.delivered(queue));
    }

    /**
     * Calls {@link RabbitMqSourceListener#failed(String, Throwable)}.
     */
    public void failed(@Nullable Throwable failure) {
        call("failed", () -> listener.failed(queue, failure));
    }

    /**
     * Calls {@link RabbitMqSourceListener#parked(String)}.
     */
    public void parked() {
        call("parked", () -> listener.parked(queue));
    }

    /**
     * Calls {@link RabbitMqSourceListener#redelivered(String)}.
     */
    public void redelivered() {
        call("redelivered", () -> listener.redelivered(queue));
    }

    // Throwable, since a listener written in Kotlin can throw a checked exception through the Java interface, and a
    // listener that cannot link against its metrics library throws a LinkageError, neither of which may stop a source.
    // A VirtualMachineError is rethrown, since nothing keeps running after one
    private void call(String callback, Runnable call) {
        try {
            call.run();
        } catch (VirtualMachineError e) {
            throw e;
        } catch (Throwable e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("The {} listener threw from {}(..) for queue \"{}\". Ignoring it.",
                    listener.getClass().getName(), callback, queue, e);
        }
    }
}
