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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * The transient-versus-permanent classification {@link RabbitMqCloudEventBridge.Builder#retryStrategy(org.occurrent.retry.RetryStrategy)}
 * and {@link org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventBridge.Builder#retryStrategy(org.occurrent.retry.RetryStrategy)}
 * default to, and the Spring Boot starter's {@code occurrent.broker.rabbitmq.bridge.retry.*} wiring reapplies
 * whenever it replaces that default with property-driven timing. One shared method rather than the same
 * {@code instanceof} check written out at each of those four call sites, so a broadened or narrowed classification
 * never drifts between them.
 * <p>
 * A {@link RabbitMqBridgeException} or a {@link RabbitMqPublishException} is only sometimes transient.
 * {@code build()} opens two channels under {@link org.occurrent.broker.api.blocking.DeliveryFailurePolicy#PARK},
 * the consume channel through {@link RabbitMqBridgeException} and the parking publisher's confirm channel through
 * {@link RabbitMqPublishException} ({@link RabbitMqConfirmPublisher}'s own {@code openConfirmChannel}), and a
 * broker briefly unreachable can fail either one. Both wrap one of the same two underlying causes, an
 * {@link java.io.IOException} with no further detail (a socket failure, say, "the broker is not reachable right
 * now") or a {@link ShutdownSignalException} (unchecked, thrown when the connection is itself already closed or
 * mid recovery), and both of those are transient.
 * <p>
 * A {@link ShutdownSignalException} carrying an AMQP hard-close reply is not, regardless of which of the two
 * wrapper types carries it. {@code NOT_FOUND} (404, a queue bound to an exchange that does not exist),
 * {@code PRECONDITION_FAILED} (406, a queue redeclared with different arguments than an existing one),
 * {@code ACCESS_REFUSED} (403, a rejected credential or an insufficient permission), {@code INVALID_PATH} (402, a
 * nonexistent vhost) and {@code NOT_ALLOWED} (530) are the broker's own answer that this specific request is wrong,
 * not that it is temporarily unreachable, and every one of those five is byte-for-byte identical on every retried
 * attempt. Retrying one anyway is exactly the operator error a retry loop can hide, so this refuses those five
 * outright and retries everything else, including a hard close this classification does not specifically
 * recognise, since an unrecognised one is far more likely to be transient than a silent new permanent failure mode.
 * <p>
 * {@link #isTransient(Throwable)} returns for any cause chain that eventually revisits a throwable it has already
 * seen, including one built to loop back on itself through {@link Throwable#initCause(Throwable)}. This is called
 * from inside a retry loop, so a classification that never returns hangs {@code build()} with it, worse than
 * either answer it could have given instead. {@code Throwable.getCause()} is not {@code final}, so an override
 * that hands back a distinct object on every call defeats this the same way it defeats
 * {@link Throwable#printStackTrace()} itself, which walks a cause chain the identical way for the identical
 * reason. Every {@code getCause()} this module or its dependencies actually construct returns a stable object.
 */
public final class RabbitMqBuildFailureClassifier {

    private RabbitMqBuildFailureClassifier() {
    }

    public static boolean isTransient(Throwable throwable) {
        if (!(throwable instanceof RabbitMqBridgeException) && !(throwable instanceof RabbitMqPublishException)
                && !(throwable instanceof ShutdownSignalException)) {
            return false;
        }
        ShutdownSignalException shutdown = shutdownSignalIn(throwable);
        return shutdown == null || isTransientReplyCode(shutdown.getReason());
    }

    // A cause chain built through Throwable.initCause(...) can loop back on itself (a genuine cycle, not just a
    // repeated equal-looking exception), and Throwable itself allows that, so the walk has to notice it has
    // already visited a throwable before following its cause again, the same guard java.lang.Throwable's own
    // printStackTrace() uses for the same reason. identity, not equals(), since two distinct exceptions with the
    // same message are not the same node.
    private static ShutdownSignalException shutdownSignalIn(Throwable throwable) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = throwable; current != null && visited.add(current); current = current.getCause()) {
            if (current instanceof ShutdownSignalException shutdownSignalException) {
                return shutdownSignalException;
            }
        }
        return null;
    }

    private static boolean isTransientReplyCode(Method reason) {
        int replyCode;
        if (reason instanceof AMQP.Connection.Close close) {
            replyCode = close.getReplyCode();
        } else if (reason instanceof AMQP.Channel.Close close) {
            replyCode = close.getReplyCode();
        } else {
            return true;
        }
        return replyCode != AMQP.NOT_FOUND && replyCode != AMQP.PRECONDITION_FAILED
                && replyCode != AMQP.ACCESS_REFUSED && replyCode != AMQP.INVALID_PATH
                && replyCode != AMQP.NOT_ALLOWED;
    }
}
