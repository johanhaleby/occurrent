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
 * broker briefly unreachable can fail either one. Both usually wrap one of the same two underlying causes, an
 * {@link java.io.IOException} with no further detail (a socket failure, say, "the broker is not reachable right
 * now") or a {@link ShutdownSignalException} (unchecked, thrown when the connection is itself already closed or
 * mid recovery), and both of those are transient. Both can also carry no cause at all. {@code openChannel()}
 * returning empty (every channel number on the {@code Connection} already in use) throws directly rather than
 * wrapping anything, and this classification treats that the same way, transient, since a channel slot freeing up
 * is exactly the kind of thing a retry can wait out.
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
 * A {@link ShutdownSignalException#isInitiatedByApplication()} shutdown is not transient regardless of its reply
 * code, or even absent one. It means this module's own {@code Connection} or {@code Channel} was closed
 * deliberately, and RabbitMQ's automatic connection recovery, the mechanism that would otherwise make a retried
 * attempt on the same {@code Connection} eventually succeed, never reopens something the client itself chose to
 * close. Retrying it anyway spends the whole backoff window watching an attempt that cannot ever succeed on its
 * own, so this refuses it outright the same way it refuses the five explicitly recognised hard closes above.
 * <p>
 * {@link #isTransient(Throwable)} returns for every cause chain a caller can construct, including one that loops
 * back on itself through {@link Throwable#initCause(Throwable)} and one whose {@code getCause()} never repeats an
 * object, since a caller-supplied override is not bound to the second case either. This is called from inside a
 * retry loop, so a classification that never returns hangs {@code build()} with it, worse than either answer it
 * could have given instead. The walk tracks visited identity, the guard {@link Throwable#printStackTrace()} itself
 * uses against a repeating cause, and additionally bounds itself to {@value #MAX_CAUSE_CHAIN_DEPTH} hops, since
 * identity alone cannot catch a chain that never repeats a node at all; a {@code getCause()} override under a
 * caller's control is not required to.
 */
public final class RabbitMqBuildFailureClassifier {

    // Real cause chains this module or its dependencies ever build are a handful of nodes deep, so this is headroom
    // rather than a limit anything legitimate could hit. It exists solely to bound a getCause() override that
    // hands back a fresh, never-repeated object on every call, which the identity-based visited set below cannot
    // detect, since termination there depends on eventually seeing an object twice.
    private static final int MAX_CAUSE_CHAIN_DEPTH = 10_000;

    private RabbitMqBuildFailureClassifier() {
    }

    public static boolean isTransient(Throwable throwable) {
        if (!(throwable instanceof RabbitMqBridgeException) && !(throwable instanceof RabbitMqPublishException)
                && !(throwable instanceof ShutdownSignalException)) {
            return false;
        }
        ShutdownSignalException shutdown = shutdownSignalIn(throwable);
        return shutdown == null || (!shutdown.isInitiatedByApplication() && isTransientReplyCode(shutdown.getReason()));
    }

    // A cause chain built through Throwable.initCause(...) can loop back on itself (a genuine cycle, not just a
    // repeated equal-looking exception), and Throwable itself allows that, so the walk has to notice it has
    // already visited a throwable before following its cause again, the same guard java.lang.Throwable's own
    // printStackTrace() uses for the same reason. identity, not equals(), since two distinct exceptions with the
    // same message are not the same node. That catches a repeating node, but Throwable.getCause() is not final, so
    // a caller-supplied override can hand back a distinct object on every call, an unbounded chain that never
    // repeats rather than a cycle; the hop count below is what stops the walk on that chain instead.
    private static ShutdownSignalException shutdownSignalIn(Throwable throwable) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = throwable;
        for (int hops = 0; current != null && hops < MAX_CAUSE_CHAIN_DEPTH; hops++) {
            if (!visited.add(current)) {
                return null;
            }
            if (current instanceof ShutdownSignalException shutdownSignalException) {
                return shutdownSignalException;
            }
            current = current.getCause();
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
