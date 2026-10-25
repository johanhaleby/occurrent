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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.ConsumerRecoveryListener;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Keeps the consumer tag of {@link RabbitMqCloudEventSource} and {@code RabbitMqDomainEventSource} true across an
 * automatic connection recovery. Public only because the two sources live in different packages. Not part of the
 * supported API.
 * <p>
 * A recovered connection registers the source's consumer on the recovered channel again only when topology recovery
 * is turned on. The client then reports the tag the consumer ended up with. With topology recovery turned off nothing
 * registers it, and the tag the source holds names a consumer that no longer exists. The source's poll starts a
 * consumer only while it holds no tag, so it would never consume again. This clears the tag in that case, and the
 * poll registers a new consumer on its next tick.
 * <p>
 * The client calls a connection's {@link RecoveryListener#handleRecoveryStarted(Recoverable)}, then recovers its
 * channels, then its topology, and only then calls {@link RecoveryListener#handleRecovery(Recoverable)}. The tag is
 * therefore judged on the last of those. A channel's own recovery listener runs before the topology is recovered and
 * could not tell the two cases apart.
 */
public final class RabbitMqConsumerRecovery {

    private final AtomicReference<@Nullable String> consumerTag;
    private final String queue;
    private final Logger log;
    private final Object lock = new Object();
    private boolean detached;
    private @Nullable Runnable removal;

    /**
     * @param consumerTag The source's consumer tag. Written here only by compare-and-set, since the source's poll
     *                    writes it from its own thread while a recovery runs, and a consumer the poll registered
     *                    after the recovery started must not be cleared.
     * @param queue       The queue the source consumes from, only used in log messages.
     * @param log         The source's own logger.
     */
    public RabbitMqConsumerRecovery(AtomicReference<@Nullable String> consumerTag, String queue, Logger log) {
        this.consumerTag = requireNonNull(consumerTag, "consumerTag cannot be null");
        this.queue = requireNonNull(queue, "queue cannot be null");
        this.log = requireNonNull(log, "log cannot be null");
    }

    /**
     * Starts following the recoveries of {@code connection}. Does nothing unless it is the client's own
     * {@link AutorecoveringConnection}, since no other connection recovers by itself, and nothing after
     * {@link #detach()}. It has to be the connection the source was built with. The one a channel reports through
     * {@link Channel#getConnection()} is the plain connection underneath, which never calls a recovery listener.
     */
    public void attachTo(Connection connection) {
        requireNonNull(connection, "connection cannot be null");
        if (!(connection instanceof AutorecoveringConnection recovering)) {
            return;
        }
        synchronized (lock) {
            if (detached || removal != null) {
                return;
            }
            Registration created = new Registration();
            recovering.addRecoveryListener(created);
            recovering.addConsumerRecoveryListener(created);
            removal = () -> {
                recovering.removeRecoveryListener(created);
                recovering.removeConsumerRecoveryListener(created);
            };
        }
    }

    /**
     * Stops following recoveries. Safe to call more than once, and before {@link #attachTo(Connection)}.
     */
    public void detach() {
        Runnable toRun;
        synchronized (lock) {
            detached = true;
            toRun = removal;
            removal = null;
        }
        if (toRun != null) {
            toRun.run();
        }
    }

    private final class Registration implements RecoveryListener, ConsumerRecoveryListener {
        // The tag the source held when the recovery started, and whether the client re-registered it since.
        private volatile @Nullable String tagAtStart;
        private volatile boolean reRegistered;

        @Override
        public void handleRecoveryStarted(Recoverable recoverable) {
            reRegistered = false;
            tagAtStart = consumerTag.get();
        }

        @Override
        public void consumerRecovered(String oldConsumerTag, String newConsumerTag) {
            // The listener is connection wide, so this is also called for every other consumer on the connection.
            if (consumerTag.compareAndSet(oldConsumerTag, newConsumerTag)) {
                reRegistered = true;
            }
        }

        @Override
        public void handleRecovery(Recoverable recoverable) {
            String tag = tagAtStart;
            tagAtStart = null;
            if (tag == null || reRegistered) {
                return;
            }
            if (consumerTag.compareAndSet(tag, null)) {
                log.info("The connection under queue \"{}\" recovered without registering consumer \"{}\" again, which is "
                        + "what turning topology recovery off means. This source registers a new consumer on its next poll.",
                        queue, tag);
            }
        }
    }
}
