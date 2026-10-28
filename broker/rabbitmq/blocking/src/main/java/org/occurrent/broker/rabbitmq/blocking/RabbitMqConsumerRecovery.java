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
import com.rabbitmq.client.ChannelContinuationTimeoutException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.ConsumerRecoveryListener;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Registers and cancels the consumer of {@link RabbitMqCloudEventSource} and {@code RabbitMqDomainEventSource}, and
 * keeps the source's consumer tag true across an automatic connection recovery. Public only because the two sources
 * live in different packages. Not part of the supported API.
 * <p>
 * A recovered connection registers the source's consumer on the recovered channel again only when topology recovery
 * is turned on. The client then reports the tag the consumer ended up with. With topology recovery turned off nothing
 * registers it, and the tag the source holds names a consumer that no longer exists. The source's poll starts a
 * consumer only while it holds no tag, so it would never consume again. This clears the tag in that case, and on its
 * next tick the poll cancels the old consumer, which the client still keeps a record of, and registers a new one.
 * <p>
 * The client calls a connection's {@link RecoveryListener#handleRecoveryStarted(Recoverable)}, then recovers its
 * channels, then its topology, and only then calls {@link RecoveryListener#handleRecovery(Recoverable)}. The tag is
 * therefore judged on the last of those. A channel's own recovery listener runs before the topology is recovered and
 * could not tell the two cases apart.
 * <p>
 * Topology recovery registers every consumer the client had a record of when the recovery started, including one the
 * source cancels while the recovery runs. So {@link #consume(Consume)} and {@link #cancelRetired(Channel)} do nothing
 * while a recovery runs, and a consumer the source no longer wants stays retired until a cancel that no recovery
 * overlapped has removed it.
 */
public final class RabbitMqConsumerRecovery {

    private final AtomicReference<@Nullable String> consumerTag;
    private final String queue;
    private final Logger log;
    private final Object lock = new Object();
    private boolean detached;
    private @Nullable Runnable removal;
    // Raised when a recovery starts and again when it ends, so it is odd while one runs, and a change across a call to
    // the channel means a recovery overlapped that call. Under lock.
    private long recoveryCount;
    // Consumers the source no longer wants and has not yet seen cancelled. Under lock.
    private final Set<String> retired = new LinkedHashSet<>();

    /**
     * @param consumerTag The source's consumer tag. The source clears it on its own when it stops, so the recovery
     *                    callbacks write it only by compare-and-set.
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
            // Both removals run, also when the first throws.
            removal = () -> new RabbitMqTeardown(null)
                    .run(() -> recovering.removeRecoveryListener(created))
                    .run(() -> recovering.removeConsumerRecoveryListener(created))
                    .finish();
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

    /**
     * Registers the source's consumer by calling {@code basicConsume}, and holds the tag it returns. Does nothing while
     * a recovery runs. When a recovery starts before {@code basicConsume} returns, the client may have recorded that
     * consumer for the recovery to register again, so the source does not hold its tag. The consumer is retired
     * instead, and the next {@link #cancelRetired(Channel)} cancels it.
     */
    public void consume(Consume basicConsume) throws IOException {
        requireNonNull(basicConsume, "basicConsume cannot be null");
        long before;
        synchronized (lock) {
            if (isRecovering()) {
                return;
            }
            before = recoveryCount;
        }
        String tag = basicConsume.consume();
        synchronized (lock) {
            if (recoveryCount == before) {
                consumerTag.set(tag);
                return;
            }
            retired.add(tag);
        }
        log.debug("A connection recovery under queue \"{}\" started while consumer \"{}\" was being registered. This "
                + "source cancels that consumer and registers a new one on its next polls.", queue, tag);
    }

    /**
     * Stops holding {@code tag}, if the source holds it, and keeps it for {@link #cancelRetired(Channel)} to cancel.
     */
    public void retire(String tag) {
        requireNonNull(tag, "tag cannot be null");
        synchronized (lock) {
            consumerTag.compareAndSet(tag, null);
            retired.add(tag);
        }
    }

    /**
     * Cancels every retired consumer on {@code channel}. Does nothing while a recovery runs.
     * <p>
     * A consumer is forgotten once a cancel that no recovery overlapped returns, or fails with a
     * {@link ShutdownSignalException} or with an {@link IOException} other than a
     * {@link ChannelContinuationTimeoutException}. In each of those the client no longer has the consumer, or the
     * channel it was on is closing. After any other failure the consumer may still be registered, and the next call
     * cancels it again.
     *
     * @return {@code true} when no retired consumer is left.
     * @throws IOException The failure of a cancel after which the consumer may still be registered.
     */
    public boolean cancelRetired(Channel channel) throws IOException {
        requireNonNull(channel, "channel cannot be null");
        List<String> toCancel;
        long before;
        synchronized (lock) {
            if (isRecovering()) {
                return false;
            }
            before = recoveryCount;
            toCancel = new ArrayList<>(retired);
        }
        for (String tag : toCancel) {
            Throwable failure = null;
            try {
                channel.basicCancel(tag);
            } catch (Throwable e) {
                failure = e;
            }
            boolean gone = failure == null || failure instanceof ShutdownSignalException
                    || (failure instanceof IOException && !(failure instanceof ChannelContinuationTimeoutException));
            boolean forgotten;
            synchronized (lock) {
                forgotten = gone && recoveryCount == before;
                if (forgotten) {
                    retired.remove(tag);
                }
            }
            if (!forgotten) {
                if (failure == null) {
                    return false;
                }
                throw asIoException(failure);
            }
            if (failure != null) {
                log.debug("Cancelling consumer \"{}\" on queue \"{}\" failed, and the client no longer has it.", tag, queue, failure);
            }
        }
        synchronized (lock) {
            return retired.isEmpty();
        }
    }

    // Under lock
    private boolean isRecovering() {
        return recoveryCount % 2 != 0;
    }

    private static IOException asIoException(Throwable failure) {
        if (failure instanceof IOException ioException) {
            return ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        // A checked exception javac does not know of, which a TrafficListener written in Kotlin can throw
        return new IOException(failure);
    }

    /**
     * The {@code basicConsume} call {@link #consume(Consume)} makes.
     */
    @FunctionalInterface
    public interface Consume {
        String consume() throws IOException;
    }

    private final class Registration implements RecoveryListener, ConsumerRecoveryListener {
        // The tag the source held when the recovery started, and whether the client re-registered it since. Under lock.
        private @Nullable String tagAtStart;
        private boolean reRegistered;

        @Override
        public void handleRecoveryStarted(Recoverable recoverable) {
            synchronized (lock) {
                if (!isRecovering()) {
                    recoveryCount++;
                }
                reRegistered = false;
                tagAtStart = consumerTag.get();
            }
        }

        @Override
        public void consumerRecovered(String oldConsumerTag, String newConsumerTag) {
            // The listener is connection wide, so this is also called for every other consumer on the connection.
            synchronized (lock) {
                if (consumerTag.compareAndSet(oldConsumerTag, newConsumerTag)) {
                    reRegistered = true;
                } else if (retired.remove(oldConsumerTag)) {
                    // Retired while this recovery ran, and registered again by it
                    retired.add(newConsumerTag);
                }
            }
        }

        @Override
        public void handleRecovery(Recoverable recoverable) {
            String cleared = null;
            synchronized (lock) {
                if (isRecovering()) {
                    recoveryCount++;
                }
                String tag = tagAtStart;
                tagAtStart = null;
                if (tag != null && !reRegistered && consumerTag.compareAndSet(tag, null)) {
                    // The client keeps its record of the consumer, and the callbacks with it, until a cancel removes it
                    retired.add(tag);
                    cleared = tag;
                }
            }
            if (cleared != null) {
                log.info("The connection under queue \"{}\" recovered without registering consumer \"{}\" again, which is "
                        + "what turning topology recovery off means. This source registers a new consumer on its next poll.",
                        queue, cleared);
            }
        }
    }
}
