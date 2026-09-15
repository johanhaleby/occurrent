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

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * The one thread a consume-side bridge handles its deliveries on. Shared between {@link RabbitMqCloudEventBridge} and
 * {@code RabbitMqDomainEventBridge} rather than written twice.
 * <p>
 * The RabbitMQ client calls every consumer on a connection from one shared pool, sized by the number of available
 * processors unless the {@code ConnectionFactory} was given an executor of its own. A bridge that handled a delivery
 * on that pool's thread would keep it for as long as the handler ran, so a handler waiting on an unavailable database
 * would stop every other bridge on the same connection from receiving anything once the pool ran out of threads. A
 * bridge hands each delivery to one of these instead and returns at once, so the client's thread is free again
 * whatever the handler does.
 * <p>
 * One thread per bridge keeps deliveries handled one at a time, in the order the broker sent them. The queue in front
 * of it is never longer than the bridge's {@code prefetchCount}, since the broker sends no more than that many
 * unacknowledged deliveries to one consumer.
 * <p>
 * A delivery this worker never starts, because {@link #stopAcceptingWork()} or {@link #close(Duration)} ran first, is
 * left unacknowledged. The bridge closes its channel right after either call, and RabbitMQ puts every unacknowledged
 * delivery on a closed channel back on the queue, so nothing is lost.
 */
public final class RabbitMqDeliveryWorker {

    private final String queue;
    private final Logger log;
    private final ExecutorService executor;
    private volatile @Nullable Thread thread;
    private volatile boolean stopped;

    /**
     * @param threadName The name of the worker thread, so a thread dump shows which bridge a stuck handler belongs to.
     * @param queue      The queue the bridge consumes from, only used in log messages.
     * @param log        The bridge's own logger.
     */
    public RabbitMqDeliveryWorker(String threadName, String queue, Logger log) {
        requireNonNull(threadName, "threadName cannot be null");
        this.queue = requireNonNull(queue, "queue cannot be null");
        this.log = requireNonNull(log, "log cannot be null");
        this.executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread workerThread = new Thread(runnable, threadName);
            workerThread.setDaemon(true);
            thread = workerThread;
            return workerThread;
        });
    }

    /**
     * Queues {@code work} for the worker thread and returns without waiting for it. Called from the RabbitMQ client's
     * consumer callback.
     *
     * @param deliveryTag The delivery {@code work} handles, only used in log messages.
     * @param work        Handles the delivery, including acknowledging it.
     */
    public void submit(long deliveryTag, Runnable work) {
        try {
            executor.execute(() -> run(deliveryTag, work));
        } catch (RejectedExecutionException e) {
            // Throwing back into the client would make its exception handler close the channel.
            log.debug("Delivery tag {} on queue \"{}\" arrived after the bridge stopped. It stays unacknowledged, " +
                    "and closing the channel puts it back on the queue.", deliveryTag, queue);
        }
    }

    private void run(long deliveryTag, Runnable work) {
        if (stopped) {
            return;
        }
        try {
            work.run();
        } catch (RuntimeException e) {
            log.error("Handling delivery tag {} on queue \"{}\" failed outside the bridge's delivery failure policy, " +
                    "most likely while acknowledging it. The delivery stays unacknowledged until the channel closes.",
                    deliveryTag, queue, e);
        }
    }

    /**
     * Whether the calling thread is this worker's own thread.
     */
    public boolean isWorkerThread() {
        return Thread.currentThread() == thread;
    }

    /**
     * Lets a delivery already running finish, starts no queued one, and returns without waiting. Safe to call from
     * the worker thread itself, which is where a bridge that stops for good calls it from.
     */
    public void stopAcceptingWork() {
        stopped = true;
        executor.shutdown();
    }

    /**
     * Starts no queued delivery and waits up to {@code timeout} for the one already running to finish. A handler still
     * running after that is interrupted and logged at {@code warn}, since the bridge closes its channel next, which
     * puts that delivery back on the queue to be handled again. Returns at once when called from the worker thread,
     * since waiting there would wait for the caller itself.
     */
    public void close(Duration timeout) {
        stopAcceptingWork();
        if (isWorkerThread()) {
            return;
        }
        try {
            if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("A handler on queue \"{}\" was still running {} after the bridge was asked to close. Closing " +
                        "anyway. Its delivery was never acknowledged, so RabbitMQ delivers it again.", queue, timeout);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
