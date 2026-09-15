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
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import com.rabbitmq.client.impl.recovery.RecoveryAwareChannelN;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
 * One thread per bridge keeps deliveries handled one at a time, in the order the broker sent them. The broker sends
 * no more than the bridge's {@code prefetchCount} unacknowledged deliveries to one consumer, the bridge starts a new
 * consumer only once {@link #isIdle()}, and once an automatic connection recovery starts,
 * {@link #discardOnRecovery(Channel)} makes sure no delivery from the channel that died is started. So the queue in
 * front of the thread holds about {@code prefetchCount} deliveries from the current channel, however many pauses or
 * recoveries happen while a handler is blocked. Only about, since {@link #isIdle()} counts what this worker has been
 * handed, and a delivery the client has taken but not yet handed over is not counted when a consumer starts.
 * <p>
 * A delivery this worker never starts, because {@link #stopAcceptingWork()} or {@link #stop(Duration)} ran first or a
 * recovery dropped it, is left unacknowledged. RabbitMQ puts every unacknowledged delivery on a closed channel back on
 * the queue, and the bridge closes its channel right after either call, so nothing is lost.
 */
public final class RabbitMqDeliveryWorker {

    private final String queue;
    private final Logger log;
    private final ThreadPoolExecutor executor;
    private final AtomicInteger unfinishedDeliveries = new AtomicInteger();
    private final AtomicLong discardUpToDeliveryTag = new AtomicLong();
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
        this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), runnable -> {
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
     * A delivery from a channel a recovery has already replaced is dropped here rather than queued, and so is one
     * arriving after {@link #stopAcceptingWork()}. Both stay unacknowledged, and the channel the bridge closes next
     * puts them back on the queue.
     *
     * @param deliveryTag The delivery {@code work} handles.
     * @param work        Handles the delivery, including acknowledging it, and deals with its own failures. Anything
     *                    escaping it ends this worker's thread, which the executor then replaces.
     */
    public void submit(long deliveryTag, Runnable work) {
        if (deliveryTag <= discardUpToDeliveryTag.get()) {
            // A callback from a channel a recovery has already replaced. Dropped here rather than queued, so one that
            // arrives while a handler is blocked does not wait in memory until that handler finishes.
            log.debug("Dropping delivery tag {} on queue \"{}\", it came from a channel that has been replaced. " +
                    "RabbitMQ delivers it again on the recovered channel.", deliveryTag, queue);
            return;
        }
        unfinishedDeliveries.incrementAndGet();
        DeliveryTask task = new DeliveryTask(deliveryTag, work);
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            unfinishedDeliveries.decrementAndGet();
            // Throwing back into the client would make its exception handler close the channel.
            log.debug("Delivery tag {} on queue \"{}\" arrived after the bridge stopped. It stays unacknowledged, " +
                    "and closing the channel puts it back on the queue.", deliveryTag, queue);
            return;
        }
        // A recovery starting between the check above and the queueing would otherwise keep the task here, so take it
        // back. One already running is dropped by run() instead.
        if (deliveryTag <= discardUpToDeliveryTag.get() && executor.getQueue().remove(task)) {
            unfinishedDeliveries.decrementAndGet();
        }
    }

    /**
     * Makes sure that no delivery from a channel that died is started once an automatic recovery of {@code channel}
     * has begun. Does nothing for any other channel.
     * <p>
     * RabbitMQ has already put every unacknowledged delivery from the dead channel back on the queue, and the
     * recovered channel delivers each of them again. Without this a handler blocked across several recoveries would
     * find a copy of the same message waiting for it from every one of them.
     * <p>
     * The client calls this after it has created the replacement channel and before it registers the bridge's
     * consumer on it again. The replacement numbers its deliveries after every tag the dead channel issued, and that
     * last tag is read off the replacement here, so a callback from the dead channel that only reaches this worker
     * later is dropped too, while nothing from the replacement ever is. A channel that is not the client's own
     * {@code AutorecoveringChannel} over a {@code RecoveryAwareChannelN} gives no such guarantee about its tags, so
     * for it nothing is dropped and a blocked handler may see the same message again after a recovery.
     */
    public void discardOnRecovery(Channel channel) {
        if (channel instanceof Recoverable recoverable) {
            recoverable.addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                    if (channel instanceof AutorecoveringChannel recovering && recovering.getDelegate() instanceof RecoveryAwareChannelN replacement) {
                        discardUpTo(replacement.getActiveDeliveryTagOffset());
                    }
                }

                @Override
                public void handleRecovery(Recoverable recoverable) {
                }
            });
        }
    }

    private void discardUpTo(long deliveryTag) {
        discardUpToDeliveryTag.accumulateAndGet(deliveryTag, Math::max);
        int discarded = 0;
        // Removing through the queue itself, since the worker may take a task between reading it and removing it.
        for (Runnable task : executor.getQueue().toArray(new Runnable[0])) {
            if (((DeliveryTask) task).deliveryTag <= deliveryTag && executor.getQueue().remove(task)) {
                discarded++;
            }
        }
        unfinishedDeliveries.addAndGet(-discarded);
        if (discarded > 0) {
            log.debug("Dropped the deliveries on queue \"{}\" still waiting for the worker when the connection " +
                    "dropped. RabbitMQ delivers them again on the recovered channel.", queue);
        }
    }

    private void run(long deliveryTag, Runnable work) {
        if (stopped || deliveryTag <= discardUpToDeliveryTag.get()) {
            return;
        }
        work.run();
    }

    /**
     * Whether every delivery submitted so far has finished or been dropped. RabbitMQ applies {@code prefetchCount} to
     * each consumer separately, so a bridge starts a new consumer only once this is true. Otherwise a consumer started
     * after a pause would add a second window of deliveries behind a handler that is still blocked.
     * <p>
     * This counts what has been submitted. A delivery the client has taken off the socket but not yet handed over is
     * not counted, so a consumer can start while such a delivery is still on its way here.
     */
    public boolean isIdle() {
        return unfinishedDeliveries.get() == 0;
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
     * Starts no queued delivery and waits up to {@code timeout} for the one already running to finish.
     *
     * @return {@code false} when a delivery was still running after {@code timeout}, {@code true} otherwise. Always
     * {@code true} when called from the worker thread, which returns at once since waiting there would wait for the
     * caller itself.
     */
    public boolean stop(Duration timeout) {
        stopAcceptingWork();
        if (isWorkerThread()) {
            return true;
        }
        try {
            return executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Interrupts a delivery still running after {@link #stop(Duration)} gave up waiting for it, and logs it at
     * {@code warn}. The bridge makes sure beforehand that nothing the handler does afterwards acknowledges the
     * delivery, so the channel close puts it back on the queue.
     */
    public void interruptRunningWork(Duration timeout) {
        log.warn("A handler on queue \"{}\" was still running {} after the bridge was asked to close. Closing anyway. " +
                "Its delivery is not acknowledged, so RabbitMQ delivers it again.", queue, timeout);
        executor.shutdownNow();
    }

    private final class DeliveryTask implements Runnable {
        private final long deliveryTag;
        private final Runnable work;

        private DeliveryTask(long deliveryTag, Runnable work) {
            this.deliveryTag = deliveryTag;
            this.work = work;
        }

        @Override
        public void run() {
            try {
                RabbitMqDeliveryWorker.this.run(deliveryTag, work);
            } finally {
                unfinishedDeliveries.decrementAndGet();
            }
        }
    }
}
