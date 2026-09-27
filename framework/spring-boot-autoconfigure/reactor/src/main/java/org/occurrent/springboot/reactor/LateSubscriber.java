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

package org.occurrent.springboot.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Runs the subscribe call of an event-store registration. A subscription model may block inside subscribe, and
// ReactorDurableSubscriptionModel does, reading the stored position for a DEFAULT start with block(). A bean built after
// startup is built on whichever thread asked for it, and on a Reactor non-blocking thread that block() throws. There
// the call runs on boundedElastic instead and the bean is returned without waiting for it. A failure then has no
// caller left to reach, so it is logged and the registration's claims are released. On any other thread the call
// runs where it is, and a failure fails the bean as before.
final class LateSubscriber {
    private static final Logger log = LoggerFactory.getLogger(LateSubscriber.class);
    // The same bound ProjectionAnnotationRegistrar.close() waits for its background catch-ups.
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final Scheduler scheduler;
    // A subscribe running on the scheduler holds the read lock and close() takes the write lock, so once close()
    // returns no subscribe is running and none starts afterwards. Every one that ran finished before the subscription
    // model shut down, and the model stops it along with the others when it does.
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean closed;

    LateSubscriber() {
        this(Schedulers.boundedElastic());
    }

    // For a test that decides when a subscribe moved off the caller's thread runs.
    LateSubscriber(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    void subscribe(String registration, Runnable subscribe, Runnable releaseOnFailure) {
        if (!Schedulers.isInNonBlockingThread()) {
            subscribe.run();
            return;
        }
        String callerThread = Thread.currentThread().getName();
        scheduler.schedule(() -> subscribeUnlessClosed(registration, callerThread, subscribe, releaseOnFailure));
    }

    private void subscribeUnlessClosed(String registration, String callerThread, Runnable subscribe, Runnable releaseOnFailure) {
        lock.readLock().lock();
        try {
            if (closed) {
                log.debug("Did not subscribe {}, since the application context started closing first.", registration);
                return;
            }
            subscribe.run();
        } catch (RuntimeException | Error e) {
            log.error("Could not subscribe {}. Its bean was built on the Reactor non-blocking thread {}, so the subscribe ran "
                    + "afterwards on {} and the failure had no caller to reach. It receives no events.", registration, callerThread,
                    Thread.currentThread().getName(), e);
            releaseOnFailure.run();
        } finally {
            lock.readLock().unlock();
        }
    }

    // Called once the application context starts closing, before any bean is destroyed. The subscription model shuts
    // down while beans are destroyed, and a subscribe reaching it after that could leave a subscription running.
    // It waits at most CLOSE_TIMEOUT, so a subscribe stuck reading its position cannot hold the shutdown open. One
    // still running after that can finish after the model shut down, and then it is up to the model to refuse it.
    void close() {
        boolean locked = false;
        try {
            locked = lock.writeLock().tryLock(CLOSE_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closed = true;
        if (locked) {
            lock.writeLock().unlock();
        }
    }
}
