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

import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

// Runs the subscribe call of a registration for a bean built after startup. A subscription model may block inside
// subscribe, and ReactorDurableSubscriptionModel does, so on a Reactor non-blocking thread that block() throws.
//
// Where the subscription starts decides what happens there. A start that is the same whenever the subscribe runs,
// BEGINNING or an explicit position, is subscribed on the scheduler after the bean is returned, and tried again after
// a failure that can go away, so nothing is skipped however late it gets there. A start that can depend on when it
// runs, NOW or DEFAULT, is subscribed where it is, as on any other thread. Subscribing that one later could skip
// whatever the caller writes between getting the bean and the subscribe, so a model that blocks fails the bean
// instead, with a message saying how to register it.
final class LateSubscriber {
    private static final Logger log = LoggerFactory.getLogger(LateSubscriber.class);
    // The same bound ProjectionAnnotationRegistrar.close() waits for its background catch-ups.
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration FIRST_RETRY_DELAY = Duration.ofMillis(100);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(30);

    // The subscribe call of one registration. attempt can run again after it failed, so it has to pick up where the
    // failed one stopped.
    @FunctionalInterface
    interface SubscribeCall {
        void subscribe(Supplier<String> registration, boolean startIsFixed, Runnable attempt);
    }

    static final SubscribeCall INLINE = (registration, startIsFixed, attempt) -> attempt.run();

    private final Scheduler scheduler;
    private final Duration closeTimeout;
    private final Duration firstRetryDelay;
    // An attempt holds the read lock while it runs and close() takes the write lock, waiting at most closeTimeout. An
    // attempt still running when close() stops waiting can finish after close() returns, and none starts after that.
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Set<Late> waiting = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    LateSubscriber() {
        this(Schedulers.boundedElastic(), CLOSE_TIMEOUT, FIRST_RETRY_DELAY);
    }

    // For a test that decides when a subscribe moved off the caller's thread runs, how long close() waits, and how
    // long the first retry waits.
    LateSubscriber(Scheduler scheduler, Duration closeTimeout, Duration firstRetryDelay) {
        this.scheduler = scheduler;
        this.closeTimeout = closeTimeout;
        this.firstRetryDelay = firstRetryDelay;
    }

    static boolean startIsFixed(StartAt startAt) {
        return !startAt.isNow() && !startAt.isDefault();
    }

    static boolean startIsFixed(DcbStartAt startAt) {
        return startIsFixed(startAt.toStartAt());
    }

    // Whether a failed attempt is tried again, by what the failure says, following the split SubscriptionRefusedException
    // documents. An IllegalArgumentException, which every SubscriptionRefusedException is, a duplicate id and a refused
    // filter or start among them, says the call itself is wrong, and an UnsupportedOperationException says the model
    // cannot serve it at all. Neither is tried again, and nor is a NullPointerException or an Error, since the same
    // call fails the same way. Anything else is, an IllegalStateException, which says something went wrong at the time
    // or another node holds what the call needs, and whatever a storage or its driver throws, a Spring
    // DataAccessException or a MongoException, which this module cannot name. block() rethrows a checked exception or
    // an Error the JVM survives wrapped in a Reactor exception, which is looked through.
    static boolean retriable(Throwable failure) {
        Throwable unwrapped = Exceptions.unwrap(failure);
        return !(unwrapped instanceof Error
                 || unwrapped instanceof IllegalArgumentException
                 || unwrapped instanceof UnsupportedOperationException
                 || unwrapped instanceof NullPointerException);
    }

    // releaseOnGiveUp gives back what the registration claimed, once a subscribe moved to the scheduler stops trying.
    // A failure thrown to the caller is the caller's to release.
    SubscribeCall call(Runnable releaseOnGiveUp) {
        return (registration, startIsFixed, attempt) -> subscribe(registration, startIsFixed, attempt, releaseOnGiveUp);
    }

    private void subscribe(Supplier<String> registration, boolean startIsFixed, Runnable attempt, Runnable releaseOnGiveUp) {
        if (!Schedulers.isInNonBlockingThread()) {
            attempt.run();
            return;
        }
        String callerThread = Thread.currentThread().getName();
        if (!startIsFixed) {
            try {
                attempt.run();
            } catch (RuntimeException e) {
                if (refusedToBlock(e)) {
                    throw new IllegalStateException(("%s may start from wherever the event feed has reached when it subscribes (startAt = NOW, or DEFAULT, which does that when no position is stored for it), "
                            + "and its subscription model blocks while subscribing, which Reactor does not allow on the non-blocking thread %s that is building its bean. Subscribing it later on "
                            + "another thread could skip whatever is written between the bean being returned and that subscribe. Build the bean on a thread that may block, for example with "
                            + "Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic()), or start it at the beginning or at an explicit position, which does not depend on when it subscribes.")
                            .formatted(registration.get(), callerThread), e);
                }
                throw e;
            }
            return;
        }
        if (closed) {
            throw new IllegalStateException("Cannot subscribe %s, since the application context is closing.".formatted(registration.get()));
        }
        Late late = new Late(registration, callerThread, attempt, releaseOnGiveUp);
        waiting.add(late);
        try {
            late.task.replace(scheduler.schedule(() -> attempt(late, 1, firstRetryDelay)));
        } catch (RuntimeException e) {
            // Thrown to the caller, which releases the claims itself
            waiting.remove(late);
            throw e;
        }
    }

    private void attempt(Late late, int attemptNumber, Duration retryDelay) {
        lock.readLock().lock();
        try {
            if (!late.state.compareAndSet(State.WAITING, State.RUNNING)) {
                return;
            }
            if (closed) {
                late.giveUpBecauseClosing();
                return;
            }
            late.attempt.run();
            late.done();
            return;
        } catch (RuntimeException | Error e) {
            if (!retriable(e)) {
                log.error("Could not subscribe {}, and it is not tried again, since the failure says the subscribe itself is wrong rather than that something went wrong at the time. "
                          + "It receives no events, and what it claimed is given back, so building its bean again can register it once the cause is fixed. Its bean was built on the Reactor "
                          + "non-blocking thread {}, so the subscribe ran on {} with no caller to throw to.", late.registration.get(), late.callerThread, Thread.currentThread().getName(), e);
                late.giveUp();
                return;
            }
            if (closed) {
                log.error("Could not subscribe {} on attempt {}, and it is not tried again, since the application context is closing.", late.registration.get(), attemptNumber, e);
                late.giveUp();
                return;
            }
            log.error("Could not subscribe {} on attempt {}, trying again in {} ms. Its bean was built on the Reactor non-blocking thread {}, so the subscribe runs on {} with no caller to throw to, "
                      + "and it receives no events until an attempt succeeds.", late.registration.get(), attemptNumber, retryDelay.toMillis(), late.callerThread, Thread.currentThread().getName(), e);
            late.state.set(State.WAITING);
        } finally {
            lock.readLock().unlock();
        }
        Duration doubled = retryDelay.multipliedBy(2);
        Duration nextDelay = doubled.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : doubled;
        try {
            // Once close() disposed the task, replace disposes the new one as well, so a retry scheduled after close()
            // gave up on it never runs
            late.task.replace(scheduler.schedule(() -> attempt(late, attemptNumber + 1, nextDelay), retryDelay.toMillis(), TimeUnit.MILLISECONDS));
        } catch (RuntimeException e) {
            if (late.state.compareAndSet(State.WAITING, State.GAVE_UP)) {
                log.error("Gave up subscribing {}, since the scheduler refused the next attempt. It receives no events.", late.registration.get(), e);
                late.release();
            }
        }
    }

    // Reactor's own refusal of block() on a non-blocking thread. It has no type of its own, so it is recognised by its message.
    private static boolean refusedToBlock(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof IllegalStateException && t.getMessage() != null && t.getMessage().contains("blocking, which is not supported in thread")) {
                return true;
            }
        }
        return false;
    }

    // Called once the application context starts closing, before any bean is destroyed, and again when this post
    // processor is destroyed, which also covers a refresh that failed. The subscription model shuts down while beans
    // are destroyed, and a subscribe reaching it after that could leave a subscription running. closed is set first,
    // so an attempt that has not started yet never starts, and one waiting for its retry is cancelled and gives back
    // what it claimed here rather than when its delay runs out. It waits at most closeTimeout for an attempt that is
    // running, so a subscribe stuck reading its position cannot hold the shutdown open. One still running after that
    // can finish after the model shut down, and then it is up to the model to refuse it.
    void close() {
        closed = true;
        cancelWaiting();
        boolean locked = false;
        try {
            locked = lock.writeLock().tryLock(closeTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (locked) {
            lock.writeLock().unlock();
        } else {
            log.warn("Stopped waiting for a late subscribe after {} ms while the application context closes. If it finishes after the subscription model has shut down, it is up to the model to refuse it.", closeTimeout.toMillis());
        }
        // Again, for an attempt that was running during the first pass and failed after reading closed as false
        cancelWaiting();
    }

    private void cancelWaiting() {
        for (Late late : List.copyOf(waiting)) {
            if (late.state.compareAndSet(State.WAITING, State.GAVE_UP)) {
                late.task.dispose();
                late.warnClosing();
                late.release();
            }
        }
    }

    private enum State {WAITING, RUNNING, SUBSCRIBED, GAVE_UP}

    // One registration moved to the scheduler. state decides who ends it, so an attempt and close() never both do.
    private final class Late {
        private final Supplier<String> registration;
        private final String callerThread;
        private final Runnable attempt;
        private final Runnable releaseOnGiveUp;
        private final AtomicReference<State> state = new AtomicReference<>(State.WAITING);
        private final Disposable.Swap task = Disposables.swap();

        private Late(Supplier<String> registration, String callerThread, Runnable attempt, Runnable releaseOnGiveUp) {
            this.registration = registration;
            this.callerThread = callerThread;
            this.attempt = attempt;
            this.releaseOnGiveUp = releaseOnGiveUp;
        }

        private void done() {
            state.set(State.SUBSCRIBED);
            waiting.remove(this);
        }

        private void giveUpBecauseClosing() {
            warnClosing();
            giveUp();
        }

        private void giveUp() {
            state.set(State.GAVE_UP);
            release();
        }

        private void release() {
            waiting.remove(this);
            releaseOnGiveUp.run();
        }

        private void warnClosing() {
            log.warn("Did not subscribe {}, since the application context started closing first. Its start position does not depend on when it subscribes, so it skips nothing when the application next registers it.", registration.get());
        }
    }
}
