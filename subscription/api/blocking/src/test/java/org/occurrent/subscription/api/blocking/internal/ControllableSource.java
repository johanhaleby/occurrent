package org.occurrent.subscription.api.blocking.internal;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A source whose replay thread parks at the gates the scheduler steps it through, before every history read and
 * before the drain. The marker store is shared between the sources of one run.
 */
final class ControllableSource implements BlockingHandover.Source<String> {

    private final List<String> history;
    private final AtomicBoolean marker;
    private final Semaphore toReplay = new Semaphore(0);
    final Semaphore toScheduler = new Semaphore(0);
    private volatile boolean open;
    /** What the replay yielded, so a pass that read nothing counts for nothing */
    final List<String> yielded = new CopyOnWriteArrayList<>();
    volatile boolean cancelled;
    /** Set when the script cancelled this source while its catch-up had not returned */
    volatile boolean cancelledBeforeItReturned;
    volatile boolean failCallback;
    volatile boolean markedWhileCancelled;
    volatile boolean markerWritten;

    final AtomicLong clock;
    /** When the catch-up that read this source started and ended, on the run's clock */
    volatile long startedAt = Long.MAX_VALUE;
    volatile long endedAt = Long.MAX_VALUE;
    volatile Thread catchUpThread;
    /** When the drain of this catch-up began, taken once the source is told the history is done, so after the drain was claimed */
    final List<Long> drainStartedAt = new CopyOnWriteArrayList<>();
    /** What the catch-up returned or "threw ...", null while it has not ended */
    volatile String outcome;
    /** When an injected failure fired in this catch-up's own replay or drain: the fold threw, the callback threw or an interrupt landed at a gate. A callback that throws is not one, it changes only the answer of its own payload */
    final List<Long> failuresFiredAt = new CopyOnWriteArrayList<>();

    ControllableSource(List<String> history, AtomicBoolean marker, AtomicLong clock) {
        this.history = history;
        this.marker = marker;
        this.clock = clock;
    }

    void failureFired() {
        failuresFiredAt.add(clock.incrementAndGet());
    }

    void step() {
        toReplay.release();
    }

    void openAllGates() {
        open = true;
        toReplay.release(1_000);
    }

    private void gate() {
        if (open) {
            return;
        }
        toScheduler.release();
        try {
            toReplay.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failureFired();
            throw new IllegalStateException("interrupted at a gate", e);
        }
    }

    @Override
    public boolean isAlreadyCaughtUp() {
        return marker.get();
    }

    @Override
    public Stream<String> replay() {
        Iterator<String> it = history.iterator();
        Iterator<String> gated = new Iterator<>() {
            @Override
            public boolean hasNext() {
                gate();
                return it.hasNext();
            }

            @Override
            public String next() {
                if (!it.hasNext()) {
                    throw new NoSuchElementException();
                }
                String item = it.next();
                yielded.add(item);
                return item;
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(gated, 0), false);
    }

    @Override
    public void historyDone() {
        drainStartedAt.add(clock.incrementAndGet());
        gate();
    }

    @Override
    public boolean keepReplaying() {
        return !cancelled;
    }

    @Override
    public void markCaughtUp() {
        markedWhileCancelled = cancelled;
        markerWritten = true;
        marker.set(true);
    }

    @Override
    public void alreadyDeliveredByReplay(String payload) {
        if (failCallback) {
            throw new IllegalStateException("recording callback failed for " + payload);
        }
    }

    boolean awaitParkedOrDone(Thread replayThread) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < until) {
            try {
                if (toScheduler.tryAcquire(1, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (!replayThread.isAlive()) {
                return true;
            }
        }
        return false;
    }
}
