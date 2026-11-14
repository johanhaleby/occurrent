package org.occurrent.subscription.api.blocking.internal;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    volatile boolean cancelled;
    volatile boolean failCallback;
    volatile boolean markedWhileCancelled;
    volatile boolean markerWritten;

    ControllableSource(List<String> history, AtomicBoolean marker) {
        this.history = history;
        this.marker = marker;
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
                return it.next();
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(gated, 0), false);
    }

    @Override
    public void historyDone() {
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
