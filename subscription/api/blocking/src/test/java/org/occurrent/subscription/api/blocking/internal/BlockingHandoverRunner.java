package org.occurrent.subscription.api.blocking.internal;

import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.api.blocking.internal.BlockingHandoverSpec.CatchUp;
import org.occurrent.subscription.api.blocking.internal.BlockingHandoverSpec.Offer;
import org.occurrent.subscription.api.blocking.internal.BlockingHandoverSpec.Trace;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs one script against a handover on a scheduler's terms. The replay thread moves only when a step says so, the
 * accept calls run on helper threads, and the script then runs to its end and everything is asked to finish.
 */
final class BlockingHandoverRunner {

    static final List<String> HISTORY = List.of("h1", "h2");

    enum Action {
        CATCH_UP, STEP, CANCEL, STOP, ACCEPT, ACCEPT_REPORTING, ACCEPT_IF_LIVE, ACCEPT_COPY_OF_HISTORY, FAIL_FOLD, FAIL_CALLBACK, INTERRUPT,
        /** The next payload a drain delivers parks inside deliver, so a payload offered next is offered while that drain runs. Released when the script ends. */
        HOLD_DRAIN,
        /** A catch-up of a source that is already caught up. It drains what the buffer holds at once, on a thread of its own, and starts even while another catch-up runs */
        CATCH_UP_CAUGHT_UP,
        /** The next payload a catch-up delivers outside the history feeds a further payload to the handover from inside the fold, which buffers it for a later round */
        FEED_FROM_HANDLER
    }

    record Script(boolean startsLive, List<Action> actions) {
        @Override
        public String toString() {
            return (startsLive ? "live " : "fresh ") + actions;
        }
    }

    private BlockingHandoverRunner() {
    }

    /**
     * offeredAt is when the offer was answered. A reported payload that is lost is excused by a failure that fired
     * after it in a catch-up that ended after it, up to and including the first of those, in start order, that did
     * not return false. A stopped catch-up leaves the buffer to the next one, but the stop path can drain it too.
     */
    private record Helper(Thread thread, String payload, String call, AtomicReference<String> answer, AtomicLong offeredAt, AtomicLong startedAt, List<String> deliveredWhenStarted) {
    }

    /** gated is false for a catch-up of a caught-up source, which has no gate for a step to open */
    private record Replay(ControllableSource source, Thread thread, AtomicReference<String> result, boolean gated) {
    }

    static Trace run(Script script) {
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean failNextFold = new AtomicBoolean();
        List<String> notes = new CopyOnWriteArrayList<>();
        List<String> markerFindings = new CopyOnWriteArrayList<>();
        AtomicLong clock = new AtomicLong();
        List<ControllableSource> catchUps = new CopyOnWriteArrayList<>();
        List<String> overlaps = new CopyOnWriteArrayList<>();
        AtomicInteger catchUpsDelivering = new AtomicInteger();
        AtomicBoolean holdNextDrainDelivery = new AtomicBoolean();
        AtomicBoolean feedFromNextFold = new AtomicBoolean();
        AtomicInteger feeds = new AtomicInteger();
        // The thread parked in the held drain delivery, so a step does not wait for it to reach a gate
        AtomicReference<Thread> heldThread = new AtomicReference<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        Semaphore drainHold = new Semaphore(0);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            boolean ofACatchUp = catchUps.stream().anyMatch(source -> source.catchUpThread == Thread.currentThread());
            // Counted from the first thing the delivery does, so a drain held inside it still counts as delivering
            if (ofACatchUp && catchUpsDelivering.incrementAndGet() > 1) {
                overlaps.add("two catch-ups were delivering at once, the second delivering " + payload);
            }
            if (ofACatchUp && !HISTORY.contains(payload) && holdNextDrainDelivery.compareAndSet(true, false)) {
                catchUps.stream().filter(source -> source.catchUpThread == Thread.currentThread()).forEach(source -> source.toScheduler.release());
                heldThread.set(Thread.currentThread());
                try {
                    if (!drainHold.tryAcquire(5, TimeUnit.SECONDS)) {
                        notes.add("the held drain was not released within 5 seconds");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    heldThread.set(null);
                }
            }
            if (ofACatchUp && !HISTORY.contains(payload) && feedFromNextFold.compareAndSet(true, false)) {
                try {
                    self.get().acceptReportingDelivery("f" + feeds.getAndIncrement());
                } catch (RuntimeException refused) {
                    // A catch-up that has failed refuses it, and that is the caller's to see, not this fold's.
                }
            }
            try {
                if (failNextFold.getAndSet(false)) {
                    catchUps.stream().filter(source -> source.catchUpThread == Thread.currentThread()).forEach(ControllableSource::failureFired);
                    throw new IllegalStateException("fold failed for " + payload);
                }
                delivered.add(payload);
                if (ofACatchUp) {
                    Thread.yield();
                }
            } finally {
                if (ofACatchUp) {
                    catchUpsDelivering.decrementAndGet();
                }
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), "thing");
        self.set(handover);
        if (script.startsLive()) {
            ControllableSource alreadyCaughtUp = new ControllableSource(List.of(), new AtomicBoolean(true), clock);
            alreadyCaughtUp.openAllGates();
            handover.catchUp(alreadyCaughtUp);
        }
        AtomicBoolean marker = new AtomicBoolean();
        List<Replay> replays = new ArrayList<>();
        List<Helper> helpers = new ArrayList<>();
        boolean failCallback = false;
        int next = 0;
        for (Action action : script.actions()) {
            Replay current = replays.isEmpty() ? null : replays.get(replays.size() - 1);
            boolean replaying = current != null && current.thread().isAlive();
            long running = replays.stream().filter(replay -> replay.thread().isAlive()).count();
            // The catch-up a step or an interrupt is for, the oldest one that still runs and has a gate to open
            Replay gated = replays.stream().filter(replay -> replay.thread().isAlive() && replay.gated()).findFirst().orElse(null);
            switch (action) {
                case CATCH_UP, CATCH_UP_CAUGHT_UP -> {
                    // A second catch-up starts while one runs, which is the only way two of them are ever there at once
                    if (running < 2) {
                        boolean caughtUp = action == Action.CATCH_UP_CAUGHT_UP;
                        ControllableSource source = caughtUp ? new ControllableSource(List.of(), new AtomicBoolean(true), clock) : new ControllableSource(HISTORY, marker, clock);
                        if (caughtUp) {
                            source.openAllGates();
                        }
                        source.failCallback = failCallback;
                        AtomicReference<String> result = new AtomicReference<>();
                        Thread thread = Thread.ofPlatform().name("handover-replay").unstarted(() -> {
                            result.set(runCatchUp(handover, source, notes, catchUps));
                        });
                        catchUps.add(source);
                        replays.add(new Replay(source, thread, result, !caughtUp));
                        thread.start();
                        if (running == 0 && !caughtUp) {
                            source.awaitParkedOrDone(thread);
                        } else {
                            settle(thread);
                        }
                    }
                }
                case STEP -> {
                    // A catch-up parked in the held drain has no gate to open until the hold is released
                    if (gated != null && heldThread.get() != gated.thread()) {
                        gated.source().step();
                        awaitParked(gated, running > 1);
                    }
                }
                case CANCEL -> {
                    if (current != null) {
                        current.source().cancelled = true;
                        current.source().cancelledBeforeItReturned |= replaying;
                    }
                }
                case STOP -> {
                    try {
                        handover.stopIfNotCatchingUp();
                    } catch (Throwable t) {
                        notes.add("stopIfNotCatchingUp threw " + t);
                    }
                }
                case FAIL_FOLD -> failNextFold.set(true);
                case HOLD_DRAIN -> holdNextDrainDelivery.set(true);
                case FEED_FROM_HANDLER -> feedFromNextFold.set(true);
                case FAIL_CALLBACK -> {
                    failCallback = true;
                    replays.forEach(replay -> replay.source().failCallback = true);
                }
                case INTERRUPT -> {
                    if (gated != null) {
                        boolean held = heldThread.get() == gated.thread();
                        gated.thread().interrupt();
                        awaitParked(gated, running > 1 || held);
                    }
                }
                default -> {
                    String payload = action == Action.ACCEPT_COPY_OF_HISTORY ? HISTORY.get(0) : "p" + next++;
                    String call = action == Action.ACCEPT_REPORTING ? "acceptReportingDelivery" : action == Action.ACCEPT_IF_LIVE ? "acceptIfLive" : "accept";
                    AtomicReference<String> answer = new AtomicReference<>();
                    AtomicLong offeredAt = new AtomicLong(Long.MAX_VALUE);
                    AtomicLong startedAt = new AtomicLong(Long.MAX_VALUE);
                    startedAt.set(clock.incrementAndGet());
                    List<String> deliveredWhenStarted = List.copyOf(delivered);
                    Thread thread = Thread.ofPlatform().name("handover-accept-" + payload).unstarted(() -> {
                        answer.set(offer(handover, call, payload));
                        offeredAt.set(clock.incrementAndGet());
                    });
                    helpers.add(new Helper(thread, payload, call, answer, offeredAt, startedAt, deliveredWhenStarted));
                    thread.start();
                    settle(thread);
                }
            }
        }
        List<String> replayed = new ArrayList<>();
        drainHold.release(1_000);
        finish(replays, helpers, handover, marker, notes, markerFindings, replayed, clock, catchUps);
        List<Offer> offers = helpers.stream().map(helper -> new Offer(helper.payload(), helper.call(), helper.answer().get(), failureFiredAfter(helper.offeredAt().get(), catchUps), helper.startedAt().get(), helper.deliveredWhenStarted())).toList();
        List<CatchUp> started = replays.stream().map(replay -> new CatchUp(String.valueOf(replay.result().get()), replay.source().cancelledBeforeItReturned)).toList();
        List<Long> drainStarts = catchUps.stream().flatMap(source -> source.drainStartedAt.stream()).toList();
        return new Trace(HISTORY, List.copyOf(delivered), offers, List.copyOf(markerFindings), List.copyOf(notes), replayed, started, drainStarts, List.copyOf(overlaps));
    }

    /**
     * A catch-up that throws is expected only when an injected failure fired in its own replay or drain, or in another
     * catch-up of the run, which leaves the handover refusing every catch-up after it
     */
    private static String runCatchUp(BlockingHandover<String, String> handover, ControllableSource source, List<String> notes, List<ControllableSource> catchUps) {
        source.catchUpThread = Thread.currentThread();
        source.startedAt = source.clock.incrementAndGet();
        try {
            source.outcome = String.valueOf(handover.catchUp(source));
        } catch (Throwable t) {
            source.outcome = "threw " + t.getClass().getSimpleName();
            if (catchUps.stream().allMatch(other -> other.failuresFiredAt.isEmpty())) {
                notes.add("a catch-up threw although no failure was injected into the run: " + t);
            }
        } finally {
            source.endedAt = source.clock.incrementAndGet();
        }
        return source.outcome;
    }

    private static boolean failureFiredAfter(long offeredAt, List<ControllableSource> catchUps) {
        List<ControllableSource> endedAfter = catchUps.stream()
                .filter(source -> source.endedAt > offeredAt)
                .sorted(Comparator.comparingLong(source -> source.startedAt))
                .toList();
        for (ControllableSource source : endedAfter) {
            if (source.failuresFiredAt.stream().anyMatch(firedAt -> firedAt > offeredAt)) {
                return true;
            }
            if (!"false".equals(source.outcome)) {
                return false;
            }
        }
        return false;
    }

    private static String offer(BlockingHandover<String, String> handover, String call, String payload) {
        try {
            switch (call) {
                case "accept" -> {
                    handover.accept(payload);
                    return "applied";
                }
                case "acceptReportingDelivery" -> {
                    return "reported:" + handover.acceptReportingDelivery(payload);
                }
                default -> {
                    return "reported:" + handover.acceptIfLive(payload);
                }
            }
        } catch (Throwable t) {
            return "refused:" + t.getClass().getSimpleName();
        }
    }

    /**
     * Waits until the catch-up is parked at a gate or has ended. When another catch-up runs too, this one may be waiting
     * for it instead of parked, so it also stops waiting once it has stayed blocked for a while.
     */
    private static void awaitParked(Replay replay, boolean maybeWaitingForAnother) {
        if (!maybeWaitingForAnother) {
            replay.source().awaitParkedOrDone(replay.thread());
            return;
        }
        int blocked = 0;
        long until = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < until && blocked < 10) {
            try {
                if (replay.source().toScheduler.tryAcquire(1, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            Thread.State state = replay.thread().getState();
            if (state == Thread.State.TERMINATED) {
                return;
            }
            blocked = state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING || state == Thread.State.BLOCKED ? blocked + 1 : 0;
        }
    }

    /** Waits until the thread has returned or has stayed blocked for a while. */
    private static void settle(Thread thread) {
        int blocked = 0;
        long until = System.nanoTime() + 2_000_000_000L;
        while (System.nanoTime() < until && blocked < 5) {
            Thread.State state = thread.getState();
            if (state == Thread.State.TERMINATED) {
                return;
            }
            blocked = state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING || state == Thread.State.BLOCKED ? blocked + 1 : 0;
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void finish(List<Replay> replays, List<Helper> helpers, BlockingHandover<String, String> handover, AtomicBoolean marker, List<String> notes, List<String> markerFindings, List<String> replayed, AtomicLong clock, List<ControllableSource> catchUps) {
        for (Replay replay : replays) {
            replay.source().openAllGates();
            join(replay.thread(), 5_000);
            if (replay.thread().isAlive()) {
                notes.add("a replay never returned");
                replay.thread().interrupt();
            }
            replayed.addAll(replay.source().yielded);
            String result = replay.result().get();
            if (replay.source().markedWhileCancelled) {
                markerFindings.add("the marker was written after the replay was cancelled");
            }
            if ("false".equals(result) && replay.source().markerWritten) {
                markerFindings.add("the marker was written by a catch-up that returned false");
            }
        }
        try {
            handover.stopIfNotCatchingUp();
        } catch (Throwable t) {
            notes.add("stopIfNotCatchingUp threw " + t);
        }
        for (Helper helper : helpers) {
            join(helper.thread(), 3_000);
            if (helper.thread().isAlive()) {
                helper.thread().interrupt();
                join(helper.thread(), 1_000);
            }
        }
        ControllableSource clean = new ControllableSource(HISTORY, marker, clock);
        clean.openAllGates();
        catchUps.add(clean);
        Thread later = Thread.ofPlatform().name("handover-later-catch-up").start(() -> runCatchUp(handover, clean, notes, catchUps));
        join(later, 5_000);
        if (later.isAlive()) {
            notes.add("a later catch-up never returned, so the replay turn was not released");
            later.interrupt();
        } else if (!"true".equals(clean.outcome) && !"false".equals(clean.outcome) && catchUps.stream().allMatch(other -> other.failuresFiredAt.isEmpty())) {
            // A fold failure that the script armed and nothing consumed can still fire in this catch-up, and then it may throw
            notes.add("the later catch-up did not return, it gave " + clean.outcome);
        }
        replayed.addAll(clean.yielded);
    }

    private static void join(Thread thread, long millis) {
        try {
            thread.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
