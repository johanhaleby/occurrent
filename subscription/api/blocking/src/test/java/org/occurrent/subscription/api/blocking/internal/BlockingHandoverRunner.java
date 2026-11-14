package org.occurrent.subscription.api.blocking.internal;

import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.api.blocking.internal.BlockingHandoverSpec.Offer;
import org.occurrent.subscription.api.blocking.internal.BlockingHandoverSpec.Trace;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs one script against a handover on a scheduler's terms. The replay thread moves only when a step says so, the
 * accept calls run on helper threads, and the script then runs to its end and everything is asked to finish.
 */
final class BlockingHandoverRunner {

    static final List<String> HISTORY = List.of("h1", "h2");

    enum Action {
        CATCH_UP, STEP, CANCEL, STOP, ACCEPT, ACCEPT_REPORTING, ACCEPT_IF_LIVE, ACCEPT_COPY_OF_HISTORY, FAIL_FOLD, FAIL_CALLBACK, INTERRUPT
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
     * offeredAt is when the offer was answered. The buffer outlives a stopped catch-up (BlockingHandover.Source#replayAbandoned),
     * so the payload belongs to the first catch-up, in start order, that ends after that moment without having
     * returned false (a stop): the replay open at the offer unless it is stopped, otherwise the next one to run, the
     * finish() catch-up included. A reported payload lost to a failure is excused only by a failure that fired in that catch-up after
     * offeredAt, found by where it fired and not by where its action sits in the script.
     */
    private record Helper(Thread thread, String payload, String call, AtomicReference<String> answer, AtomicLong offeredAt) {
    }

    private record Replay(ControllableSource source, Thread thread, AtomicReference<String> result) {
    }

    static Trace run(Script script) {
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean failNextFold = new AtomicBoolean();
        List<String> notes = new CopyOnWriteArrayList<>();
        List<String> markerFindings = new CopyOnWriteArrayList<>();
        AtomicLong clock = new AtomicLong();
        List<ControllableSource> catchUps = new CopyOnWriteArrayList<>();
        ThreadLocal<ControllableSource> catchUpOfThread = new ThreadLocal<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (failNextFold.getAndSet(false)) {
                ControllableSource inCatchUp = catchUpOfThread.get();
                if (inCatchUp != null) {
                    inCatchUp.failuresFiredAt.add(clock.incrementAndGet());
                }
                throw new IllegalStateException("fold failed for " + payload);
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), "thing");
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
            switch (action) {
                case CATCH_UP -> {
                    if (!replaying) {
                        ControllableSource source = new ControllableSource(HISTORY, marker, clock);
                        source.failCallback = failCallback;
                        AtomicReference<String> result = new AtomicReference<>();
                        Thread thread = Thread.ofPlatform().name("handover-replay").unstarted(() -> {
                            result.set(runCatchUp(handover, source, catchUpOfThread));
                        });
                        catchUps.add(source);
                        replays.add(new Replay(source, thread, result));
                        thread.start();
                        source.awaitParkedOrDone(thread);
                    }
                }
                case STEP -> {
                    if (replaying) {
                        current.source().step();
                        current.source().awaitParkedOrDone(current.thread());
                    }
                }
                case CANCEL -> {
                    if (current != null) {
                        current.source().cancelled = true;
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
                case FAIL_CALLBACK -> {
                    failCallback = true;
                    replays.forEach(replay -> replay.source().failCallback = true);
                }
                case INTERRUPT -> {
                    if (replaying) {
                        current.thread().interrupt();
                        current.source().awaitParkedOrDone(current.thread());
                    }
                }
                default -> {
                    String payload = action == Action.ACCEPT_COPY_OF_HISTORY ? HISTORY.get(0) : "p" + next++;
                    String call = action == Action.ACCEPT_REPORTING ? "acceptReportingDelivery" : action == Action.ACCEPT_IF_LIVE ? "acceptIfLive" : "accept";
                    AtomicReference<String> answer = new AtomicReference<>();
                    AtomicLong offeredAt = new AtomicLong(Long.MAX_VALUE);
                    Thread thread = Thread.ofPlatform().name("handover-accept-" + payload).unstarted(() -> {
                        answer.set(offer(handover, call, payload));
                        offeredAt.set(clock.incrementAndGet());
                    });
                    helpers.add(new Helper(thread, payload, call, answer, offeredAt));
                    thread.start();
                    settle(thread);
                }
            }
        }
        List<String> replayed = new ArrayList<>();
        finish(replays, helpers, handover, marker, notes, markerFindings, replayed, clock, catchUps, catchUpOfThread);
        List<Offer> offers = helpers.stream().map(helper -> new Offer(helper.payload(), helper.call(), helper.answer().get(), failureFiredAfter(helper.offeredAt().get(), catchUps))).toList();
        return new Trace(HISTORY, List.copyOf(delivered), offers, List.copyOf(markerFindings), List.copyOf(notes), replayed);
    }

    private static String runCatchUp(BlockingHandover<String, String> handover, ControllableSource source, ThreadLocal<ControllableSource> catchUpOfThread) {
        source.catchUpThread = Thread.currentThread();
        catchUpOfThread.set(source);
        source.startedAt = source.clock.incrementAndGet();
        try {
            source.outcome = String.valueOf(handover.catchUp(source));
        } catch (Throwable t) {
            source.outcome = "threw " + t.getClass().getSimpleName();
        } finally {
            source.endedAt = source.clock.incrementAndGet();
            catchUpOfThread.remove();
        }
        return source.outcome;
    }

    private static boolean failureFiredAfter(long offeredAt, List<ControllableSource> catchUps) {
        return catchUps.stream()
                .sorted(java.util.Comparator.comparingLong(source -> source.startedAt))
                .filter(source -> source.endedAt > offeredAt && !"false".equals(source.outcome))
                .findFirst()
                .map(owner -> owner.failuresFiredAt.stream().anyMatch(firedAt -> firedAt > offeredAt))
                .orElse(false);
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

    private static void finish(List<Replay> replays, List<Helper> helpers, BlockingHandover<String, String> handover, AtomicBoolean marker, List<String> notes, List<String> markerFindings, List<String> replayed, AtomicLong clock, List<ControllableSource> catchUps, ThreadLocal<ControllableSource> catchUpOfThread) {
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
        Thread later = Thread.ofPlatform().name("handover-later-catch-up").start(() -> runCatchUp(handover, clean, catchUpOfThread));
        join(later, 5_000);
        if (later.isAlive()) {
            notes.add("a later catch-up never returned, so the replay turn was not released");
            later.interrupt();
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
