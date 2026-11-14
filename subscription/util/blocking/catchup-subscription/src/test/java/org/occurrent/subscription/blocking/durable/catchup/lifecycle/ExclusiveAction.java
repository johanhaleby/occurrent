package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.Kind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Wraps the subscriber's action and watches S1 (never two actions in flight for one id) and S2 (a subscribe's
 * generation delivers nothing after a cancel of its id, or a pause of it or a stop, has returned).
 * Only a generation whose subscribe returned before that call began is bound by it. A pause or a stop binds until an
 * enabling call, a resume of the id that returned ok or a start(true) that returned or failed after its effect, ends after the
 * pause or the stop began. One still in flight counts, since it can take effect at any instant.
 */
final class ExclusiveAction {

    static final class Generation {
        final String id;
        final int number;
        final List<String> received = new CopyOnWriteArrayList<>();
        boolean subscribedOk;
        long returnedAt;
        boolean cancelled;
        int inAction;

        Generation(String id, int number) {
            this.id = id;
            this.number = number;
        }

        @Override
        public String toString() {
            return id + "#" + number;
        }
    }

    /** A pause of one id or a stop of all that returned ok, from when it began to when it returned */
    private record Barrier(@Nullable String id, long beganAt, long returnedAt) {
    }

    /** A resume of one id or a start(true) of all. It stays enabling until it ends without having resumed anything */
    private static final class Enabling {
        final @Nullable String id;
        final long beganAt;
        long endedAt = Long.MAX_VALUE;
        boolean resumed = true;

        Enabling(@Nullable String id, long beganAt) {
            this.id = id;
            this.beganAt = beganAt;
        }
    }

    private final Object lock = new Object();
    private final Set<String> ids;
    private final List<Generation> generations = new ArrayList<>();
    private final Map<String, Integer> inActionPerId = new HashMap<>();
    private final List<Barrier> barriers = new ArrayList<>();
    private final List<Enabling> enablings = new ArrayList<>();
    private final ThreadLocal<Enabling> enablingOfThisThread = new ThreadLocal<>();
    private long clock;
    private final ThreadLocal<Long> beganAt = ThreadLocal.withInitial(() -> 0L);
    private static final long ACTION_NANOS = 20_000L;
    final List<String> s1Violations = new CopyOnWriteArrayList<>();
    final List<String> s2Violations = new CopyOnWriteArrayList<>();

    ExclusiveAction(Set<String> ids) {
        this.ids = ids;
    }

    Generation newGeneration(String id) {
        synchronized (lock) {
            Generation generation = new Generation(id, generations.size());
            generations.add(generation);
            return generation;
        }
    }

    Consumer<CloudEvent> actionOf(Generation generation) {
        return event -> {
            enter(generation);
            try {
                generation.received.add(event.getId());
                LockSupport.parkNanos(ACTION_NANOS);
            } finally {
                exit(generation);
            }
        };
    }

    private void enter(Generation generation) {
        synchronized (lock) {
            int now = inActionPerId.merge(generation.id, 1, Integer::sum);
            generation.inAction++;
            if (now > 1) {
                s1Violations.add("two actions in flight for " + generation.id + " (entered by " + generation + ")");
            }
            if (generation.cancelled) {
                s2Violations.add(generation + " started an action after its cancel returned");
            } else if (barredByAPauseOrAStop(generation)) {
                s2Violations.add(generation + " started an action after a pause or a stop returned");
            }
        }
    }

    private void exit(Generation generation) {
        synchronized (lock) {
            inActionPerId.merge(generation.id, -1, Integer::sum);
            generation.inAction--;
        }
    }

    boolean idle() {
        synchronized (lock) {
            return inActionPerId.values().stream().allMatch(count -> count == 0);
        }
    }

    void begin(LifecycleCall call) {
        synchronized (lock) {
            beganAt.set(++clock);
            if (call.kind() == Kind.RESUME || call.kind() == Kind.START_TRUE) {
                Enabling enabling = new Enabling(call.kind() == Kind.RESUME ? call.id() : null, clock);
                enablings.add(enabling);
                enablingOfThisThread.set(enabling);
            }
        }
    }

    void end(LifecycleCall call, @Nullable Generation made, boolean returned) {
        synchronized (lock) {
            clock++;
            switch (call.kind()) {
                case RESUME -> finish(enablingOfThisThread.get(), returned);
                case START_TRUE -> finish(enablingOfThisThread.get(), returned || call.failure() == LifecycleCall.StartFailure.AFTER_EFFECT);
                case SUBSCRIBE -> {
                    if (returned && made != null) {
                        made.subscribedOk = true;
                        made.returnedAt = clock;
                    }
                }
                case CANCEL -> {
                    if (returned) {
                        generations.stream().filter(g -> g.id.equals(call.id()) && returnedBefore(g)).forEach(g -> g.cancelled = true);
                    }
                }
                case PAUSE -> {
                    if (returned) {
                        barriers.add(new Barrier(call.id(), beganAt.get(), clock));
                    }
                }
                case STOP -> {
                    if (returned) {
                        barriers.add(new Barrier(null, beganAt.get(), clock));
                    }
                }
                default -> {
                }
            }
        }
    }

    private void finish(@Nullable Enabling enabling, boolean resumed) {
        if (enabling != null) {
            enabling.endedAt = clock;
            enabling.resumed = resumed;
            enablingOfThisThread.remove();
        }
    }

    private boolean returnedBefore(Generation g) {
        return g.subscribedOk && g.returnedAt < beganAt.get();
    }

    private boolean barredByAPauseOrAStop(Generation g) {
        return barriers.stream().anyMatch(barrier -> (barrier.id() == null || barrier.id().equals(g.id)) && g.subscribedOk && g.returnedAt < barrier.beganAt()
                && enablings.stream().noneMatch(enabling -> (enabling.id == null || enabling.id.equals(g.id)) && enabling.resumed && enabling.endedAt >= barrier.beganAt()));
    }

    @Nullable Generation currentGeneration(String id) {
        synchronized (lock) {
            return generations.stream().filter(g -> g.id.equals(id) && g.subscribedOk && !g.cancelled).max((a, b) -> Long.compare(a.returnedAt, b.returnedAt)).orElse(null);
        }
    }
}
