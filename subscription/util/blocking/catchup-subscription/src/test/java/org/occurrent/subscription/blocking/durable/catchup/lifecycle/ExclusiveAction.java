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
 * Only a generation whose subscribe returned before that call began is bound by it. A resume of the id or a
 * start(true) in flight disarms a pause or a stop, since either can take effect after it.
 */
final class ExclusiveAction {

    static final class Generation {
        final String id;
        final int number;
        final List<String> received = new CopyOnWriteArrayList<>();
        boolean subscribedOk;
        long returnedAt;
        boolean cancelled;
        boolean armed;
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

    private final Object lock = new Object();
    private final Set<String> ids;
    private final List<Generation> generations = new ArrayList<>();
    private final Map<String, Integer> inActionPerId = new HashMap<>();
    private final Map<String, Integer> resumesInFlight = new HashMap<>();
    private int startTrueInFlight;
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
            } else if (generation.armed) {
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
            if (call.kind() == Kind.RESUME) {
                resumesInFlight.merge(call.id(), 1, Integer::sum);
                disarm(call.id());
            } else if (call.kind() == Kind.START_TRUE) {
                startTrueInFlight++;
                ids.forEach(this::disarm);
            }
        }
    }

    void end(LifecycleCall call, @Nullable Generation made, boolean returned) {
        synchronized (lock) {
            clock++;
            switch (call.kind()) {
                case RESUME -> resumesInFlight.merge(call.id(), -1, Integer::sum);
                case START_TRUE -> startTrueInFlight--;
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
                        arm(call.id());
                    }
                }
                case STOP -> {
                    if (returned) {
                        ids.forEach(this::arm);
                    }
                }
                default -> {
                }
            }
        }
    }

    private boolean returnedBefore(Generation g) {
        return g.subscribedOk && g.returnedAt < beganAt.get();
    }

    private void arm(String id) {
        if (startTrueInFlight == 0 && resumesInFlight.getOrDefault(id, 0) == 0) {
            generations.stream().filter(g -> g.id.equals(id) && returnedBefore(g) && !g.cancelled).forEach(g -> g.armed = true);
        }
    }

    private void disarm(String id) {
        generations.stream().filter(g -> g.id.equals(id)).forEach(g -> g.armed = false);
    }

    @Nullable Generation currentGeneration(String id) {
        synchronized (lock) {
            return generations.stream().filter(g -> g.id.equals(id) && g.subscribedOk && !g.cancelled).max((a, b) -> Long.compare(a.returnedAt, b.returnedAt)).orElse(null);
        }
    }
}
