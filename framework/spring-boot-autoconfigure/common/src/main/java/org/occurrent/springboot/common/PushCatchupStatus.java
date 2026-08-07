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

package org.occurrent.springboot.common;

import org.jspecify.annotations.NullMarked;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Where each {@code source = PUSH} projection and saga is in its catch-up, so an application can tell a read model that
 * is still filling from one that is ready to serve.
 * <p>
 * This exists because {@code startupMode = BACKGROUND} starts the application while the replay is still running. Nobody
 * waits for that replay, which is the whole point, so neither its progress nor its failure has anywhere to be returned
 * or thrown. Inject this bean and check it from a health indicator or a readiness probe. A failure is also logged at
 * {@code ERROR}, so an application that never injects this still sees it in the logs.
 * <p>
 * The states are exhaustive, which is what makes a readiness probe expressible:
 * <pre>{@code
 * switch (status.of("orders")) {
 *     case CatchingUp ignored -> notReadyYet();
 *     case Live ignored       -> ready();
 *     case NotStarted ignored -> notStartedYet();
 *     case Failed failed      -> unhealthy(failed.cause());
 *     case Unknown ignored    -> notRegisteredHere();
 * }
 * }</pre>
 * Where there is a subscription model to ask, {@link CatchingUp}, {@link Live} and {@link NotStarted} are derived from
 * it rather than recorded, so a model that was stopped and started again, replaying its history a second time, reports
 * {@link CatchingUp} again rather than staying at whatever it reached the first time. A {@code DomainEventFeed} cannot
 * be asked, so those ids carry a recorded state instead.
 * <p>
 * One class for both stacks, since a push catch-up means the same thing on each. That is why {@link #register} takes
 * {@link BooleanSupplier}s rather than a subscription model type: this module depends on neither stack's subscription
 * API and should not start.
 */
@NullMarked
public final class PushCatchupStatus {

    /**
     * Where one subscription id is in its catch-up. Sealed, so a caller can switch over every case, and only
     * {@link Failed} carries a cause.
     */
    public sealed interface CatchupStatus {
        /**
         * @return The subscription or projection id this status is about.
         */
        String id();
    }

    /**
     * The projection or saga is registered but its subscription has not been started, so it is neither replaying nor
     * taking live events. That is what {@code occurrent.subscription.mode = manual} leaves it as until the application
     * starts it, and what a stopped subscription model leaves it as until something starts it again.
     * <p>
     * Distinct from {@link Unknown}, which is an id nothing here registered at all, and from {@link CatchingUp}, which
     * is working through history on its own and will reach {@link Live} without anyone intervening. This one will not.
     */
    public record NotStarted(String id) implements CatchupStatus {
        public NotStarted {
            Objects.requireNonNull(id, "id cannot be null");
        }
    }

    /**
     * The catch-up is replaying history. Whatever this id projects into is incomplete, so a read model behind it is
     * not ready to serve.
     */
    public record CatchingUp(String id) implements CatchupStatus {
        public CatchingUp {
            Objects.requireNonNull(id, "id cannot be null");
        }
    }

    /**
     * The catch-up finished and handed over, so this id is taking live events. Includes {@code catchup = NONE}, which
     * has no history to replay and is live from the start.
     */
    public record Live(String id) implements CatchupStatus {
        public Live {
            Objects.requireNonNull(id, "id cannot be null");
        }
    }

    /**
     * The catch-up failed and will not recover on its own. The subscription keeps its registration and refuses every
     * event afterwards, so the source redelivers rather than losing them. Fix the cause, then cancel the subscription
     * and subscribe again.
     */
    public record Failed(String id, Throwable cause) implements CatchupStatus {
        public Failed {
            Objects.requireNonNull(id, "id cannot be null");
            Objects.requireNonNull(cause, "cause cannot be null");
        }
    }

    /**
     * Nothing here knows this id. It is not a push projection or saga registered by an Occurrent starter, or it is
     * spelled differently. Deliberately distinct from {@link Live}, since the question a readiness probe asks is
     * whether a named read model is ready, and an unknown name is not an answer of yes.
     */
    public record Unknown(String id) implements CatchupStatus {
        public Unknown {
            Objects.requireNonNull(id, "id cannot be null");
        }
    }

    private final Object lock = new Object();
    // Ids whose live state can be derived by asking a subscription model. Insertion-ordered so all() reports ids in
    // registration order.
    private final Map<String, Liveness> liveness = new LinkedHashMap<>();
    // Ids whose state has to be recorded instead: a failure, which no model reports, and the DomainEventFeed path,
    // which has no model to ask.
    private final Map<String, CatchupStatus> recorded = new LinkedHashMap<>();

    /**
     * Track {@code id} by asking the model it runs on where it is. Public only because the annotation processors that
     * call it live in the two starter packages rather than this one. Application code reads this bean, it does not
     * write to it.
     * <p>
     * Both answers are needed. {@code catchingUp} alone cannot tell a subscription that has handed over from one that
     * was never started, since neither is replaying, and reporting the second as {@link Live} would tell a readiness
     * probe that a projection withheld by {@code occurrent.subscription.mode = manual} is ready to serve.
     *
     * @param id         The projection or saga id.
     * @param catchingUp Answers whether a replay for this id is in flight, normally a subscription model's
     *                   {@code isCatchingUp(id)}.
     * @param running    Answers whether this id's subscription is running at all, normally {@code isRunning(id)}.
     */
    public void register(String id, BooleanSupplier catchingUp, BooleanSupplier running) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(catchingUp, "catchingUp cannot be null");
        Objects.requireNonNull(running, "running cannot be null");
        synchronized (lock) {
            liveness.put(id, new Liveness(catchingUp, running));
        }
    }

    private record Liveness(BooleanSupplier catchingUp, BooleanSupplier running) {
    }

    /**
     * Record that the catch-up of {@code id} has started, for a feed that cannot be asked. Processor-facing, see
     * {@link #register}.
     */
    public void recordCatchingUp(String id) {
        record(id, new CatchingUp(id));
    }

    /**
     * Record that the catch-up of {@code id} finished and it is taking live events, for a feed that cannot be asked.
     * Processor-facing, see {@link #register}.
     */
    public void recordLive(String id) {
        record(id, new Live(id));
    }

    /**
     * Record that the catch-up of {@code id} failed. Processor-facing, see {@link #register}. A second failure for the
     * same id replaces the first.
     */
    public void recordFailure(String id, Throwable cause) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(cause, "cause cannot be null");
        record(id, new Failed(id, cause));
    }

    private void record(String id, CatchupStatus status) {
        Objects.requireNonNull(id, "id cannot be null");
        synchronized (lock) {
            recorded.put(id, status);
        }
    }

    /**
     * Where the projection or saga with this id is in its catch-up.
     *
     * @param id The projection or saga id.
     * @return Its status, or {@link Unknown} if nothing here knows the id.
     */
    public CatchupStatus of(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        final Liveness asked;
        synchronized (lock) {
            CatchupStatus recordedStatus = recorded.get(id);
            // A failure wins over anything a model would say. It is terminal, and a model forgets a replay that
            // failed, so asking would report the id as Live.
            if (recordedStatus instanceof Failed) {
                return recordedStatus;
            }
            asked = liveness.get(id);
            if (asked == null) {
                return recordedStatus == null ? new Unknown(id) : recordedStatus;
            }
        }
        // Asked outside the lock, since these read a subscription model and holding this lock across them would tie
        // every reader of this bean to whatever that model synchronizes on.
        if (asked.catchingUp().getAsBoolean()) {
            return new CatchingUp(id);
        }
        // Checked after catching up, because a model reports a replay as running: asking this first would report a
        // replay in flight as live.
        return asked.running().getAsBoolean() ? new Live(id) : new NotStarted(id);
    }

    /**
     * @param id The projection or saga id.
     * @return {@code true} only when {@code id} is known and has handed over to live events. {@link Unknown} answers
     * {@code false}, because a readiness probe asking about a name nothing recognises has not been told yes.
     */
    public boolean isCaughtUp(String id) {
        return of(id) instanceof Live;
    }

    /**
     * Every push projection and saga this application registered, keyed by id, in registration order.
     */
    public Map<String, CatchupStatus> all() {
        final LinkedHashSet<String> ids;
        synchronized (lock) {
            ids = new LinkedHashSet<>(liveness.keySet());
            ids.addAll(recorded.keySet());
        }
        // A LinkedHashMap rather than Map.copyOf, which would drop the registration order this promises.
        Map<String, CatchupStatus> statuses = new LinkedHashMap<>();
        // of(..) rather than a snapshot taken under the lock, so a derived state is read the same way here as it is
        // one id at a time.
        ids.forEach(id -> statuses.put(id, of(id)));
        return Collections.unmodifiableMap(statuses);
    }

    @Override
    public String toString() {
        return "PushCatchupStatus" + all().values();
    }
}
