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
 * The mutable {@link PushCatchupStatus} the framework populates as it registers and drives each {@code source = PUSH}
 * projection and saga.
 * <p>
 * This type is {@code public} only so that the annotation registrars, which live in the two starter packages, can
 * construct it and call {@link #register}, {@link #recordCatchingUp}, {@link #recordLive} and {@link #recordFailure}.
 * It is not a user-facing API: an application injects {@link PushCatchupStatus}, whose read-only surface is the whole
 * point, the same split {@code SagaInstancesRegistryImpl} uses for {@code SagaInstancesRegistry}.
 */
@NullMarked
public final class PushCatchupStatusImpl implements PushCatchupStatus {

    private final Object lock = new Object();
    // Ids whose live state can be derived by asking a subscription model. Insertion-ordered so all() reports ids in
    // registration order.
    private final Map<String, Liveness> liveness = new LinkedHashMap<>();
    // Ids whose state has to be recorded instead: a failure, which no model reports, and the DomainEventFeed path,
    // which has no model to ask.
    private final Map<String, CatchupStatus> recorded = new LinkedHashMap<>();

    /**
     * Track {@code id} by asking the model it runs on where it is. Public only because the annotation processors that
     * call it live in the two starter packages rather than this one. Application code reads {@link PushCatchupStatus},
     * it does not write to it.
     * <p>
     * Both answers are needed. {@code catchingUp} alone cannot tell a subscription that has handed over from one that
     * was never started, since neither is replaying, and reporting the second as {@link Live} would tell a readiness
     * probe that a projection withheld by {@code occurrent.subscription.mode = manual} is ready to serve.
     * <p>
     * Rejects a second registration for an id already registered here, rather than replacing it. On one stack this is
     * unreachable, the {@code @Projection} and {@code @Saga} registrars each keep their own id set and refuse a
     * duplicate against it before this is ever called. But the blocking and reactor post-processors each hold a
     * separate id set while sharing one {@code PushCatchupStatus} bean, so nothing stops both stacks in one context
     * from registering the same id, and a silent {@code put} would then answer {@link #of} for whichever source
     * registered last while the other kept running unreported. This throws instead of reusing
     * {@code DuplicateSubscriptionIdException}, since that type lives in {@code occurrent-subscription-core} and this
     * module deliberately depends on neither stack's subscription API (see the class doc), so it cannot depend on that
     * exception either.
     *
     * @param id         The projection or saga id.
     * @param catchingUp Answers whether a replay for this id is in flight, normally a subscription model's
     *                   {@code isCatchingUp(id)}.
     * @param running    Answers whether this id's subscription is running at all, normally {@code isRunning(id)}.
     * @throws IllegalArgumentException if {@code id} is already registered
     */
    public void register(String id, BooleanSupplier catchingUp, BooleanSupplier running) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(catchingUp, "catchingUp cannot be null");
        Objects.requireNonNull(running, "running cannot be null");
        synchronized (lock) {
            Liveness previous = liveness.putIfAbsent(id, new Liveness(catchingUp, running));
            if (previous != null) {
                throw new IllegalArgumentException("A push projection or saga with id '%s' is already registered. Each id must be unique across the whole application, including when the blocking and reactive stacks share one context and this bean.".formatted(id));
            }
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

    @Override
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

    @Override
    public boolean isCaughtUp(String id) {
        return of(id) instanceof Live;
    }

    @Override
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
