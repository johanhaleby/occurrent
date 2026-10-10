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

package org.occurrent.dsl.saga.blocking;

import java.time.Duration;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Tuning for a {@link SagaRunner}: how often it polls its state store for due timers, how many due instances it fires per
 * poll, and how many times it retries a compare-and-set save that lost to a concurrent write before giving up.
 * <p>
 * {@code maxCasAttempts} also bounds dispatch amplification: because commands are dispatched before the save and a lost
 * compare-and-set retries the whole step, a single input can re-dispatch its entire command list up to
 * {@code maxCasAttempts} times. However, a timer retry re-checks if the timer is still due against the reloaded envelope
 * before dispatching, which fences out stale timers and limits realistic amplification to roughly the number of competing
 * nodes, not the full {@code maxCasAttempts}. Command receivers must be idempotent and tolerate that multiplicity, not
 * merely at-least-once delivery.
 *
 * @param timerPollInterval    how often to poll for due timers
 * @param timerBatchLimit      the maximum number of due instances fired per poll
 * @param maxCasAttempts       the maximum compare-and-set attempts for one input before failing, also the maximum number
 *                             of times that input's commands can be re-dispatched
 * @param redeliveryDetection  what to do with an event the runner cannot recognise a redelivery of
 * @param quarantineAfter      how long one instance has to keep failing before it can be quarantined on whichever
 *                             event it is failing on then, or empty to never quarantine. Empty is the default, and
 *                             {@link #withQuarantineAfter(Duration)} turns quarantine on. Reaching the budget is not
 *                             enough on its own, and {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} lists what
 *                             else has to hold. When it is empty the saga keeps rethrowing for as long as the subscription model offers the event
 *                             again, which is what every version up to 0.33.0 did.
 *                             The clock belongs to the instance rather than to one event, so a second event that starts
 *                             failing inherits the elapsed time instead of restarting the budget. It covers everything
 *                             after the saga has worked out which instance the event belongs to, through to the store
 *                             saving the result, and an {@code Error} counts like a {@code RuntimeException}, with
 *                             {@link OutOfMemoryError} the one exclusion, since that is the process failing rather than
 *                             this instance's work. A delivery that fails before the saga can work out which instance
 *                             it belongs to is never let past, because acknowledging it would lose it, so it is
 *                             refused on every redelivery regardless of this setting, and the repeated ERROR that
 *                             refusal logs is paced on this budget when it is set and on a fixed five-minute interval
 *                             when it is not, so that ERROR still fires on a subscription model this switches
 *                             quarantine off for, while that model keeps offering the event. A runner
 *                             ignores this and never quarantines unless its subscription model guarantees that it holds
 *                             every event it delivers, since a quarantined instance skips everything addressed to it
 *                             afterwards and skipping acknowledges. Being able to answer for one event is not enough on
 *                             its own, though the event an instance stops on is checked as well before it is
 *                             acknowledged
 */
public record SagaRunnerConfig(Duration timerPollInterval, int timerBatchLimit, int maxCasAttempts,
                               RedeliveryDetection redeliveryDetection, Optional<Duration> quarantineAfter) {

    public SagaRunnerConfig {
        requireNonNull(timerPollInterval, "timerPollInterval cannot be null");
        requireNonNull(redeliveryDetection, "redeliveryDetection cannot be null");
        requireNonNull(quarantineAfter, "quarantineAfter cannot be null, use disableQuarantine() to never quarantine");
        if (quarantineAfter.filter(budget -> budget.isZero() || budget.isNegative()).isPresent()) {
            // Zero is refused rather than read as "quarantine on the first failure", because the Spring property reads
            // zero as never, and one literal meaning opposite things on the two paths is worse than refusing it here.
            throw new IllegalArgumentException("quarantineAfter must be positive, use disableQuarantine() to never quarantine");
        }
        if (timerPollInterval.isZero() || timerPollInterval.isNegative()) {
            throw new IllegalArgumentException("timerPollInterval must be positive");
        }
        if (timerBatchLimit < 1) {
            throw new IllegalArgumentException("timerBatchLimit must be at least 1");
        }
        if (maxCasAttempts < 1) {
            throw new IllegalArgumentException("maxCasAttempts must be at least 1");
        }
    }

    /**
     * A configuration requiring redelivery detection, which is what every runner wants unless its feed is known to
     * carry no stream metadata at all.
     */
    public SagaRunnerConfig(Duration timerPollInterval, int timerBatchLimit, int maxCasAttempts) {
        this(timerPollInterval, timerBatchLimit, maxCasAttempts, RedeliveryDetection.REQUIRED);
    }

    /** A configuration that never quarantines. */
    public SagaRunnerConfig(Duration timerPollInterval, int timerBatchLimit, int maxCasAttempts,
                            RedeliveryDetection redeliveryDetection) {
        this(timerPollInterval, timerBatchLimit, maxCasAttempts, redeliveryDetection, Optional.empty());
    }

    /**
     * The default configuration: poll every 15 seconds, fire up to 100 due instances per poll, retry a lost save up to 50
     * times, require redelivery detection, and never quarantine. The poll interval only bounds how late a due timer fires, and saga
     * timeouts run at a minutes-to-days timescale, so 15 seconds (the same default as JobRunr) keeps the store query
     * load low while firing well within tolerance. Lower it only when you rely on short timeouts firing promptly.
     */
    public static SagaRunnerConfig defaults() {
        return new SagaRunnerConfig(Duration.ofSeconds(15), 100, 50, RedeliveryDetection.REQUIRED, Optional.empty());
    }

    /** A copy of this configuration with a different poll interval. */
    public SagaRunnerConfig withTimerPollInterval(Duration interval) {
        return new SagaRunnerConfig(interval, timerBatchLimit, maxCasAttempts, redeliveryDetection, quarantineAfter);
    }

    /** A copy of this configuration with a different redelivery-detection posture. */
    public SagaRunnerConfig withRedeliveryDetection(RedeliveryDetection detection) {
        return new SagaRunnerConfig(timerPollInterval, timerBatchLimit, maxCasAttempts, detection, quarantineAfter);
    }

    /**
     * A copy of this configuration that quarantines an instance once it has kept failing for {@code quarantineAfter}.
     * Quarantine is off by default because 0.34.0 has no operation that brings a quarantined instance back, so
     * {@code SagaStateStore.delete(sagaId)} is the only way out of it, and that abandons the instance.
     * <p>
     * Five minutes is a reasonable budget. Once a MongoDB subscription model's backoff saturates it retries every two
     * seconds, so five minutes is about a hundred and fifty attempts. A replica-set election takes seconds and a
     * rolling restart a minute or two, so both finish well inside it without quarantining anything. The budget is also
     * the earliest a failing instance can be quarantined.
     * <p>
     * Use {@link #disableQuarantine()} to turn quarantine off again.
     *
     * @throws NullPointerException     if {@code quarantineAfter} is {@code null}
     * @throws IllegalArgumentException if {@code quarantineAfter} is zero or negative
     */
    public SagaRunnerConfig withQuarantineAfter(Duration quarantineAfter) {
        requireNonNull(quarantineAfter, "quarantineAfter cannot be null, use disableQuarantine() to never quarantine");
        return new SagaRunnerConfig(timerPollInterval, timerBatchLimit, maxCasAttempts, redeliveryDetection, Optional.of(quarantineAfter));
    }

    /**
     * A copy of this configuration that never quarantines, which is also what {@link #defaults()} gives. A faulty
     * instance then keeps failing for as long as the subscription model offers the failed event again, which is the
     * behaviour every version up to 0.33.0 had.
     * <p>
     * What the failing event holds up meanwhile is decided by whatever feeds the subscription, and
     * {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} says what that can be.
     */
    public SagaRunnerConfig disableQuarantine() {
        return new SagaRunnerConfig(timerPollInterval, timerBatchLimit, maxCasAttempts, redeliveryDetection, Optional.empty());
    }
}
