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

import org.jspecify.annotations.Nullable;

import java.time.Duration;

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
 * @param quarantineAfter      how long one event may keep failing for one instance before that instance is quarantined
 *                             at the event's position and the subscription is allowed past it, or {@code null} to keep
 *                             rethrowing forever, which is what every version up to 0.33.0 did
 */
public record SagaRunnerConfig(Duration timerPollInterval, int timerBatchLimit, int maxCasAttempts,
                               RedeliveryDetection redeliveryDetection, @Nullable Duration quarantineAfter) {

    public SagaRunnerConfig {
        requireNonNull(timerPollInterval, "timerPollInterval cannot be null");
        requireNonNull(redeliveryDetection, "redeliveryDetection cannot be null");
        if (quarantineAfter != null && quarantineAfter.isNegative()) {
            throw new IllegalArgumentException("quarantineAfter cannot be negative");
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

    /** A configuration with the default quarantine budget of five minutes. */
    public SagaRunnerConfig(Duration timerPollInterval, int timerBatchLimit, int maxCasAttempts,
                            RedeliveryDetection redeliveryDetection) {
        this(timerPollInterval, timerBatchLimit, maxCasAttempts, redeliveryDetection, DEFAULT_QUARANTINE_AFTER);
    }

    /**
     * The default quarantine budget. Once a MongoDB subscription model's backoff saturates it retries every two
     * seconds, so five minutes is on the order of a hundred and fifty attempts, which is ample evidence that an input
     * is not going to succeed. It also spans the failures worth surviving without quarantining anything, because a replica-set
     * election takes seconds and a rolling restart a minute or two, and both finish well inside it. Against that, it
     * holds the block on the saga's other instances to five minutes rather than forever.
     */
    public static final Duration DEFAULT_QUARANTINE_AFTER = Duration.ofMinutes(5);

    /**
     * The default configuration: poll every 15 seconds, fire up to 100 due instances per poll, retry a lost save up to 50
     * times, require redelivery detection, and quarantine an instance whose event has kept failing for five minutes. The poll interval only bounds how late a due timer fires, and saga
     * timeouts run at a minutes-to-days timescale, so 15 seconds (the same default as JobRunr) keeps the store query
     * load low while firing well within tolerance. Lower it only when you rely on short timeouts firing promptly.
     */
    public static SagaRunnerConfig defaults() {
        return new SagaRunnerConfig(Duration.ofSeconds(15), 100, 50, RedeliveryDetection.REQUIRED, DEFAULT_QUARANTINE_AFTER);
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
     * A copy of this configuration with a different quarantine budget, or with {@code null} to never quarantine. Pass
     * {@code null} only when you would rather one faulty instance kept blocking every other instance of the same saga
     * than have it suspended, since that is the behaviour it restores.
     */
    public SagaRunnerConfig withQuarantineAfter(@Nullable Duration quarantineAfter) {
        return new SagaRunnerConfig(timerPollInterval, timerBatchLimit, maxCasAttempts, redeliveryDetection, quarantineAfter);
    }
}
