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

import static java.util.Objects.requireNonNull;

/**
 * Tuning for a {@link SagaRunner}: how often it polls its state store for due timers, how many due instances it fires per
 * poll, and how many times it retries a compare-and-set save that lost to a concurrent write before giving up.
 *
 * @param timerPollInterval how often to poll for due timers
 * @param timerBatchLimit   the maximum number of due instances fired per poll
 * @param maxCasAttempts    the maximum compare-and-set attempts for one input before failing
 */
public record SagaRunnerConfig(Duration timerPollInterval, int timerBatchLimit, int maxCasAttempts) {

    public SagaRunnerConfig {
        requireNonNull(timerPollInterval, "timerPollInterval cannot be null");
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

    /** The default configuration: poll every second, fire up to 100 due instances per poll, retry a lost save up to 50 times. */
    public static SagaRunnerConfig defaults() {
        return new SagaRunnerConfig(Duration.ofSeconds(1), 100, 50);
    }

    /** A copy of this configuration with a different poll interval. */
    public SagaRunnerConfig withTimerPollInterval(Duration interval) {
        return new SagaRunnerConfig(interval, timerBatchLimit, maxCasAttempts);
    }
}
