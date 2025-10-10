/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.retry;

import org.jspecify.annotations.NullMarked;

import java.time.Duration;
import java.util.Objects;

/**
 * Retry strategy to use if the action throws an exception.
 */
@NullMarked
public sealed interface Backoff {

    /**
     * @return Don't retry and re-throw an exception thrown when action is invoked.
     */
    static Backoff none() {
        return new None();
    }

    /**
     * @return Retry after a fixed number of millis if an action throws an exception.
     */
    static Backoff fixed(long millis) {
        return new Fixed(millis);
    }

    /**
     * @return Retry after a fixed duration if an action throws an exception.
     */
    static Backoff fixed(Duration duration) {
        Objects.requireNonNull(duration, "Duration cannot be null");
        return new Fixed(duration.toMillis());
    }

    /**
     * @return Retry after with exponential backoff if an action throws an exception.
     */
    static Backoff exponential(Duration initial, Duration max, double multiplier) {
        return new Exponential(initial, max, multiplier);
    }


    final class None implements Backoff {
        private None() {
        }
    }

    final class Fixed implements Backoff {
        public final long millis;

        private Fixed(long millis) {
            if (millis <= 0) {
                throw new IllegalArgumentException("Millis cannot be less than zero");
            }
            this.millis = millis;
        }
    }

    final class Exponential implements Backoff {
        public final Duration initial;
        public final Duration max;
        public final double multiplier;

        private Exponential(Duration initial, Duration max, double multiplier) {
            Objects.requireNonNull(initial, "Initial duration cannot be null");
            Objects.requireNonNull(max, "Max duration cannot be null");
            if (multiplier <= 0) {
                throw new IllegalArgumentException("multiplier cannot be less than zero");
            }
            this.initial = initial;
            this.max = max;
            this.multiplier = multiplier;
        }
    }
}