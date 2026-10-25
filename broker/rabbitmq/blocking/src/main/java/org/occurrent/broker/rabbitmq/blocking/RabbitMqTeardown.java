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

package org.occurrent.broker.rabbitmq.blocking;

import java.time.Duration;

/**
 * What {@link RabbitMqCloudEventSource#close()} and {@code RabbitMqDomainEventSource#close()} share when they tear
 * down. Public only because the two sources live in different packages. Not part of the supported API.
 */
public final class RabbitMqTeardown {

    // Half of the longest span nanoTime differences can express, so adding it to System.nanoTime() never wraps past
    // the point where a deadline comparison by subtraction stops working.
    private static final long LONGEST_TIMEOUT_NANOS = Long.MAX_VALUE / 2;

    private RabbitMqTeardown() {
    }

    /**
     * {@code timeout} in nanoseconds, or the longest span a deadline can be set that far ahead, about 146 years, when
     * {@code timeout} is longer than that. {@link Duration#toNanos()} throws instead.
     */
    public static long timeoutNanos(Duration timeout) {
        return timeout.compareTo(Duration.ofNanos(LONGEST_TIMEOUT_NANOS)) > 0 ? LONGEST_TIMEOUT_NANOS : timeout.toNanos();
    }
}
