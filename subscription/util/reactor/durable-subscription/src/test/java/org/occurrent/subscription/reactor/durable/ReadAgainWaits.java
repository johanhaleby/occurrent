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

package org.occurrent.subscription.reactor.durable;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The waits that {@link ReactorDurableSubscriptionModel} documents before it reads where the feed was again, after a
 * read that failed. The first wait is between 100 and 150 milliseconds, and the waits about double from there, up to
 * 5 seconds, which the seventh wait is the first that can reach.
 */
final class ReadAgainWaits {
    private static final long FIRST_MILLIS = 100;
    private static final long LONGEST_MILLIS = 5000;

    private ReadAgainWaits() {
    }

    /**
     * Each wait in {@code waits}, the first wait first, is within half and one and a half of a wait that starts at
     * 100 milliseconds and doubles up to 5 seconds, and never shorter than 100 milliseconds or longer than 5 seconds.
     */
    static void assertDocumented(List<Duration> waits) {
        assertThat(waits).as("waits before a read again").isNotEmpty();
        for (int wait = 1; wait <= waits.size(); wait++) {
            long doubled = Math.min(FIRST_MILLIS << (wait - 1), LONGEST_MILLIS);
            long shortest = Math.max(FIRST_MILLIS, doubled / 2);
            long longest = Math.min(LONGEST_MILLIS, doubled * 3 / 2);
            assertThat(waits.get(wait - 1).toMillis()).as("milliseconds of wait " + wait + " of " + waits).isBetween(shortest, longest);
        }
    }
}
