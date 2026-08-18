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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class AppliedAppendRecordingRegistryTest {

    private final AppliedAppendRecordingRegistry registry =
            new AppliedAppendRecordingRegistry(Duration.ofMillis(200), Duration.ofSeconds(5), 2.0);

    @Test
    void a_freshly_registered_projection_is_due_at_the_initial_interval() {
        registry.register("p1", () -> false, () -> {
        });

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(200).toNanos());
    }

    @Test
    void a_live_tick_doubles_the_interval() {
        registry.register("p1", () -> false, () -> {
        });

        registry.tick("p1");

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(400).toNanos());
    }

    @Test
    void repeated_live_ticks_cap_the_interval_at_the_configured_max() {
        registry.register("p1", () -> false, () -> {
        });

        for (int i = 0; i < 10; i++) {
            registry.tick("p1");
        }

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofSeconds(5).toNanos());
    }

    @Test
    void a_replaying_tick_resets_the_interval_to_the_fast_end_and_calls_replayObserved() {
        AtomicInteger replayObservedCalls = new AtomicInteger();
        AtomicBoolean replaying = new AtomicBoolean(false);
        registry.register("p1", replaying::get, replayObservedCalls::incrementAndGet);

        // Grow the interval first, so the reset below is actually observable.
        registry.tick("p1");
        registry.tick("p1");
        assertThat(registry.dueInNanos("p1")).isGreaterThan(Duration.ofMillis(200).toNanos());

        replaying.set(true);
        registry.tick("p1");

        assertThat(replayObservedCalls.get()).isEqualTo(1);
        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(200).toNanos());
    }

    @Test
    void a_tick_or_dueInNanos_for_an_unregistered_id_throws() {
        assertThatThrownBy(() -> registry.tick("unknown")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.dueInNanos("unknown")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void two_projections_pace_independently() {
        AtomicBoolean p2Replaying = new AtomicBoolean(true);
        registry.register("p1", () -> false, () -> {
        });
        registry.register("p2", p2Replaying::get, () -> {
        });

        registry.tick("p1");
        registry.tick("p2");

        assertThat(registry.dueInNanos("p1")).isEqualTo(Duration.ofMillis(400).toNanos());
        assertThat(registry.dueInNanos("p2")).isEqualTo(Duration.ofMillis(200).toNanos());
    }
}
