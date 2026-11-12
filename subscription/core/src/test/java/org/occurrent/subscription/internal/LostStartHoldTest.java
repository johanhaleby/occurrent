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

package org.occurrent.subscription.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class LostStartHoldTest {

    @Test
    void is_not_asked_by_default() {
        assertThat(LostStartHold.isAsked()).isFalse();
    }

    @Test
    void is_asked_inside_askedDuring() {
        // Given
        AtomicBoolean seen = new AtomicBoolean();

        // When
        LostStartHold.askedDuring(() -> seen.set(LostStartHold.isAsked()));

        // Then
        assertThat(seen).isTrue();
    }

    @Test
    void is_not_asked_after_askedDuring_returns() {
        // When
        LostStartHold.askedDuring(() -> {
        });

        // Then
        assertThat(LostStartHold.isAsked()).isFalse();
    }

    @Test
    void is_not_asked_after_askedDuring_whose_runnable_throws_and_the_exception_propagates_unchanged() {
        // Given
        IllegalStateException failure = new IllegalStateException("subscribe failed");

        // When
        // Then
        assertThatThrownBy(() -> LostStartHold.askedDuring(() -> {
            throw failure;
        })).isSameAs(failure);
        assertThat(LostStartHold.isAsked()).isFalse();
    }

    @Test
    void suspendedDuring_inside_askedDuring_sees_not_asked_and_the_outer_is_asked_again_afterwards() {
        // Given
        AtomicBoolean seenSuspended = new AtomicBoolean(true);
        AtomicBoolean seenAfter = new AtomicBoolean();

        // When
        LostStartHold.askedDuring(() -> {
            LostStartHold.suspendedDuring(() -> {
                seenSuspended.set(LostStartHold.isAsked());
                return null;
            });
            seenAfter.set(LostStartHold.isAsked());
        });

        // Then
        assertThat(seenSuspended).isFalse();
        assertThat(seenAfter).isTrue();
    }

    @Test
    void suspendedDuring_returns_what_the_supplier_returns() {
        assertThat(LostStartHold.suspendedDuring(() -> "result")).isEqualTo("result");
    }

    @Test
    void the_outer_is_asked_again_after_a_suspendedDuring_whose_supplier_throws() {
        // Given
        IllegalStateException failure = new IllegalStateException("call failed");
        AtomicBoolean seenAfter = new AtomicBoolean();

        // When
        LostStartHold.askedDuring(() -> {
            assertThatThrownBy(() -> LostStartHold.suspendedDuring(() -> {
                throw failure;
            })).isSameAs(failure);
            seenAfter.set(LostStartHold.isAsked());
        });

        // Then
        assertThat(seenAfter).isTrue();
        assertThat(LostStartHold.isAsked()).isFalse();
    }

    @Test
    void a_nested_askedDuring_leaves_the_outer_asked() {
        // Given
        AtomicBoolean seenAfterInner = new AtomicBoolean();

        // When
        LostStartHold.askedDuring(() -> {
            LostStartHold.askedDuring(() -> {
            });
            seenAfterInner.set(LostStartHold.isAsked());
        });

        // Then
        assertThat(seenAfterInner).isTrue();
        assertThat(LostStartHold.isAsked()).isFalse();
    }

    @Test
    void is_not_asked_on_another_thread() throws InterruptedException {
        // Given
        AtomicBoolean seenOnOtherThread = new AtomicBoolean(true);
        AtomicReference<Thread> other = new AtomicReference<>();

        // When
        LostStartHold.askedDuring(() -> {
            other.set(new Thread(() -> seenOnOtherThread.set(LostStartHold.isAsked())));
            other.get().start();
        });
        other.get().join();

        // Then
        assertThat(seenOnOtherThread).isFalse();
    }

    @Test
    void askedDuringCall_reports_a_subscription_held_paused_inside_the_call() {
        // When
        LostStartHold.Answer<String> answer = LostStartHold.askedDuringCall(() -> {
            LostStartHold.heldPaused();
            return "result";
        });

        // Then
        assertThat(answer.result()).isEqualTo("result");
        assertThat(answer.heldPaused()).isTrue();
    }

    @Test
    void askedDuringCall_reports_a_subscription_held_paused_inside_a_nested_suspendedDuring() {
        // When
        LostStartHold.Answer<String> answer = LostStartHold.askedDuringCall(() -> LostStartHold.suspendedDuring(() -> {
            LostStartHold.heldPaused();
            return "result";
        }));

        // Then
        assertThat(answer.heldPaused()).isTrue();
    }

    @Test
    void askedDuringCall_reports_nothing_held_when_heldPaused_is_not_called() {
        // When
        LostStartHold.Answer<String> answer = LostStartHold.askedDuringCall(() -> "result");

        // Then
        assertThat(answer.heldPaused()).isFalse();
    }

    @Test
    void a_nested_askedDuringCall_has_a_flag_of_its_own_and_leaves_the_flag_of_the_outer_one() {
        // Given
        AtomicReference<LostStartHold.Answer<String>> inner = new AtomicReference<>();

        // When
        LostStartHold.Answer<String> outer = LostStartHold.askedDuringCall(() -> {
            inner.set(LostStartHold.askedDuringCall(() -> "inner"));
            LostStartHold.heldPaused();
            return "outer";
        });
        LostStartHold.Answer<String> outerThatHeldNothing = LostStartHold.askedDuringCall(() -> {
            LostStartHold.askedDuringCall(() -> {
                LostStartHold.heldPaused();
                return "inner";
            });
            return "outer";
        });

        // Then
        assertThat(inner.get().heldPaused()).as("inner that held nothing").isFalse();
        assertThat(outer.heldPaused()).as("outer that held").isTrue();
        assertThat(outerThatHeldNothing.heldPaused()).as("outer when only the inner held").isFalse();
    }

    @Test
    void heldPaused_outside_a_call_does_nothing_and_does_not_leak_to_a_later_call() {
        // When
        LostStartHold.heldPaused();
        LostStartHold.Answer<String> later = LostStartHold.askedDuringCall(() -> "result");

        // Then
        assertThat(later.heldPaused()).isFalse();
    }

    @Test
    void the_flag_is_cleared_after_an_askedDuringCall_that_throws() {
        // Given
        IllegalStateException failure = new IllegalStateException("fails");

        // When
        assertThatThrownBy(() -> LostStartHold.askedDuringCall(() -> {
            LostStartHold.heldPaused();
            throw failure;
        })).isSameAs(failure);
        LostStartHold.Answer<String> later = LostStartHold.askedDuringCall(() -> "result");

        // Then
        assertThat(later.heldPaused()).isFalse();
        assertThat(LostStartHold.isAsked()).isFalse();
    }
}
