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

package org.occurrent.application.service.reactor;

import org.junit.jupiter.api.*;
import org.occurrent.application.service.ExecuteFilter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.StreamReadFilter;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("ExecuteOptions")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ExecuteOptionsTest {

    @Nested
    @DisplayName("when withers are composed in any order")
    class When_withers_are_composed_in_any_order {

        @Test
        void filter_preserves_a_previously_set_side_effect_and_from_stream_version() {
            // Given
            var filter = StreamReadFilter.type(NameDefined.class.getName());
            var sideEffectInvocations = new AtomicInteger();

            // When
            var executeOptions = ExecuteOptions.<DomainEvent>options()
                    .fromStreamVersion(5L)
                    .sideEffect(events -> Mono.fromRunnable(sideEffectInvocations::incrementAndGet))
                    .filter(filter);

            executeOptions.sideEffect().apply(List.of(nameDefined("Ada"))).block();

            // Then
            assertAll(
                    () -> assertThat(executeOptions.filter()).isEqualTo(filter),
                    () -> assertThat(executeOptions.fromStreamVersion()).isEqualTo(5L),
                    () -> assertThat(sideEffectInvocations).hasValue(1)
            );
        }

        @Test
        void execute_filter_preserves_a_previously_set_side_effect_and_from_stream_version() {
            // Given
            var executeFilter = ExecuteFilter.<DomainEvent>type(NameDefined.class);
            var sideEffectInvocations = new AtomicInteger();

            // When
            var executeOptions = ExecuteOptions.<DomainEvent>options()
                    .fromStreamVersion(5L)
                    .sideEffect(events -> Mono.fromRunnable(sideEffectInvocations::incrementAndGet))
                    .filter(executeFilter);

            executeOptions.sideEffect().apply(List.of(nameDefined("Ada"))).block();

            // Then
            assertAll(
                    () -> assertThat(executeOptions.executeFilter()).isEqualTo(executeFilter),
                    () -> assertThat(executeOptions.filter()).isNull(),
                    () -> assertThat(executeOptions.fromStreamVersion()).isEqualTo(5L),
                    () -> assertThat(sideEffectInvocations).hasValue(1)
            );
        }
    }

    @Nested
    @DisplayName("when setting fromStreamVersion")
    class When_setting_from_stream_version {

        @Test
        void retains_the_value() {
            var executeOptions = ExecuteOptions.<DomainEvent>options().fromStreamVersion(42L);

            assertThat(executeOptions.fromStreamVersion()).isEqualTo(42L);
        }

        @Test
        void rejects_a_negative_value() {
            assertThatThrownBy(() -> ExecuteOptions.<DomainEvent>options().fromStreamVersion(-1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot be negative");
        }

        @Test
        void rejects_a_value_above_int_max() {
            assertThatThrownBy(() -> ExecuteOptions.<DomainEvent>options().fromStreamVersion((long) Integer.MAX_VALUE + 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Integer.MAX_VALUE")
                    .hasMessageContaining("skip");
        }
    }

    @Nested
    @DisplayName("when checking equality")
    class When_checking_equality {

        private static final Function<List<DomainEvent>, Mono<Void>> SHARED_SIDE_EFFECT = events -> Mono.empty();

        @Test
        void options_with_same_values_are_equal_and_share_hash_code() {
            var filter = StreamReadFilter.type(NameDefined.class.getName());

            var first = ExecuteOptions.<DomainEvent>options().filter(filter).sideEffect(SHARED_SIDE_EFFECT).fromStreamVersion(5L);
            var second = ExecuteOptions.<DomainEvent>options().filter(filter).sideEffect(SHARED_SIDE_EFFECT).fromStreamVersion(5L);

            assertAll(
                    () -> assertThat(first).isEqualTo(second),
                    () -> assertThat(first.hashCode()).isEqualTo(second.hashCode())
            );
        }

        @Test
        void differs_when_filter_differs() {
            var first = ExecuteOptions.<DomainEvent>options().filter(StreamReadFilter.type(NameDefined.class.getName())).sideEffect(SHARED_SIDE_EFFECT);
            var second = ExecuteOptions.<DomainEvent>options().filter(StreamReadFilter.type(NameWasChanged.class.getName())).sideEffect(SHARED_SIDE_EFFECT);

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void differs_when_execute_filter_differs() {
            var first = ExecuteOptions.<DomainEvent>options().filter(ExecuteFilter.<DomainEvent>type(NameDefined.class)).sideEffect(SHARED_SIDE_EFFECT);
            var second = ExecuteOptions.<DomainEvent>options().filter(ExecuteFilter.<DomainEvent>type(NameWasChanged.class)).sideEffect(SHARED_SIDE_EFFECT);

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void differs_when_side_effect_differs() {
            var first = ExecuteOptions.<DomainEvent>options().sideEffect(SHARED_SIDE_EFFECT);
            var second = ExecuteOptions.<DomainEvent>options();

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void differs_when_from_stream_version_differs() {
            var first = ExecuteOptions.<DomainEvent>options().sideEffect(SHARED_SIDE_EFFECT).fromStreamVersion(5L);
            var second = ExecuteOptions.<DomainEvent>options().sideEffect(SHARED_SIDE_EFFECT).fromStreamVersion(6L);

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void separately_written_but_behaviourally_identical_side_effects_are_not_equal() {
            // Pins deliberate behavior: sideEffect compares by identity, so this must stay unequal.
            var first = ExecuteOptions.<DomainEvent>options().sideEffect((Function<List<DomainEvent>, Mono<Void>>) events -> Mono.empty());
            var second = ExecuteOptions.<DomainEvent>options().sideEffect((Function<List<DomainEvent>, Mono<Void>>) events -> Mono.empty());

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void options_are_not_equal_to_null() {
            assertThat(ExecuteOptions.<DomainEvent>options()).isNotEqualTo(null);
        }

        @Test
        void options_are_not_equal_to_an_unrelated_type() {
            assertThat(ExecuteOptions.<DomainEvent>options()).isNotEqualTo("not-execute-options");
        }
    }

    private static NameDefined nameDefined(String name) {
        return new NameDefined("event-" + name, LocalDateTime.of(2024, 1, 2, 3, 4), "user", name);
    }
}
