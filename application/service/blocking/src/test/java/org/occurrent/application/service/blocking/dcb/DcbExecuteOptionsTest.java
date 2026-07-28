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

package org.occurrent.application.service.blocking.dcb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.Tag;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("DcbExecuteOptions")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DcbExecuteOptionsTest {

    @Nested
    @DisplayName("when checking equality")
    class When_checking_equality {

        private static final Consumer<List<DomainEvent>> SHARED_SIDE_EFFECT = events -> {
        };
        private static final TagGenerator<DomainEvent> SHARED_TAG_GENERATOR = event -> Set.of(Tag.parse("name:1"));

        @Test
        void options_with_same_values_are_equal_and_share_hash_code() {
            var first = DcbExecuteOptions.<DomainEvent>options().sideEffect(SHARED_SIDE_EFFECT).tagGenerator(SHARED_TAG_GENERATOR).fromPosition(5L);
            var second = DcbExecuteOptions.<DomainEvent>options().sideEffect(SHARED_SIDE_EFFECT).tagGenerator(SHARED_TAG_GENERATOR).fromPosition(5L);

            assertAll(
                    () -> assertThat(first).isEqualTo(second),
                    () -> assertThat(first.hashCode()).isEqualTo(second.hashCode())
            );
        }

        @Test
        void differs_when_side_effect_differs() {
            var first = DcbExecuteOptions.<DomainEvent>options().sideEffect(SHARED_SIDE_EFFECT);
            var second = DcbExecuteOptions.<DomainEvent>options();

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void differs_when_tag_generator_differs() {
            var first = DcbExecuteOptions.<DomainEvent>options().tagGenerator(SHARED_TAG_GENERATOR);
            var second = DcbExecuteOptions.<DomainEvent>options().tagGenerator(event -> Set.of(Tag.parse("name:2")));

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void differs_when_from_position_differs() {
            var first = DcbExecuteOptions.<DomainEvent>options().sideEffect(SHARED_SIDE_EFFECT).fromPosition(5L);
            var second = DcbExecuteOptions.<DomainEvent>options().sideEffect(SHARED_SIDE_EFFECT).fromPosition(6L);

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void separately_written_but_behaviourally_identical_tag_generators_are_not_equal() {
            // Pins deliberate behavior: tagGenerator compares by identity, so this must stay unequal.
            var first = DcbExecuteOptions.<DomainEvent>options().tagGenerator((TagGenerator<DomainEvent>) event -> Set.of(Tag.parse("name:1")));
            var second = DcbExecuteOptions.<DomainEvent>options().tagGenerator((TagGenerator<DomainEvent>) event -> Set.of(Tag.parse("name:1")));

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void options_are_not_equal_to_null() {
            assertThat(DcbExecuteOptions.<DomainEvent>options()).isNotEqualTo(null);
        }

        @Test
        void options_are_not_equal_to_an_unrelated_type() {
            assertThat(DcbExecuteOptions.<DomainEvent>options()).isNotEqualTo("not-execute-options");
        }
    }

    private record DomainEvent(String type, String name) {
    }
}
