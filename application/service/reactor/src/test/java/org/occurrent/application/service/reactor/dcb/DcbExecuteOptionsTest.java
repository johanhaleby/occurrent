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

package org.occurrent.application.service.reactor.dcb;

import org.junit.jupiter.api.*;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.Tag;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("DcbExecuteOptions")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DcbExecuteOptionsTest {

    @Nested
    @DisplayName("when checking equality")
    @SuppressWarnings("removal")
    class When_checking_equality {

        private static final Function<List<DomainEvent>, Mono<Void>> SHARED_SIDE_EFFECT = events -> Mono.empty();
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
        void differs_when_versions_are_requested_in_only_one() {
            var first = DcbExecuteOptions.<DomainEvent>options().withVersions();
            var second = DcbExecuteOptions.<DomainEvent>options();

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void differs_when_from_versions_differ() {
            var first = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 40);
            var second = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 3), 40);

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void options_with_same_from_versions_are_equal_and_share_hash_code() {
            var first = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 40);
            var second = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 40);

            assertAll(
                    () -> assertThat(first).isEqualTo(second),
                    () -> assertThat(first.hashCode()).isEqualTo(second.hashCode())
            );
        }

        @Test
        void differs_when_the_highest_folded_position_differs() {
            var first = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 40);
            var second = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 41);

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void the_highest_folded_position_is_part_of_the_string_form() {
            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 40);

            assertThat(options.toString()).contains("highestFoldedPosition=40");
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

    private static final Function<List<DomainEvent>, Mono<Void>> SIDE_EFFECT = events -> Mono.empty();

    @Nested
    @DisplayName("when asking for marker versions")
    class When_asking_for_marker_versions {

        @Test
        void no_versions_are_requested_by_default() {
            var options = DcbExecuteOptions.<DomainEvent>options();

            assertAll(
                    () -> assertThat(options.versionsRequested()).isFalse(),
                    () -> assertThat(options.fromVersions()).isNull()
            );
        }

        @Test
        void withVersions_requests_versions_and_reads_the_whole_boundary() {
            var options = DcbExecuteOptions.<DomainEvent>options().withVersions();

            assertAll(
                    () -> assertThat(options.versionsRequested()).isTrue(),
                    () -> assertThat(options.fromVersions()).isNull()
            );
        }

        @Test
        void fromVersions_requests_versions_and_reads_after_the_given_ones() {
            DcbMarkerVersions saved = versions("epoch-1", 2);

            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(saved, 40);

            assertAll(
                    () -> assertThat(options.versionsRequested()).isTrue(),
                    () -> assertThat(options.fromVersions()).isEqualTo(saved)
            );
        }

        @Test
        void fromVersions_keeps_the_highest_folded_position() {
            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 40);

            assertThat(options.highestFoldedPosition()).isEqualTo(40);
        }

        @Test
        void the_highest_folded_position_is_zero_when_no_versions_are_given() {
            assertAll(
                    () -> assertThat(DcbExecuteOptions.<DomainEvent>options().highestFoldedPosition()).isZero(),
                    () -> assertThat(DcbExecuteOptions.<DomainEvent>options().withVersions().highestFoldedPosition()).isZero()
            );
        }

        @Test
        void fromVersions_accepts_highest_folded_position_zero_for_a_boundary_without_events() {
            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 0);

            assertThat(options.highestFoldedPosition()).isZero();
        }

        @Test
        void fromVersions_rejects_a_negative_highest_folded_position() {
            var options = DcbExecuteOptions.<DomainEvent>options();

            assertThatThrownBy(() -> options.fromVersions(versions("epoch-1", 2), -1)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("highestFoldedPosition");
        }

        @Test
        void withVersions_after_fromVersions_keeps_the_versions_to_read_after() {
            DcbMarkerVersions saved = versions("epoch-1", 2);

            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(saved, 40).withVersions();

            assertAll(
                    () -> assertThat(options.fromVersions()).isEqualTo(saved),
                    () -> assertThat(options.highestFoldedPosition()).isEqualTo(40)
            );
        }

        @Test
        void the_versions_survive_adding_a_side_effect_and_a_tag_generator() {
            DcbMarkerVersions saved = versions("epoch-1", 2);

            var options = DcbExecuteOptions.<DomainEvent>options().fromVersions(saved, 40).sideEffect(SIDE_EFFECT).tagGenerator(event -> Set.of(Tag.parse("name:1")));

            assertAll(
                    () -> assertThat(options.versionsRequested()).isTrue(),
                    () -> assertThat(options.fromVersions()).isEqualTo(saved),
                    () -> assertThat(options.highestFoldedPosition()).isEqualTo(40)
            );
        }

        @Test
        @SuppressWarnings("removal")
        void fromPosition_after_withVersions_is_refused() {
            var withVersions = DcbExecuteOptions.<DomainEvent>options().withVersions();

            assertThatThrownBy(() -> withVersions.fromPosition(5L)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        @SuppressWarnings("removal")
        void fromPosition_after_fromVersions_is_refused() {
            var fromVersions = DcbExecuteOptions.<DomainEvent>options().fromVersions(versions("epoch-1", 2), 40);

            assertThatThrownBy(() -> fromVersions.fromPosition(5L)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        @SuppressWarnings("removal")
        void withVersions_after_fromPosition_is_refused() {
            var fromPosition = DcbExecuteOptions.<DomainEvent>options().fromPosition(5L);

            assertThatThrownBy(fromPosition::withVersions).isInstanceOf(IllegalStateException.class);
        }

        @Test
        @SuppressWarnings("removal")
        void fromVersions_after_fromPosition_is_refused() {
            var fromPosition = DcbExecuteOptions.<DomainEvent>options().fromPosition(5L);

            assertThatThrownBy(() -> fromPosition.fromVersions(versions("epoch-1", 2), 40)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void fromVersions_rejects_null() {
            var options = DcbExecuteOptions.<DomainEvent>options();

            assertThatThrownBy(() -> options.fromVersions(null, 0)).isInstanceOf(NullPointerException.class).hasMessageContaining("markerVersions");
        }

        @Test
        @SuppressWarnings("removal")
        void fromPosition_alone_still_reads_after_the_position_without_asking_for_versions() {
            var options = DcbExecuteOptions.<DomainEvent>options().fromPosition(5L);

            assertAll(
                    () -> assertThat(options.fromPosition()).isEqualTo(5L),
                    () -> assertThat(options.versionsRequested()).isFalse()
            );
        }
    }

    private static DcbMarkerVersions versions(String epoch, long version) {
        return DcbMarkerVersions.of(Map.of("tag:name:1", new DcbMarkerVersions.MarkerVersion(epoch, version, 0)));
    }

    private record DomainEvent(String type, String name) {
    }
}
