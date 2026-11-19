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
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StringBasedCheckpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupMarkersTest {

    @Test
    void a_stored_position_is_read_as_the_catch_up_position() {
        assertThat(CatchupMarkers.positionOf("proj", GlobalCheckpoint.of(7))).isEqualTo(7L);
    }

    @Test
    void a_checkpoint_that_is_not_a_position_is_refused_with_a_not_a_catch_up_marker_exception_naming_the_id() {
        Throwable refused = catchThrowable(() -> CatchupMarkers.positionOf("proj", new StringBasedCheckpoint("resume-token-xyz")));

        assertThat(refused).isInstanceOf(CatchupMarkers.NotACatchupMarkerException.class).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("\"proj\"");
    }

    @Test
    void the_refusal_is_recognised_when_it_is_wrapped_in_another_exception() {
        Throwable refused = catchThrowable(() -> CatchupMarkers.positionOf("proj", new StringBasedCheckpoint("resume-token-xyz")));

        assertThat(CatchupMarkers.refusedANonMarker(refused)).as("the refusal itself").isTrue();
        assertThat(CatchupMarkers.refusedANonMarker(new RuntimeException("outer", new IllegalStateException("middle", refused)))).as("the refusal two causes down").isTrue();
    }

    @Test
    void an_unrelated_failure_is_not_a_refusal_of_a_non_marker() {
        assertThat(CatchupMarkers.refusedANonMarker(new IllegalStateException("not a marker, but not that refusal either"))).as("a plain IllegalStateException").isFalse();
        assertThat(CatchupMarkers.refusedANonMarker(new RuntimeException("outer", new IllegalArgumentException("cause")))).as("a wrapped unrelated failure").isFalse();
    }
}
