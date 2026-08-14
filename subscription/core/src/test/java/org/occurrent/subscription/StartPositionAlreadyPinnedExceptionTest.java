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

package org.occurrent.subscription;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class StartPositionAlreadyPinnedExceptionTest {

    private static final Checkpoint READ_AT_REGISTRATION = () -> "read-at-registration";
    private static final Checkpoint READ_BACK = () -> "read-back";

    @Test
    void the_standard_message_names_both_positions_and_says_the_second_was_read_after_the_refusal() {
        StartPositionAlreadyPinnedException exception =
                new StartPositionAlreadyPinnedException("someSubscription", READ_AT_REGISTRATION, READ_BACK);

        assertThat(exception.getMessage()).contains("read-at-registration", "read-back", "in a second call");
        assertThat(exception.subscriptionId).isEqualTo("someSubscription");
        assertThat(exception.positionRead).isSameAs(READ_AT_REGISTRATION);
        assertThat(exception.positionStored).contains(READ_BACK);
    }

    @Test
    void a_refusal_with_no_position_to_name_holds_none() {
        StartPositionAlreadyPinnedException exception = new StartPositionAlreadyPinnedException(
                "someSubscription", READ_AT_REGISTRATION, null, "reading it back found nothing");

        assertThat(exception.positionStored).isEqualTo(Optional.empty());
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void a_refusal_whose_read_back_failed_names_the_failure_as_the_cause_and_no_stored_position() {
        RuntimeException readFailure = new IllegalStateException("the checkpoint store is unreachable");

        StartPositionAlreadyPinnedException exception = StartPositionAlreadyPinnedException
                .readingTheStoredPositionBackFailed("someSubscription", READ_AT_REGISTRATION, readFailure);

        assertThat(exception.getMessage()).contains("read-at-registration", "failed");
        assertThat(exception.positionStored).isEqualTo(Optional.empty());
        assertThat(exception.getCause()).isSameAs(readFailure);
    }

    @Test
    void a_refusal_whose_read_back_found_nothing_says_so_without_claiming_the_checkpoint_was_removed() {
        StartPositionAlreadyPinnedException exception = StartPositionAlreadyPinnedException
                .readingTheStoredPositionBackFoundNothing("someSubscription", READ_AT_REGISTRATION);

        assertThat(exception.getMessage())
                .contains("read-at-registration", "found nothing")
                .doesNotContain("null");
        assertThat(exception.positionStored).isEqualTo(Optional.empty());
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void a_factory_reports_a_null_argument_as_the_argument_it_is() {
        assertThatThrownBy(() -> StartPositionAlreadyPinnedException
                .readingTheStoredPositionBackFoundNothing("someSubscription", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("read at registration");
        assertThatThrownBy(() -> StartPositionAlreadyPinnedException
                .readingTheStoredPositionBackFailed(null, READ_AT_REGISTRATION, new IllegalStateException("boom")))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("subscriptionId");
    }

    @Test
    void a_null_position_is_reported_as_the_argument_it_is_rather_than_as_a_failure_to_read_a_position_off_it() {
        assertThatThrownBy(() -> new StartPositionAlreadyPinnedException("someSubscription", READ_AT_REGISTRATION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("read back from storage");
        assertThatThrownBy(() -> new StartPositionAlreadyPinnedException("someSubscription", null, READ_BACK))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("read at registration");
        assertThatThrownBy(() -> new StartPositionAlreadyPinnedException(null, READ_AT_REGISTRATION, READ_BACK))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("subscriptionId");
    }
}
