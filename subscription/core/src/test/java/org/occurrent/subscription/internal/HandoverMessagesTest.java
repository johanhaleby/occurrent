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

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class HandoverMessagesTest {

    @Test
    void the_own_delivery_refusal_of_a_projection_feed_points_at_accept_and_wait() {
        assertThat(HandoverMessages.acceptedOrDeferredFromOwnDelivery("projection feed"))
                .endsWith("From inside its fold, call acceptAndWait(..) instead.");
    }

    @Test
    void the_own_delivery_refusal_of_a_subscription_does_not_point_at_accept_and_wait() {
        assertThat(HandoverMessages.acceptedOrDeferredFromOwnDelivery("subscription"))
                .doesNotContain("acceptAndWait")
                .endsWith("Feed the event from code this subscription does not run.");
    }

    @Test
    void the_refusal_after_a_failed_catch_up_says_how_to_change_what_is_stored_when_several_instances_run_it() {
        assertThat(HandoverMessages.catchUpFailed("subscription")).endsWith(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT);
        assertThat(HandoverMessages.catchUpFailed("projection feed")).endsWith(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    @Test
    void the_refusal_after_a_catch_up_failed_on_a_non_marker_asks_for_a_cancel_only_when_nothing_else_uses_the_checkpoint() {
        for (String noun : new String[]{"subscription", "projection feed"}) {
            assertThat(HandoverMessages.catchUpFailedOnANonMarker(noun)).as(noun)
                    .startsWith("Catch-up failed for this " + noun + ", because the checkpoint stored under its id is not a catch-up marker.")
                    .contains("cancel it only when nothing else uses that checkpoint.")
                    .contains(SharedStorageMessages.NOT_A_MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)
                    .endsWith(SharedStorageMessages.NOT_A_MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT + " " + SharedStorageMessages.NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT)
                    .doesNotContain(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT);
        }
    }

    @Test
    void the_refusal_after_any_other_failed_catch_up_still_asks_for_the_marker_step_and_not_the_non_marker_one() {
        assertThat(HandoverMessages.catchUpFailed("subscription")).doesNotContain(SharedStorageMessages.NOT_A_MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    @Test
    void the_refusal_after_a_queued_event_failed_says_how_to_change_what_is_stored_when_several_instances_run_it() {
        assertThat(HandoverMessages.queuedEventFailed("subscription")).endsWith(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT);
        assertThat(HandoverMessages.queuedEventFailed("projection feed")).endsWith(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }
}
