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
class SharedStorageMessagesTest {

    @Test
    void the_guide_is_the_section_of_the_upgrade_guide_about_a_checkpoint_or_marker_several_instances_share() {
        assertThat(SharedStorageMessages.GUIDE).endsWith("/doc/migration/upgrading-to-0.35.0.md#changing-a-checkpoint-or-marker-that-several-instances-share");
    }

    @Test
    void the_step_for_a_checkpoint_links_the_guide_and_says_to_stop_every_instance_and_start_every_instance_again_with_the_same_start() {
        assertThat(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .startsWith("When more than one instance runs the subscription, stop every instance,")
                .contains("delete its stored checkpoint or replace it with the checkpoint to start from")
                .contains("start every instance again with the same start, as " + SharedStorageMessages.GUIDE + " describes. " + SharedStorageMessages.START_AT_THE_PRESENT_AFTER_A_DELETE)
                .endsWith(" describes. " + SharedStorageMessages.START_AT_THE_PRESENT_AFTER_A_DELETE);
    }

    @Test
    void the_step_for_a_delete_that_is_conditional_or_one_of_several_choices_says_how_to_delete_and_never_tells_anyone_to_delete() {
        assertThat(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .isEqualTo("To delete the stored checkpoint when more than one instance runs the subscription,"
                        + " stop every instance, delete it or replace it with the checkpoint to start from, and start every instance again with the same start, as " + SharedStorageMessages.GUIDE + " describes."
                        + " " + SharedStorageMessages.START_AT_THE_PRESENT_AFTER_A_DELETE)
                .startsWith("To delete the stored checkpoint when more than one instance runs the subscription,")
                .isNotEqualTo(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .doesNotContain("When more than one instance runs the subscription, stop every instance,");
    }

    @Test
    void the_sentence_about_a_delete_names_a_start_from_now_or_at_the_model_default_the_events_since_the_checkpoint_and_that_the_first_instance_starts_at_the_present() {
        assertThat(SharedStorageMessages.START_AT_THE_PRESENT_AFTER_A_DELETE).isEqualTo(
                "Deleting the checkpoint of a subscription that starts from now or at the subscription model default skips the events since that checkpoint,"
                        + " even with every instance stopped, because the first instance to start finds nothing stored and starts at the present.");
    }

    @Test
    void the_step_for_a_marker_names_the_catch_up_marker_the_checkpoint_storage_keeps_and_not_whatever_is_stored_under_the_id_and_links_the_guide() {
        assertThat(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .startsWith("When more than one instance runs it, stop every instance,")
                .contains("delete the catch-up marker the checkpoint storage keeps under its id, if it keeps one,")
                .contains("start one instance again with the same start, and start the others once it has stored the marker, as ")
                .doesNotContain("what is stored under")
                .doesNotContain("start every instance again")
                .doesNotContain(SharedStorageMessages.START_AT_THE_PRESENT_AFTER_A_DELETE)
                .endsWith(", as " + SharedStorageMessages.GUIDE + " describes.");
    }

    @Test
    void the_step_for_a_checkpoint_that_is_not_a_marker_keeps_the_condition_that_nothing_else_uses_it_and_never_asks_for_an_unconditional_delete() {
        assertThat(SharedStorageMessages.NOT_A_MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .isEqualTo("When nothing else uses that checkpoint and more than one instance runs the subscription, stop every instance, delete the checkpoint,"
                        + " start one instance again with the same start, and start the others once it has stored its catch-up marker, as " + SharedStorageMessages.GUIDE + " describes.")
                .doesNotContain("start every instance again")
                .startsWith("When nothing else uses that checkpoint and more than one instance runs the subscription, stop every instance,")
                .isNotEqualTo(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .doesNotContain(SharedStorageMessages.START_AT_THE_PRESENT_AFTER_A_DELETE);
    }

    @Test
    void the_step_for_a_new_id_says_it_repeats_what_the_handler_does_from_the_start_of_the_history_and_that_stopping_the_old_instances_does_not_prevent_it_and_names_no_safe_case() {
        assertThat(SharedStorageMessages.NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .startsWith("Under a new id the subscription starts from the beginning of the history and repeats everything its handler does, the records it writes, the commands it dispatches and the messages it sends.")
                .contains(" Stop every instance on the old id before the first one on the new id starts.")
                .contains(" Stopping them doesn't prevent the repeats.")
                .contains(" With more than one instance, what else a new id needs depends on")
                .contains("where each instance keeps what it builds")
                .contains("which broker queue or consumer group it consumes")
                .endsWith(", as " + SharedStorageMessages.GUIDE + " describes.")
                .doesNotContain("safe", "Otherwise", "is enough", "records that no instance on the old id writes");
    }
}
