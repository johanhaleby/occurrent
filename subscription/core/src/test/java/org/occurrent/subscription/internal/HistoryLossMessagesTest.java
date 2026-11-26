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
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class HistoryLossMessagesTest {

    private static final Checkpoint CHECKPOINT = () -> "stored position";

    @Test
    void a_refusal_to_start_from_a_checkpoint_is_history_lost() {
        assertThat(HistoryLossMessages.isHistoryLost(HistoryLossMessages.cannotStartFrom("id", CHECKPOINT))).isTrue();
    }

    @Test
    void a_refusal_to_resume_a_subscription_is_history_lost() {
        assertThat(HistoryLossMessages.isHistoryLost(HistoryLossMessages.historyLost("id", CHECKPOINT))).isTrue();
    }

    @Test
    void a_catchup_cannot_resume_exception_made_elsewhere_is_not_history_lost() {
        assertThat(HistoryLossMessages.isHistoryLost(new CatchupCannotResumeException("The position cannot be resumed"))).isFalse();
    }

    @Test
    void a_failure_that_is_not_a_catchup_cannot_resume_exception_is_not_history_lost() {
        assertThat(HistoryLossMessages.isHistoryLost(new IllegalStateException("Cannot resume"))).isFalse();
    }

    @Test
    void the_log_for_a_subscription_paused_after_a_loss_says_how_to_change_the_checkpoint_when_several_instances_run_it() {
        assertThat(HistoryLossMessages.TERMINAL_FAILURE_LOG).contains(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT).doesNotContain(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT).contains(SharedStorageMessages.GUIDE);
    }

    @Test
    void the_log_for_a_subscription_paused_after_a_live_start_loss_says_how_to_change_the_checkpoint_when_several_instances_run_it() {
        assertThat(HistoryLossMessages.LIVE_START_LOST_LOG).contains(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT).doesNotContain(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT).contains(SharedStorageMessages.GUIDE);
    }

    @Test
    void the_logs_for_a_paused_subscription_say_it_can_be_resumed_at_a_position_only_on_a_blocking_catch_up_subscription_model_the_application_calls_directly() {
        String sentence = "until it is cancelled, or resumed at a position on a blocking catch-up subscription model the application calls directly. To get every event,";
        assertThat(HistoryLossMessages.TERMINAL_FAILURE_LOG).contains(sentence).doesNotContain("on the blocking stack resumed at a position");
        assertThat(HistoryLossMessages.LIVE_START_LOST_LOG).contains(sentence).doesNotContain("on the blocking stack resumed at a position");
    }

    @Test
    void the_log_picked_for_a_loss_says_how_to_change_the_checkpoint_when_several_instances_run_it_whether_or_not_it_was_the_live_start() {
        assertThat(HistoryLossMessages.terminalFailureLog(false)).as("a lost position").contains(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT).doesNotContain(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT);
        assertThat(HistoryLossMessages.terminalFailureLog(true)).as("a lost live start").contains(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT).doesNotContain(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    @Test
    void the_refusal_to_resume_a_subscription_says_how_to_change_the_checkpoint_when_several_instances_run_it() {
        assertThat(HistoryLossMessages.historyLost("id", CHECKPOINT)).hasMessageContaining(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT).hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    @Test
    void the_refusal_to_resume_an_unnamed_subscription_does_not_mention_a_checkpoint_several_instances_share() {
        assertThat(HistoryLossMessages.historyLost(null, CHECKPOINT)).hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT).hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    @Test
    void the_refusal_to_resume_an_unnamed_subscription_whose_live_start_was_lost_does_not_mention_a_checkpoint_several_instances_share() {
        assertThat(HistoryLossMessages.liveStartLostOnResume(null, CHECKPOINT)).hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT).hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    @Test
    void the_refusal_to_start_a_named_subscription_from_a_checkpoint_says_to_delete_a_stored_checkpoint_first_and_ends_with_the_conditional_delete_step_for_several_instances() {
        assertThat(HistoryLossMessages.cannotStartFrom("id", CHECKPOINT))
                .hasMessageContaining(" If the checkpoint is the one stored for the subscription, delete it first. Start the subscription from the beginning,")
                .hasMessageEndingWith(" " + SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    @Test
    void the_refusal_to_start_an_unnamed_subscription_from_a_checkpoint_has_no_stored_checkpoint_to_delete_and_no_shared_checkpoint_step() {
        assertThat(HistoryLossMessages.cannotStartFrom(null, CHECKPOINT))
                .hasMessageContaining("Cannot start catch-up subscription (unnamed) from checkpoint")
                .hasMessageNotContaining("delete it first")
                .hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .hasMessageEndingWith("that the subscription model still has the history back to.");
    }

    @Test
    void the_refusal_to_resume_a_subscription_whose_live_start_was_lost_says_how_to_change_the_checkpoint_when_several_instances_run_it() {
        assertThat(HistoryLossMessages.liveStartLostOnResume("id", CHECKPOINT)).hasMessageContaining(SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT).hasMessageNotContaining(SharedStorageMessages.CHECKPOINT_WHEN_SEVERAL_INSTANCES_RUN_IT);
    }

    private static final String SHARED_CHECKPOINT_DELETE_STEP = " " + SharedStorageMessages.CHECKPOINT_DELETE_WHEN_SEVERAL_INSTANCES_RUN_IT;

    @Test
    void the_log_for_a_subscription_paused_after_a_loss_says_the_cancel_deletes_any_stored_checkpoint() {
        assertThat(HistoryLossMessages.TERMINAL_FAILURE_LOG).contains("To get every event, cancel it, which deletes any checkpoint stored for it, and replay from the beginning, or subscribe again from a checkpoint the application kept that the subscription model still has the history back to."
                + SHARED_CHECKPOINT_DELETE_STEP);
    }

    @Test
    void the_log_for_a_subscription_paused_after_a_live_start_loss_says_the_cancel_deletes_any_stored_checkpoint() {
        assertThat(HistoryLossMessages.LIVE_START_LOST_LOG).contains("To get every event, cancel it, which deletes any checkpoint stored for it, and replay from the beginning, or subscribe again from a checkpoint the application kept that the subscription model still has the history back to."
                + SHARED_CHECKPOINT_DELETE_STEP);
    }

    @Test
    void the_refusal_to_resume_a_subscription_says_the_cancel_deletes_any_stored_checkpoint() {
        assertThat(HistoryLossMessages.historyLost("id", CHECKPOINT)).hasMessageContaining("Cancel it, which deletes any checkpoint stored for it, and replay from the beginning, or subscribe again from a checkpoint the application kept that the subscription model still has the history back to."
                + SHARED_CHECKPOINT_DELETE_STEP);
    }

    @Test
    void the_refusal_to_resume_a_subscription_whose_live_start_was_lost_says_the_cancel_deletes_any_stored_checkpoint() {
        assertThat(HistoryLossMessages.liveStartLostOnResume("id", CHECKPOINT)).hasMessageContaining("Cancel it, which deletes any checkpoint stored for it, and replay from the beginning, or subscribe again from a checkpoint the application kept that the subscription model still has the history back to."
                + SHARED_CHECKPOINT_DELETE_STEP);
    }

    @Test
    void the_refusal_to_resume_an_unnamed_subscription_still_says_the_cancel_deletes_any_stored_checkpoint() {
        assertThat(HistoryLossMessages.historyLost(null, CHECKPOINT)).hasMessageContaining("Cancel it, which deletes any checkpoint stored for it, and replay from the beginning, or subscribe again from a checkpoint the application kept that the subscription model still has the history back to.");
    }

    @Test
    void the_refusal_to_resume_an_unnamed_subscription_whose_live_start_was_lost_still_says_the_cancel_deletes_any_stored_checkpoint() {
        assertThat(HistoryLossMessages.liveStartLostOnResume(null, CHECKPOINT)).hasMessageContaining("Cancel it, which deletes any checkpoint stored for it, and replay from the beginning, or subscribe again from a checkpoint the application kept that the subscription model still has the history back to.");
    }
}
