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
}
