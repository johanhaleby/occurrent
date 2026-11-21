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
                .endsWith("start every instance again with the same start, as " + SharedStorageMessages.GUIDE + " describes.");
    }

    @Test
    void the_step_for_a_marker_links_the_guide_and_says_to_stop_every_instance_and_start_every_instance_again_with_the_same_start() {
        assertThat(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .startsWith("When more than one instance runs it, stop every instance,")
                .contains("delete what is stored under its id")
                .endsWith("start every instance again with the same start, as " + SharedStorageMessages.GUIDE + " describes.");
    }

    @Test
    void the_step_for_a_new_id_links_the_guide_and_names_where_each_instance_keeps_what_it_builds_which_queue_it_consumes_and_whether_the_old_id_still_runs() {
        assertThat(SharedStorageMessages.NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT)
                .startsWith("When more than one instance runs it, whether a new id is enough")
                .contains("where each instance keeps what it builds")
                .contains("which broker queue or consumer group it consumes")
                .contains("and whether instances on the old id still run")
                .endsWith(", as " + SharedStorageMessages.GUIDE + " describes.");
    }
}
