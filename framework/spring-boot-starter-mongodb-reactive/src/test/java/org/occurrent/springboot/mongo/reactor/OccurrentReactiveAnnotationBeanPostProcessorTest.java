/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StartupMode;
import org.occurrent.springboot.mongo.common.SubscriptionAnnotations;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive bean post-processor's start-up wait decision is the stack-neutral
 * {@link SubscriptionAnnotations#shouldWaitUntilStarted(boolean, StartupMode)} (shared verbatim with the blocking
 * stack), rather than a private copy on this class.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class OccurrentReactiveAnnotationBeanPostProcessorTest {

    @Test
    void stream_replay_defaults_to_background_only_when_history_is_replayed() {
        assertThat(SubscriptionAnnotations.shouldWaitUntilStarted(true, StartupMode.DEFAULT)).isFalse();
        assertThat(SubscriptionAnnotations.shouldWaitUntilStarted(false, StartupMode.DEFAULT)).isTrue();
        assertThat(SubscriptionAnnotations.shouldWaitUntilStarted(true, StartupMode.WAIT_UNTIL_STARTED)).isTrue();
        assertThat(SubscriptionAnnotations.shouldWaitUntilStarted(true, StartupMode.BACKGROUND)).isFalse();
    }
}
