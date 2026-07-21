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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig.withConfig;

@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelConfigTest {

    @Test
    void batch_size_and_max_await_time_are_unset_by_default() {
        NativeMongoSubscriptionModelConfig config = withConfig();

        assertThat(config.batchSize).isNull();
        assertThat(config.maxAwaitTime).isNull();
    }

    @Test
    void batch_size_is_retained() {
        NativeMongoSubscriptionModelConfig config = withConfig().batchSize(500);

        assertThat(config.batchSize).isEqualTo(500);
    }

    @Test
    void max_await_time_is_retained() {
        NativeMongoSubscriptionModelConfig config = withConfig().maxAwaitTime(Duration.ofMillis(500));

        assertThat(config.maxAwaitTime).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void batch_size_and_max_await_time_can_be_combined() {
        NativeMongoSubscriptionModelConfig config = withConfig().batchSize(500).maxAwaitTime(Duration.ofMillis(500));

        assertThat(config.batchSize).isEqualTo(500);
        assertThat(config.maxAwaitTime).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void batch_size_throws_iae_when_zero() {
        Throwable throwable = catchThrowable(() -> withConfig().batchSize(0));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("batchSize");
    }

    @Test
    void batch_size_throws_iae_when_negative() {
        Throwable throwable = catchThrowable(() -> withConfig().batchSize(-1));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("batchSize");
    }

    @Test
    void max_await_time_throws_iae_when_zero() {
        Throwable throwable = catchThrowable(() -> withConfig().maxAwaitTime(Duration.ZERO));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxAwaitTime");
    }

    @Test
    void max_await_time_throws_iae_when_negative() {
        Throwable throwable = catchThrowable(() -> withConfig().maxAwaitTime(Duration.ofMillis(-1)));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxAwaitTime");
    }
}
