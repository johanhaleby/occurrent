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

package org.occurrent.subscription.mongodb.spring.reactor;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class ReactorMongoSubscriptionModelConfigTest {

    @Test
    void backoff_accepts_max_greater_than_min() {
        ReactorMongoSubscriptionModelConfig config = ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.ofMillis(100), Duration.ofSeconds(2));

        assertThat(config).isNotNull();
    }

    @Test
    void backoff_accepts_max_equal_to_min() {
        ReactorMongoSubscriptionModelConfig config = ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.ofMillis(100), Duration.ofMillis(100));

        assertThat(config).isNotNull();
    }

    @Test
    void backoff_throws_iae_when_max_is_less_than_min() {
        Throwable throwable = catchThrowable(() -> ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.ofSeconds(2), Duration.ofMillis(100)));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxBackoff").hasMessageContaining("minBackoff");
    }

    @Test
    void backoff_throws_iae_when_min_is_zero() {
        Throwable throwable = catchThrowable(() -> ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.ZERO, Duration.ofSeconds(2)));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("minBackoff");
    }

    @Test
    void backoff_throws_iae_when_min_is_negative() {
        Throwable throwable = catchThrowable(() -> ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.ofMillis(-100), Duration.ofSeconds(2)));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("minBackoff");
    }
}
