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

package org.occurrent.subscription.blocking.durable.catchup;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CancelledSubscription} stands for a subscription that was cancelled before its replay reached a live
 * delegate. There is no live delegate, and nothing will ever start it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CancelledSubscriptionTest {

    @Test
    void waiting_on_it_answers_false_immediately_rather_than_true() {
        CancelledSubscription subscription = new CancelledSubscription("sub");

        boolean started = subscription.waitUntilStarted(Duration.ofSeconds(5));

        assertThat(started)
                .as("nothing left to start is not the same as having started, and a caller that acts on true would be wrong")
                .isFalse();
    }

    @Test
    void the_id_is_the_one_it_was_created_for() {
        CancelledSubscription subscription = new CancelledSubscription("sub");

        assertThat(subscription.id()).isEqualTo("sub");
    }
}
