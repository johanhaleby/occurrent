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

package org.occurrent.subscription.reactor.durable;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelShutdownTest {

    @Test
    void subscribing_after_the_model_is_shut_down_throws_subscription_model_shutdown_exception() {
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("global"), new InMemoryCheckpointStorage());
        model.shutdown();

        assertThatThrownBy(() -> model.subscribe("someSubscription", __ -> Mono.empty()))
                .isExactlyInstanceOf(SubscriptionModelShutdownException.class);
    }
}
