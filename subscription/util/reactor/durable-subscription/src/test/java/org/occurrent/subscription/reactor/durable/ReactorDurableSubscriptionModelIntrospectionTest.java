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

import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import reactor.core.publisher.Mono;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the subscription ids this model reports. It keeps running and paused subscriptions in two separate maps, so
 * the answer is their union, and a subscription registered while the model was stopped is only in the paused one.
 * <p>
 * Uses the hand-rolled feed and storage in this package rather than MongoDB, since nothing here depends on delivery.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelIntrospectionTest {

    @Test
    void knows_nothing_before_anything_subscribes() {
        ReactorDurableSubscriptionModel model = model();

        assertThat(model.subscriptionIds()).isEmpty();
    }

    @Test
    void knows_a_running_subscription() {
        ReactorDurableSubscriptionModel model = model();
        model.subscribe("someSubscription", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(model.isRunning("someSubscription")).isTrue();
        assertThat(model.subscriptionIds()).containsExactly("someSubscription");
    }

    @Test
    void knows_a_paused_subscription_too() {
        ReactorDurableSubscriptionModel model = model();
        model.subscribe("someSubscription", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        model.pauseSubscription("someSubscription");

        assertThat(model.isPaused("someSubscription")).isTrue();
        assertThat(model.subscriptionIds()).containsExactly("someSubscription");
    }

    @Test
    void knows_a_subscription_registered_while_the_model_was_stopped() {
        ReactorDurableSubscriptionModel model = model();
        model.stop();

        model.subscribe("someSubscription", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        // Registering on a stopped model records it as paused, so a model that answered from the running map alone
        // would report nothing for a subscription that exists and will deliver once started.
        assertThat(model.isPaused("someSubscription")).isTrue();
        assertThat(model.subscriptionIds()).containsExactly("someSubscription");
    }

    @Test
    void forgets_a_cancelled_subscription() {
        ReactorDurableSubscriptionModel model = model();
        model.subscribe("someSubscription", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        model.cancelSubscription("someSubscription");

        assertThat(model.subscriptionIds()).isEmpty();
    }

    @Test
    void knows_running_and_paused_subscriptions_together() {
        ReactorDurableSubscriptionModel model = model();
        model.subscribe("running", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        model.subscribe("paused", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        model.pauseSubscription("paused");

        assertThat(model.subscriptionIds()).containsExactlyInAnyOrder("running", "paused");
    }

    @Test
    void answers_a_copy_rather_than_the_maps_it_keeps() {
        ReactorDurableSubscriptionModel model = model();
        model.subscribe("first", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        Set<String> ids = model.subscriptionIds();

        model.subscribe("second", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(ids).containsExactly("first");
        assertThat(model.subscriptionIds()).containsExactlyInAnyOrder("first", "second");
    }

    private static ReactorDurableSubscriptionModel model() {
        return new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("at-registration"), new InMemoryCheckpointStorage());
    }
}
