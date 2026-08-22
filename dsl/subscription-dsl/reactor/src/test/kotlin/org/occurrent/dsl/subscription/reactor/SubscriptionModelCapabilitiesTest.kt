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

package org.occurrent.dsl.subscription.reactor

import io.cloudevents.CloudEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.SubscriptionFilter
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions
import org.occurrent.subscription.api.reactor.SubscriptionHandle
import org.occurrent.subscription.api.reactor.SubscriptionModel
import reactor.core.publisher.Mono
import java.util.function.Function

// This stack has no SubscriptionModelWrapper, so there is no wrapper-chain case to cover, only a model that
// implements the capability directly and one that does not.
@DisplayNameGeneration(ReplaceUnderscores::class)
class SubscriptionModelCapabilitiesTest {

    @Test
    fun capability_finds_the_model_itself_when_it_directly_implements_the_requested_type() {
        val model = IntrospectableModel(setOf("orders"))

        val found = model.capability<IntrospectableSubscriptions>()

        assertThat(found).isSameAs(model)
    }

    @Test
    fun capability_is_null_when_the_model_does_not_implement_the_requested_type() {
        val found = PlainModel().capability<ReplayAwareSubscriptions>()

        assertThat(found).isNull()
    }

    @Test
    fun has_capability_is_true_when_capability_would_find_something() {
        assertThat(IntrospectableModel(setOf("orders")).hasCapability<IntrospectableSubscriptions>()).isTrue()
    }

    @Test
    fun has_capability_is_false_when_capability_would_be_empty() {
        assertThat(PlainModel().hasCapability<ReplayAwareSubscriptions>()).isFalse()
    }

    private open class PlainModel : SubscriptionModel {
        override fun subscribe(subscriptionId: String, filter: SubscriptionFilter?, startAt: StartAt, action: Function<CloudEvent, Mono<Void>>): SubscriptionHandle =
            throw UnsupportedOperationException()

        override fun cancelSubscription(subscriptionId: String) {}
        override fun stop() {}
        override fun start(resumeSubscriptionsAutomatically: Boolean) {}
        override fun isRunning(): Boolean = false
        override fun isRunning(subscriptionId: String): Boolean = false
        override fun isPaused(subscriptionId: String): Boolean = false
        override fun resumeSubscription(subscriptionId: String): SubscriptionHandle = throw UnsupportedOperationException()
        override fun pauseSubscription(subscriptionId: String) {}
    }

    private class IntrospectableModel(private val ids: Set<String>) : PlainModel(), IntrospectableSubscriptions {
        override fun subscriptionIds(): Set<String> = ids
    }
}
