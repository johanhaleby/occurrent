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

package org.occurrent.dsl.subscription.blocking

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions
import org.occurrent.subscription.api.blocking.SubscriptionModel
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel

@DisplayNameGeneration(ReplaceUnderscores::class)
class SubscriptionModelCapabilitiesTest {

    private val model = InMemorySubscriptionModel()

    @AfterEach
    fun shutdownSubscriptionModel() {
        model.shutdown()
    }

    @Test
    fun capability_finds_the_model_itself_when_it_directly_implements_the_requested_type() {
        val found = model.capability<IntrospectableSubscriptions>()

        assertThat(found).isSameAs(model)
    }

    @Test
    fun capability_unwraps_a_wrapper_chain_to_reach_the_requested_type() {
        val found = Wrapper(Wrapper(model)).capability<IntrospectableSubscriptions>()

        assertThat(found).isSameAs(model)
    }

    @Test
    fun capability_is_null_when_nothing_in_the_chain_implements_the_requested_type() {
        val found = Wrapper(model).capability<RepositionableSubscriptions>()

        assertThat(found).isNull()
    }

    @Test
    fun has_capability_is_true_when_capability_would_find_something() {
        assertThat(Wrapper(model).hasCapability<IntrospectableSubscriptions>()).isTrue()
    }

    @Test
    fun has_capability_is_false_when_capability_would_be_empty() {
        assertThat(Wrapper(model).hasCapability<RepositionableSubscriptions>()).isFalse()
    }

    // Both interfaces, the way every real wrapper in this repository is shaped.
    private class Wrapper(private val delegate: SubscriptionModel) : SubscriptionModel by delegate, SubscriptionModelWrapper {
        override fun getWrappedSubscriptionModel(): SubscriptionModel = delegate
    }
}
