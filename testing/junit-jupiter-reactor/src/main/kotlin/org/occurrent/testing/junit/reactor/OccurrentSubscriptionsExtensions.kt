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

package org.occurrent.testing.junit.reactor

import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle

/**
 * An [OccurrentSubscriptionsExtension] over this subscription model where no subscription runs until a test asks for
 * one, so the model reads as the receiver instead of an argument:
 *
 * ```kotlin
 * @JvmField
 * @RegisterExtension
 * val subscriptions = subscriptionModel.stoppedByDefault()
 * ```
 *
 * Keep the `@JvmField`. Without it JUnit never picks the field up, so nothing is stopped and every subscription stays
 * live for the whole test.
 */
fun SubscriptionModelLifeCycle.stoppedByDefault(): OccurrentSubscriptionsExtension =
    OccurrentSubscriptionsExtension.stoppedByDefault(this)
