/*
 *
 *  Copyright 2023 Johan Haleby
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

package org.occurrent.dsl.view

import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.api.blocking.Subscription

inline fun <reified E : Any> Subscriptions<E>.updateView(viewName: String, startAt: StartAt? = null, crossinline updateFunction: (E) -> Unit): Subscription {
    val eventTypes: List<Class<out E>> = if (E::class.isSealed) {
        E::class.sealedSubclasses.map { it.java }.toList()
    } else {
        listOf(E::class.java)
    }
    return subscribe(viewName, eventTypes = eventTypes, startAt = startAt, fn = { e ->
        updateFunction(e)
    })
}

inline fun <reified E : Any> Subscriptions<E>.updateView(viewName: String, materializedView: MaterializedView<E>, startAt: StartAt? = null): Subscription =
    updateView(viewName, startAt) { e ->
        materializedView.update(e)
    }