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

package org.occurrent.library.hederlig.initialization

import org.occurrent.library.hederlig.CommandContext
import org.occurrent.library.hederlig.Module
import org.occurrent.library.hederlig.Query
import org.occurrent.library.hederlig.QueryContext
import kotlin.reflect.KClass

data class CommandHandler<C : Any, E : Any>(
    val id: (C) -> String, val type: KClass<C>, val fn: (List<E>, C) -> List<E>
)

data class QueryHandler<Q : Any, E : Any>(
    val type: KClass<Q>, val fn: (Q, QueryContext<E>) -> Any?
)

data class SubscriptionHandler<C : Any, E : Any>(
    val type: KClass<E>,
    val fn: (E, CommandContext<C>) -> Unit
)


data class Handlers<C : Any, E : Any, Q : Any>(val cmds: List<CommandHandler<C, E>>, val queries: List<QueryHandler<Q, E>>, val subscriptionHandlers: List<SubscriptionHandler<C, E>>)


interface HederligModuleInitializer<C : Any, E : Any, Q : Query<Any>> {
    fun initialize(handlers: Handlers<C, E, Q>): Module<C, E, Any, Q>
}