/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.library.hederlig

import org.occurrent.library.hederlig.initialization.*
import org.occurrent.library.hederlig.model.Delay
import kotlin.reflect.KClass

typealias Query<R> = Function<R>

/**
 * DSL marker annotation which is used to limit callers so that they will not have implicit access to multiple receivers whose classes are in the set of annotated classes.
 */
@DslMarker
@Target(AnnotationTarget.TYPE, AnnotationTarget.CLASS)
internal annotation class ModuleDSL

fun <C : Any, E : Any, Q : Query<Any>> module(
    definitionBuilder: (@ModuleDSL ModuleDefinitionBuilder<C, E, Q>).() -> Unit
): ModuleDefinition<C, E, Q> {
    val module = ModuleDefinitionBuilder<C, E, Q>().apply(definitionBuilder)

    @Suppress("UNCHECKED_CAST")
    return object : ModuleDefinition<C, E, Q> {
        override fun initialize(initializer: HederligModuleInitializer<C, E, Q>): Module<C, E, Any, Q> {

            val initial = Handlers<C, E, Q>(emptyList(), emptyList(), emptyList())

            val fold = module.features.fold(initial) { handlers, feature ->
                val commandHandlers = (feature.commandWithIdDefinitions.commandHandlers + feature.commandWithoutIdDefinitions.commandHandlers)
                    .map { ch ->
                        val id: (C) -> String = ch.id as (C) -> String
                        val type = ch.type as KClass<C>
                        val fn = ch.commandHandler as (List<E>, C) -> List<E>
                        CommandHandler(id, type, fn)
                    }
                val queryHandlers = feature.queryDefinitions.queryHandlers.map { qh -> QueryHandler(qh.type as KClass<Q>, qh.fn as (Any, QueryContext<E>) -> E?) }
                val subscriptionHandlers = feature.subscriptionDefinitions.subscriptionHandlers.map { sh -> SubscriptionHandler(sh.type as KClass<E>, sh.fn as (E, CommandContext<C>) -> Unit) }

                handlers.copy(
                    cmds = handlers.cmds + commandHandlers,
                    queries = handlers.queries + queryHandlers,
                    subscriptionHandlers = handlers.subscriptionHandlers + subscriptionHandlers
                )
            }

            return initializer.initialize(fold)
        }
    }
}

@ModuleDSL
class ModuleDefinitionBuilder<C : Any, E : Any, Q : Query<Any>> internal constructor() {
    internal val features = mutableListOf<FeatureBuilder<C, E, Q>>()

    fun feature(name: String, featureBuilder: (@ModuleDSL FeatureBuilder<C, E, Q>).() -> Unit) {
        val newFeatureBuilder = FeatureBuilder<C, E, Q>(name)
        featureBuilder(newFeatureBuilder)
        features.add(newFeatureBuilder)
    }
}


@ModuleDSL
class FeatureBuilder<C : Any, E : Any, Q : Query<Any>> internal constructor(private val name: String) {
    internal val commandWithIdDefinitions = CommandWithIdDefinitions<C, E>()

    // TODO Ugly! Fix!
    internal lateinit var commandWithoutIdDefinitions: CommandWithoutIdDefinitions<C, E>
    internal val subscriptionDefinitions = SubscriptionDefinitions<C, E>()
    internal val queryDefinitions = QueryDefinitions<Q, E>()

    fun commands(commandBuilder: (@ModuleDSL CommandWithIdDefinitions<C, E>).() -> Unit) {
        commandBuilder(commandWithIdDefinitions)
    }

    fun commands(id: (C) -> String, commandBuilder: (@ModuleDSL CommandWithoutIdDefinitions<C, E>).() -> Unit) {
        commandWithoutIdDefinitions = CommandWithoutIdDefinitions(id)
        commandBuilder(commandWithoutIdDefinitions)
    }

    fun subscriptions(subscriptionBuilder: (@ModuleDSL SubscriptionDefinitions<C, E>).() -> Unit) {
        subscriptionBuilder(subscriptionDefinitions)
    }

    fun queries(queryBuilder: (@ModuleDSL QueryDefinitions<Q, E>).() -> Unit) {
        queryBuilder(queryDefinitions)
    }

    // Queries
    class QueryDefinitions<Q : Query<Any>, E : Any> internal constructor() {
        val queryHandlers = mutableListOf<QueryDefinition<out Q, out E>>()

        inline fun <reified QUERY : Q> query(noinline queryHandler: (QUERY) -> Any?) {
            query { q: QUERY, _ -> queryHandler(q) }
        }

        @JvmName("queryContext")
        inline fun <reified QUERY : Q> query(noinline queryHandler: (QueryContext<E>) -> Any?) {
            query { _: QUERY, ctx ->
                queryHandler(ctx)
            }
        }

        inline fun <reified QUERY : Q> query(noinline queryHandler: (QUERY, QueryContext<E>) -> Any?) {
            queryHandlers.add(QueryDefinition(QUERY::class, queryHandler))
        }

        // TODO Add Occurrent specific extension functions in the Occurrent Bootstrap Module that expose methods in "domain queries" interface
    }

    // Maybe change from CommandPublisher to Application/Module, because maybe one wants to issue a query as a part of a subscription?
    data class QueryDefinition<Q : Any, E : Any>(
        val type: KClass<Q>,
        val fn: (Q, QueryContext<E>) -> Any?
    )


    // Subscriptions
    class SubscriptionDefinitions<C : Any, E : Any> internal constructor() {
        val subscriptionHandlers = mutableListOf<SubscriptionDefinition<out C, out E>>()

        inline fun <reified EVENT : E> on(noinline subscriptionHandler: (EVENT, CommandContext<C>) -> Unit) {
            subscriptionHandlers.add(SubscriptionDefinition(EVENT::class, subscriptionHandler))
        }

        inline fun <reified EVENT : E> on(noinline subscriptionHandler: (EVENT) -> Unit) {
            on<EVENT> { e, _ ->
                subscriptionHandler(e)
            }
        }
    }

    // Maybe change from CommandPublisher to Application/Module, because maybe one wants to issue a query as a part of a subscription?
    data class SubscriptionDefinition<C : Any, E : Any>(
        val type: KClass<E>,
        val fn: (E, CommandContext<C>) -> Unit
    )

    // Commands
    class CommandWithIdDefinitions<C : Any, E : Any> internal constructor() {
        val commandHandlers = mutableListOf<CommandHandlerDefinition<out C, out E>>()

        inline fun <reified CMD : C> command(
            noinline id: (CMD) -> String, noinline commandHandler: (List<E>, CMD) -> List<E>
        ) {
            commandHandlers.add(CommandHandlerDefinition(CMD::class, id, commandHandler))
        }

    }

    class CommandWithoutIdDefinitions<C : Any, E : Any> internal constructor(val id: (C) -> String) {
        val commandHandlers = mutableListOf<CommandHandlerDefinition<out C, out E>>()

        inline fun <reified CMD : C> command(noinline commandHandler: (List<E>, CMD) -> List<E>) {
            commandHandlers.add(CommandHandlerDefinition(CMD::class, id, commandHandler))
        }

    }

    data class CommandHandlerDefinition<C : Any, E : Any>(
        val type: KClass<C>,
        val id: (C) -> String,
        val commandHandler: (List<E>, C) -> List<E>
    )
}

interface CommandContext<C : Any> {
    fun publish(command: C)

    // When integrating with deadline DSL we can do like this:
    // 1. Register all command types to the DeadlineRegistry on boot, regardless of whether they are used, with category "hederlig:<command type>" (we can get the type from the command definition)
    // 2. Schedule a deadline with the command and use the same category!
    fun publish(command: C, delay: Delay)
}

interface QueryContext<E : Any> {
    fun <EVENT : E> queryForSequence(type: KClass<EVENT>): Sequence<EVENT>
}

inline fun <reified EVENT : Any> QueryContext<in EVENT>.queryForSequence(): Sequence<EVENT> = queryForSequence(EVENT::class)
inline fun <reified EVENT : Any> QueryContext<in EVENT>.queryForList(): List<EVENT> = queryForSequence<EVENT>().toList()

interface ModuleDefinition<C : Any, E : Any, Q : Query<Any>> {
    fun initialize(initializer: HederligModuleInitializer<C, E, Q>): Module<C, E, Any, Q>
}

interface Module<C, E, R2, Q : Query<Any>> {
    fun <R : R2, QUERY : Query<R>> query(query: QUERY): R
    fun publish(c: C)
}