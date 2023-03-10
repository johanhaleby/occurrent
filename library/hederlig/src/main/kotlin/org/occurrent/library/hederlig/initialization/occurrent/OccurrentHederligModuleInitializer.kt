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

package org.occurrent.library.hederlig.initialization.occurrent

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.query.blocking.queryForSequence
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.library.hederlig.CommandContext
import org.occurrent.library.hederlig.Module
import org.occurrent.library.hederlig.QueryContext
import org.occurrent.library.hederlig.initialization.Handlers
import org.occurrent.library.hederlig.initialization.HederligModuleInitializer
import org.occurrent.library.hederlig.model.Delay
import org.occurrent.library.hederlig.model.Query
import kotlin.reflect.KClass


class OccurrentHederligModuleInitializer<C : Any, E : Any, Q : Query<out Any>>(
    private val applicationService: ApplicationService<E>,
    private val subscriptionDSL: Subscriptions<E>,
    private val queriesDSL: DomainEventQueries<E>
) : HederligModuleInitializer<C, E, Q> {

    override fun initialize(handlers: Handlers<C, E, Q>): Module<C, E, Any, Q> {
        return OccurrentModule(applicationService, subscriptionDSL, queriesDSL, handlers)
    }
}

class OccurrentModule<C : Any, E : Any, Q : Query<out Any>>(
    private val applicationService: ApplicationService<E>,
    private val subscriptionDSL: Subscriptions<E>,
    private val queriesDSL: DomainEventQueries<E>,
    private val handlers: Handlers<C, E, Q>
) : Module<C, E, Any, Q> {

    init {
        handlers.subscriptionHandlers.forEach { subscriptionHandler ->
            subscriptionDSL.subscribe(
                subscriptionId = subscriptionHandler.type.simpleName!!,
                eventType = subscriptionHandler.type.java
            ) { e ->
                val commandsToPublish = mutableListOf<C>()
                subscriptionHandler.fn(e, object : CommandContext<C> {
                    override fun publish(command: C) {
                        commandsToPublish + command
                    }

                    override fun publish(command: C, delay: Delay) {
                        TODO("Not yet implemented")
                    }
                })
                // TODO Async support
                commandsToPublish.forEach(::publish)
            }
        }
    }


    override fun publish(c: C) {
        val commandHandler = handlers.cmds.find { it.type == c::class } ?: throw IllegalArgumentException("Cannot find a command handler for type ${c::class.qualifiedName}")
        val streamId = commandHandler.id(c)
        applicationService.execute(streamId) { events: List<E> ->
            commandHandler.fn(events, c)
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <R : Any?, QUERY : Query<R>> query(q: QUERY): R {
        val queryHandler = handlers.queries.find { it.type == q::class } ?: throw IllegalArgumentException("Cannot find a query handler for type ${q::class.qualifiedName}")

        val result = queryHandler.fn(q as Q, object : QueryContext<E> {
            override fun <EVENT : E> queryForSequence(type: KClass<EVENT>): Sequence<EVENT> = queriesDSL.queryForSequence(type)
        })

        return result as R
    }
}