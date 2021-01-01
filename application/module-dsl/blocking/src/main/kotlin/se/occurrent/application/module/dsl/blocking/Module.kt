/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.blockurrent

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.application.subscription.dsl.blocking.Subscriptions
import org.occurrent.eventstore.api.blocking.EventStore
import org.occurrent.subscription.api.blocking.SubscriptionModel
import kotlin.reflect.KClass

/**
 * DSL marker annotation which is used to limit callers so that they will not have implicit access to multiple receivers whose classes are in the set of annotated classes.
 */
@DslMarker
@Target(AnnotationTarget.TYPE, AnnotationTarget.CLASS)
internal annotation class ModuleDSL

fun <C : Any, E : Any> module(cloudEventConverter: CloudEventConverter<E>, es: EventStore, eventNameFromType: (KClass<out E>) -> String = { e -> e.simpleName!! }, b: (@ModuleDSL ModuleBuilder<C, E>).() -> Unit): Module<C> {
    val commandDispatcher = Commands<C, E>(GenericApplicationService(es, cloudEventConverter))
    ModuleBuilder(cloudEventConverter, commandDispatcher, eventNameFromType).apply(b)

    return object : Module<C> {
        override fun dispatch(vararg commands: C) {
            commands.forEach { command ->
                val function = commandDispatcher.dispatchers[command::class] ?: throw IllegalArgumentException("No command dispatcher registered for ${command::class.qualifiedName}")
                function(command)
            }
        }
    }
}

@ModuleDSL
class ModuleBuilder<C : Any, E : Any> internal constructor(private val cloudEventConverter: CloudEventConverter<E>, private val commandBuilder: Commands<C, E>,
                                                           private val eventNameFromType: (KClass<out E>) -> String) {
    fun commands(commands: (@ModuleDSL Commands<C, E>).() -> Unit) {
        commandBuilder.apply(commands)
    }

    fun subscriptions(subscriptionModel: SubscriptionModel, subscriptions: (@ModuleDSL Subscriptions<E>).() -> Unit) {
        Subscriptions(subscriptionModel, cloudEventConverter, eventNameFromType).apply(subscriptions)
    }
}

@ModuleDSL
class Commands<C : Any, E> internal constructor(val applicationService: ApplicationService<E>) {
    val dispatchers = mutableMapOf<KClass<out C>, (C) -> Unit>()

    inline fun <reified CMD : C> command(crossinline commandHandler: (CMD) -> Unit) {
        dispatchers[CMD::class] = { cmd -> commandHandler(cmd as CMD) }
    }

    inline fun <reified CMD : C> command(crossinline streamIdGetter: (CMD) -> String, crossinline commandHandler: (Sequence<E>, CMD) -> Sequence<E>) {
        val dispatcherFn: (CMD) -> Unit = { command ->
            val streamId = streamIdGetter(command)
            applicationService.execute(streamId) { e: Sequence<E> ->
                commandHandler(e, command)
            }
        }

        dispatchers[CMD::class] = { cmd -> dispatcherFn(cmd as CMD) }
    }
}

interface Module<C : Any> {
    fun dispatch(vararg commands: C)
}


@JvmName("listCommand")
inline fun <C : Any, E : Any, reified CMD : C> Commands<C, E>.command(crossinline streamIdGetter: (CMD) -> String, crossinline commandHandler: (List<E>, CMD) -> List<E>) {
    command(streamIdGetter) { eventSeq, cmd ->
        commandHandler(eventSeq.toList(), cmd).asSequence()
    }
}