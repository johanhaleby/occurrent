/*
 * Copyright 2021 Johan Haleby
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

package se.occurrent.dsl.module.blocking.em

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.eventstore.api.blocking.EventStore
import org.occurrent.subscription.api.blocking.SubscriptionModel
import se.occurrent.dsl.module.blocking.BasicCommandDispatcher
import se.occurrent.dsl.module.blocking.CommandDispatcher
import se.occurrent.dsl.module.blocking.Module
import se.occurrent.dsl.module.blocking.ModuleDSL
import se.occurrent.dsl.module.blocking.em.EventModelSlice.SliceDefinition
import se.occurrent.dsl.module.blocking.em.EventModelSlice.ViewDefinition.Mode.Async
import se.occurrent.dsl.module.blocking.em.EventModelSlice.ViewDefinition.Mode.Sync
import kotlin.reflect.KClass


fun <C : Any, E : Any> eventModel(
    eventStore: EventStore, cloudEventConverter: CloudEventConverter<E>, eventNameFromType: (KClass<out E>) -> String = { e -> e.simpleName!! },
    eventModelSpecification: (@ModuleDSL EventModelSpecification<C, E>).() -> Unit
): Module<C> {
    val spec = EventModelSpecification<C, E>(eventStore, cloudEventConverter, eventNameFromType).apply(eventModelSpecification)

    return object : Module<C> {
        override fun dispatch(vararg commands: C) {
            commands.forEach { command ->
                spec.commandDispatchers.takeWhile { dispatcher -> !dispatcher.dispatch(command) }
            }
        }
    }
}


@ModuleDSL
class EventModelSpecification<C : Any, E : Any> internal constructor(
    private val eventStore: EventStore,
    private val cloudEventConverter: CloudEventConverter<E>,
    private val eventNameFromType: (KClass<out E>) -> String
) {
    internal val commandDispatchers = mutableListOf<CommandDispatcher<C, out Any>>()

    fun swimlane(name: String, swimelane : SwimlaneDefinition<C, E>.() -> Unit) {
        SwimlaneDefinition<C, E>(eventStore, cloudEventConverter).apply(swimelane)
    }

    fun <B : Any> commands(commandDispatcher: CommandDispatcher<C, B>, commands: (@ModuleDSL B).() -> Unit) {
        this.commandDispatchers.add(commandDispatcher)
        commandDispatcher.builder().apply(commands)
    }

    fun commands(commandDispatcher: (@ModuleDSL C) -> Unit) {
        this.commandDispatchers.add(BasicCommandDispatcher(commandDispatcher))
    }

    fun subscriptions(subscriptionModel: SubscriptionModel, subscriptions: (@ModuleDSL Subscriptions<E>).() -> Unit) {
        Subscriptions(subscriptionModel, cloudEventConverter, eventNameFromType).apply(subscriptions)
    }
}

class SwimlaneDefinition<C : Any, E : Any> internal constructor(eventStore: EventStore, cloudEventConverter: CloudEventConverter<E>) {
    private val applicationService = GenericApplicationService(eventStore, cloudEventConverter)

    fun slice(actor : String, name: String? = null, slice: SliceDefinition<C, E>.() -> Unit) {
        SliceDefinition<C, E>(applicationService).apply(slice)
    }
}

data class EventModelCommandDispatcher<C : Any>(val commandType: KClass<out C>, val dispatcherFn: (C) -> Unit)

sealed class EventModelSlice<C, E> {

    data class SliceDefinition<C : Any, E : Any>(internal val applicationService: ApplicationService<E>) : EventModelSlice<C, E>() {
        fun wireframe(name: String): CommandDefinition<C, E> = CommandDefinition(applicationService, name)
    }

    data class CommandDefinition<C : Any, E : Any>(val applicationService: ApplicationService<E>, val wireframeName: String) : EventModelSlice<C, E>() {

        @JvmName("listCommand")
        inline fun <reified CMD : C> command(crossinline streamIdGetter: (CMD) -> String, crossinline commandHandler: (List<E>, CMD) -> List<E>): EventDefinition<C, E> {
            return command(streamIdGetter) { eventSeq: Sequence<E>, cmd ->
                commandHandler(eventSeq.toList(), cmd).asSequence()
            }
        }

        inline fun <reified CMD : C> command(crossinline streamIdGetter: (CMD) -> String, crossinline commandHandler: (Sequence<E>, CMD) -> Sequence<E>): EventDefinition<C, E> {
            val dispatcherFn: (C) -> Unit = { command ->
                command as CMD
                val streamId = streamIdGetter(command)
                applicationService.execute(streamId) { e: Sequence<E> ->
                    commandHandler(e, command)
                }
            }
            return EventDefinition(wireframeName, EventModelCommandDispatcher(CMD::class, dispatcherFn))
        }
    }

    data class EventDefinition<C : Any, E : Any>(val wireframeName: String, val commandDispatcher: EventModelCommandDispatcher<C>) : EventModelSlice<C, E>() {
        inline fun <reified EVENT : E> event(): ViewDefinition<C, EVENT> = ViewDefinition(wireframeName, commandDispatcher, EVENT::class)
    }

    data class ViewDefinition<C : Any, E : Any>(
        private val wireframeName: String,
        private val commandDispatcher: EventModelCommandDispatcher<C>, private val eventType: KClass<E>
    ) : EventModelSlice<C, E>() {
        enum class Mode {
            Sync, Async
        }

        fun updateView(viewName: String, mode: Mode = Async, fn: (E) -> Unit): Unit = when (mode) {
            Sync -> TODO()
            Async -> TODO()
        }
    }
}