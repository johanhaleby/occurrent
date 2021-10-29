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

package se.occurrent.dsl.module.blocking

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.typemapper.ReflectionTypeMapper
import org.occurrent.application.typemapper.TypeMapper
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.subscription.api.blocking.SubscriptionModel

/**
 * DSL marker annotation which is used to limit callers so that they will not have implicit access to multiple receivers whose classes are in the set of annotated classes.
 */
@DslMarker
@Target(AnnotationTarget.TYPE, AnnotationTarget.CLASS)
internal annotation class ModuleDSL

fun <C : Any, E : Any> module(
    cloudEventConverter: CloudEventConverter<E>, typeMapper: TypeMapper<E> = ReflectionTypeMapper.qualified(),
    b: (@ModuleDSL ModuleBuilder<C, E>).() -> Unit
): Module<C> {
    val module = ModuleBuilder<C, E>(cloudEventConverter, typeMapper).apply(b)

    return object : Module<C> {
        override fun dispatch(vararg commands: C) {
            commands.forEach { command ->
                module.commandDispatchers.takeWhile { dispatcher -> !dispatcher.dispatch(command) }
            }
        }
    }
}

@ModuleDSL
class ModuleBuilder<C : Any, E : Any> internal constructor(private val cloudEventConverter: CloudEventConverter<E>, private val typeMapper: TypeMapper<E>) {
    internal val commandDispatchers = mutableListOf<CommandDispatcher<C, out Any>>()

    fun <B : Any> commands(commandDispatcher: CommandDispatcher<C, B>, commands: (@ModuleDSL B).() -> Unit) {
        this.commandDispatchers.add(commandDispatcher)
        commandDispatcher.builder().apply(commands)
    }

    fun commands(commandDispatcher: (@ModuleDSL C) -> Unit) {
        this.commandDispatchers.add(BasicCommandDispatcher(commandDispatcher))
    }

    fun subscriptions(subscriptionModel: SubscriptionModel, subscriptions: (@ModuleDSL Subscriptions<E>).() -> Unit) {
        Subscriptions(subscriptionModel, cloudEventConverter, typeMapper).apply(subscriptions)
    }
}

interface Module<C : Any> {
    fun dispatch(vararg commands: C)
}