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

package se.occurrent.application.module.dsl.blocking

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import kotlin.reflect.KClass


class ApplicationServiceCommandDispatcher<C : Any, E>(applicationService: ApplicationService<E>) : CommandDispatcher<C, ApplicationServiceCommandBuilder<C, E>> {
    private val builder = ApplicationServiceCommandBuilder<C, E>(applicationService)

    companion object {
        fun <C : Any, E> applicationService(applicationService: ApplicationService<E>): ApplicationServiceCommandDispatcher<C, E> = ApplicationServiceCommandDispatcher(applicationService)
    }

    override fun dispatch(command: C): Boolean {
        val function = builder.dispatchers[command::class]
        return if (function == null) {
            false
        } else {
            function(command)
            true
        }
    }

    override fun builder(): ApplicationServiceCommandBuilder<C, E> = builder
}

class ApplicationServiceCommandBuilder<C : Any, E>(val applicationService: ApplicationService<E>) {
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

@JvmName("listCommand")
inline fun <C : Any, E : Any, reified CMD : C> ApplicationServiceCommandBuilder<C, E>.command(
    crossinline streamIdGetter: (CMD) -> String,
    crossinline commandHandler: (List<E>, CMD) -> List<E>
) {
    command(streamIdGetter) { eventSeq, cmd ->
        commandHandler(eventSeq.toList(), cmd).asSequence()
    }
}