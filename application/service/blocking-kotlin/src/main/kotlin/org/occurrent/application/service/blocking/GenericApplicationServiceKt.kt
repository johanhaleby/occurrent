package org.occurrent.application.service.blocking

import java.util.*
import kotlin.streams.asSequence
import kotlin.streams.asStream


/**
 * Extension function to [GenericApplicationService] that allows working with Kotlin sequences
 */
fun <T> GenericApplicationService<T>.execute(streamId: UUID, functionThatCallsDomainModel: (Sequence<T>) -> Sequence<T>) = execute(streamId.toString(), functionThatCallsDomainModel)

/**
 * Extension function to [GenericApplicationService] that allows working with Kotlin sequences
 */
fun <T> GenericApplicationService<T>.execute(streamId: String, functionThatCallsDomainModel: (Sequence<T>) -> Sequence<T>) =
        execute(streamId) { streamOfEvents ->
            functionThatCallsDomainModel(streamOfEvents.asSequence()).asStream()
        }