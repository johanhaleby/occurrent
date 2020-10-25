package org.occurrent.application.service.blocking

import java.util.*
import java.util.stream.Stream
import kotlin.streams.asSequence
import kotlin.streams.asStream

/**
 * Extension function to [ApplicationService] that allows working with Kotlin sequences
 */
fun <T> ApplicationService<T>.execute(streamId: UUID, functionThatCallsDomainModel: (Sequence<T>) -> Sequence<T>,
                                      sideEffects: ((Sequence<T>) -> Unit)? = null) =
        execute(streamId.toString(), functionThatCallsDomainModel, sideEffects)


/**
 * Extension function to [ApplicationService] that allows working with Kotlin sequences
 */
fun <T> ApplicationService<T>.execute(streamId: String, functionThatCallsDomainModel: (Sequence<T>) -> Sequence<T>,
                                      sideEffects: ((Sequence<T>) -> Unit)? = null) =
        execute(streamId, { streamOfEvents ->
            functionThatCallsDomainModel(streamOfEvents.asSequence()).asStream()
        }, sideEffects?.toStreamSideEffect())


private fun <T> ((Sequence<T>) -> Unit).toStreamSideEffect(): (Stream<T>) -> Unit {
    return { streamOfEvents -> this(streamOfEvents.asSequence()) }
}