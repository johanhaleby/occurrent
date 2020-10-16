package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import io.cloudevents.CloudEvent
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException
import org.occurrent.eventstore.api.blocking.EventStore
import org.occurrent.eventstore.api.blocking.EventStream
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import java.util.*
import kotlin.streams.asSequence
import kotlin.streams.asStream

@Service
class GenericApplicationService constructor(private val eventStore: EventStore,
                                            private val cloudEventConverter: CloudEventConverter) {

    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    fun execute(streamId: UUID, functionThatCallsDomainModel: (Sequence<DomainEvent>) -> Sequence<DomainEvent>) {
        // Read all events from the event store for a particular stream
        val eventStream: EventStream<CloudEvent> = eventStore.read(streamId.toString())
        // Convert the cloud events into domain events
        val domainEventsInStream: Sequence<DomainEvent> = eventStream.events().map(cloudEventConverter::toDomainEvent).asSequence()

        // Call a pure function from the domain model which returns a sequence of domain events
        val newDomainEvents = functionThatCallsDomainModel(domainEventsInStream)

        // Convert domain events to cloud events and write them to the event store
        eventStore.write(streamId.toString(), eventStream.version(), newDomainEvents.map(cloudEventConverter::toCloudEvent).asStream())
    }
}