package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.application

import com.fasterxml.jackson.databind.ObjectMapper
import io.cloudevents.CloudEvent
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import java.net.URI

class CloudEventConverter(val objectMapper: ObjectMapper, cloudEventSource: URI) {
    fun toCloudEvent(domainEvent: DomainEvent): CloudEvent = TODO()
    fun toDomainEvent(cloudEvent: CloudEvent): DomainEvent = TODO()
}