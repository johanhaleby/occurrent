package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.implementation.GenericApplicationService
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import java.util.*

interface ApplicationService {
    fun execute(streamId: UUID, functionThatCallsDomainModel: (Sequence<DomainEvent>) -> Sequence<DomainEvent>)
    fun execute(streamId: String, functionThatCallsDomainModel: (Sequence<DomainEvent>) -> Sequence<DomainEvent>)
}

open class RetryableApplicationService constructor(private val applicationService: GenericApplicationService<DomainEvent>) : ApplicationService {

    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    override fun execute(streamId: UUID, functionThatCallsDomainModel: (Sequence<DomainEvent>) -> Sequence<DomainEvent>) = applicationService.execute(streamId, functionThatCallsDomainModel)

    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    override fun execute(streamId: String, functionThatCallsDomainModel: (Sequence<DomainEvent>) -> Sequence<DomainEvent>) = applicationService.execute(streamId, functionThatCallsDomainModel)
}