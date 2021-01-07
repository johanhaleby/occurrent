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
package org.occurrent.example.domain.uno.es.spring.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException
import org.occurrent.eventstore.api.blocking.EventStoreQueries
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore
import org.occurrent.example.domain.uno.Event
import org.occurrent.example.domain.uno.GameId
import org.occurrent.example.domain.uno.es.UnoCloudEventConverter
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.subscription.api.blocking.SubscriptionModel
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel
import org.occurrent.subscription.redis.spring.blocking.SpringRedisSubscriptionPositionStorage
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.redis.core.RedisOperations
import org.springframework.retry.RetryOperations
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.EnableRetry
import org.springframework.retry.annotation.Retryable
import org.springframework.retry.backoff.ExponentialBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.occurrent.application.service.blocking.ApplicationService as OccurrentApplicationService


inline fun <reified T : Any> loggerFor(): Logger = LoggerFactory.getLogger(T::class.java)

/**
 * Bootstrap the application
 */
@SpringBootApplication
@EnableRetry
class Bootstrap {
    companion object {
        private const val EVENTS_COLLECTION_NAME = "events"
    }

    @Bean
    fun transactionManager(dbFactory: MongoDatabaseFactory) = MongoTransactionManager(dbFactory)

    @Bean
    fun eventStore(template: MongoTemplate, transactionManager: MongoTransactionManager): SpringMongoEventStore {
        val eventStoreConfig = EventStoreConfig.Builder().eventStoreCollectionName(EVENTS_COLLECTION_NAME).transactionConfig(transactionManager)
            .timeRepresentation(TimeRepresentation.DATE).build()
        return SpringMongoEventStore(template, eventStoreConfig)
    }

    @Bean
    fun subscriptionStorage(redisOps: RedisOperations<String, String>): SubscriptionPositionStorage =
        SpringRedisSubscriptionPositionStorage(redisOps)

    @Bean
    fun durableSubscriptionModel(storage: SubscriptionPositionStorage, mongoTemplate: MongoTemplate, eventStoreQueries: EventStoreQueries): SubscriptionModel {
        val subscriptionModel = SpringMongoSubscriptionModel(mongoTemplate, EVENTS_COLLECTION_NAME, TimeRepresentation.DATE)
        return DurableSubscriptionModel(subscriptionModel, storage)
    }

    @Bean
    fun objectMapper() = jacksonObjectMapper()

    @Bean
    fun cloudEventConverter(objectMapper: ObjectMapper): CloudEventConverter<Event> = UnoCloudEventConverter(objectMapper)

    @Bean
    fun genericApplicationService(eventStore: SpringMongoEventStore, eventConverter: CloudEventConverter<Event>): OccurrentApplicationService<Event> =
        GenericApplicationService(eventStore, eventConverter)

    @Bean
    fun retryOperations(): RetryOperations {
        val backOffPolicy = ExponentialBackOffPolicy().apply {
            initialInterval = 100
            maxInterval = 1000
            multiplier = 2.0
        }
        val retryPolicy = SimpleRetryPolicy().apply {
            maxAttempts = Int.MAX_VALUE
        }
        return RetryTemplate().apply {
            setBackOffPolicy(backOffPolicy)
            setRetryPolicy(retryPolicy)
        }
    }
}

@Service
class UnoApplicationService(private val applicationService: OccurrentApplicationService<Event>) {

    @Transactional
    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    fun execute(gameId: GameId, domainFunction: (Sequence<Event>) -> (Sequence<Event>)) =
        applicationService.execute(gameId.toString(), domainFunction)
}

/**
 * (Forward-) Compose two functions by piping the result of the first as input to the second
 */
infix fun <P, R1, R2> Function1<P, R1>.andThen(f: (R1) -> R2): (P) -> R2 = { p: P ->
    f(this(p))
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}