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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.occurrent.eventstore.api.blocking.EventStoreQueries
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.event.eventType
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy.GamePolicyConfiguration
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.CloudEventConverter
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.loggerFor
import org.occurrent.example.domain.wordguessinggame.policy.WhenGameWasWonThenSendEmailToWinnerPolicy
import org.occurrent.filter.Filter.type
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.subscription.OccurrentSubscriptionFilter.filter
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage
import org.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription
import org.occurrent.subscription.api.blocking.Subscription
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionForMongoDB
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionPositionStorageForMongoDB
import org.occurrent.subscription.util.blocking.BlockingSubscriptionWithAutomaticPositionPersistence
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories
import org.springframework.retry.annotation.EnableRetry
import java.net.URI

/**
 * Bootstrap the application
 */
@SpringBootApplication
@EnableMongoRepositories
@EnableRetry
@Import(GamePolicyConfiguration::class)
class Bootstrap {
    companion object {
        private const val EVENTS_COLLECTION_NAME = "events"
    }

    @Bean
    fun transactionManager(dbFactory: MongoDatabaseFactory) = MongoTransactionManager(dbFactory)

    @Bean
    fun eventStore(template: MongoTemplate, transactionManager: MongoTransactionManager): SpringBlockingMongoEventStore {
        val eventStoreConfig = EventStoreConfig.Builder().eventStoreCollectionName(EVENTS_COLLECTION_NAME).transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.DATE).build()
        return SpringBlockingMongoEventStore(template, eventStoreConfig)
    }

    @Bean
    fun subscriptionRegistry(mongoTemplate: MongoTemplate): PositionAwareBlockingSubscription =
            SpringBlockingSubscriptionForMongoDB(mongoTemplate, EVENTS_COLLECTION_NAME, TimeRepresentation.DATE)

    @Bean
    fun subscriptionStorage(mongoTemplate: MongoTemplate): BlockingSubscriptionPositionStorage =
            SpringBlockingSubscriptionPositionStorageForMongoDB(mongoTemplate, "subscriptions")

    @Bean
    fun subscriptionWithAutomaticPersistence(subscription: PositionAwareBlockingSubscription, storage: BlockingSubscriptionPositionStorage) =
            BlockingSubscriptionWithAutomaticPositionPersistence(subscription, storage)

    @Bean
    fun objectMapper() = ObjectMapper().apply { registerModule(KotlinModule()) }

    @Bean
    fun cloudEventConverter(objectMapper: ObjectMapper) = CloudEventConverter(objectMapper, URI.create("urn:occurrent:domain:wordguessinggame"))
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}