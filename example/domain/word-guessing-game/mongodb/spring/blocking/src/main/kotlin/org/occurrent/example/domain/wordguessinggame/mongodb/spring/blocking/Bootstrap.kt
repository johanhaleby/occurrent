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
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.service.blocking.implementation.GenericApplicationService
import org.occurrent.eventstore.api.blocking.EventStoreQueries
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.GameCloudEventConverter
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy.SendEmailToWinnerPolicy
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.subscription.api.blocking.BlockingSubscription
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionForMongoDB
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionPositionStorageForMongoDB
import org.occurrent.subscription.util.blocking.BlockingSubscriptionWithAutomaticPositionPersistence
import org.occurrent.subscription.util.blocking.catchup.subscription.CatchupSupportingBlockingSubscription
import org.occurrent.subscription.util.blocking.catchup.subscription.CatchupSupportingBlockingSubscriptionConfig
import org.occurrent.subscription.util.blocking.catchup.subscription.SubscriptionPositionStorageConfig.useSubscriptionPositionStorage
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.retry.annotation.EnableRetry
import java.net.URI
import org.occurrent.application.service.blocking.ApplicationService as OccurrentApplicationService

/**
 * Bootstrap the application
 */
@SpringBootApplication
@EnableRetry
@Import(SendEmailToWinnerPolicy::class)
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
    fun subscriptionStorage(mongoTemplate: MongoTemplate): BlockingSubscriptionPositionStorage =
            SpringBlockingSubscriptionPositionStorageForMongoDB(mongoTemplate, "subscriptions")

    @Bean
    fun subscriptionWithAutomaticPersistence(storage: BlockingSubscriptionPositionStorage, mongoTemplate: MongoTemplate, eventStoreQueries: EventStoreQueries): BlockingSubscription {
        val subscription = SpringBlockingSubscriptionForMongoDB(mongoTemplate, EVENTS_COLLECTION_NAME, TimeRepresentation.DATE)
        val subscriptionWithAutomaticPositionPersistence = BlockingSubscriptionWithAutomaticPositionPersistence(subscription, storage)
        return CatchupSupportingBlockingSubscription(subscriptionWithAutomaticPositionPersistence, eventStoreQueries,
                CatchupSupportingBlockingSubscriptionConfig(useSubscriptionPositionStorage(storage)))
    }

    @Bean
    fun objectMapper() = ObjectMapper().apply { registerModule(KotlinModule()) }

    @Bean
    fun cloudEventConverter(objectMapper: ObjectMapper): CloudEventConverter<DomainEvent> = GameCloudEventConverter(objectMapper, URI.create("urn:occurrent:domain:wordguessinggame:game"), URI.create("urn:occurrent:domain:wordguessinggame:wordhint"), URI.create("urn:occurrent:domain:wordguessinggame:points"))

    @Bean
    fun occurrentApplicationService(eventStore: SpringBlockingMongoEventStore, eventConverter: CloudEventConverter<DomainEvent>): OccurrentApplicationService<DomainEvent> = GenericApplicationService(eventStore, eventConverter)
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}