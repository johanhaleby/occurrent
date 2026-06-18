/*
 * Copyright 2026 Johan Haleby
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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService
import org.occurrent.application.service.blocking.dcb.TagGenerator
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.GameCloudEventConverter
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.dcb.GameEventTagGenerator
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionPositionStorage
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.retry.annotation.EnableRetry
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI

@SpringBootApplication
@EnableRetry
class Bootstrap : WebMvcConfigurer {
    companion object {
        private const val EVENTS_COLLECTION_NAME = "events"
        private const val SUBSCRIPTIONS_COLLECTION_NAME = "subscriptions"
    }

    override fun addViewControllers(registry: ViewControllerRegistry) {
        registry.addRedirectViewController("/", "/games")
    }

    @Bean
    fun transactionManager(dbFactory: MongoDatabaseFactory) = MongoTransactionManager(dbFactory)

    @Bean
    fun eventStore(template: MongoTemplate, transactionManager: MongoTransactionManager): SpringMongoEventStore {
        val eventStoreConfig = EventStoreConfig.Builder()
            .eventStoreCollectionName(EVENTS_COLLECTION_NAME)
            .transactionConfig(transactionManager)
            .timeRepresentation(TimeRepresentation.DATE)
            .eventStoreCapabilities(DCB)
            .build()
        return SpringMongoEventStore(template, eventStoreConfig)
    }

    @Bean
    fun springMongoSubscriptionModel(mongoTemplate: MongoTemplate): SpringMongoSubscriptionModel =
        SpringMongoSubscriptionModel(mongoTemplate, EVENTS_COLLECTION_NAME, TimeRepresentation.DATE)

    @Bean
    fun subscriptionPositionStorage(mongoTemplate: MongoTemplate): SubscriptionPositionStorage =
        SpringMongoSubscriptionPositionStorage(mongoTemplate, SUBSCRIPTIONS_COLLECTION_NAME)

    @Bean
    fun durableSubscriptionModel(subscriptionModel: SpringMongoSubscriptionModel, storage: SubscriptionPositionStorage): DurableSubscriptionModel =
        DurableSubscriptionModel(subscriptionModel, storage)

    @Bean
    fun subscriptionDsl(subscriptionModel: DurableSubscriptionModel, converter: CloudEventConverter<GameEvent>) =
        Subscriptions(subscriptionModel, converter)

    @Bean
    fun queryDsl(eventStore: SpringMongoEventStore, converter: CloudEventConverter<GameEvent>) =
        DomainEventQueries(eventStore, converter)

    @Bean
    fun objectMapper(): ObjectMapper = jacksonObjectMapper()

    @Bean
    fun cloudEventConverter(objectMapper: ObjectMapper): CloudEventConverter<GameEvent> =
        GameCloudEventConverter(
            objectMapper,
            gameSource = URI.create("urn:occurrent:domain:wordguessinggame:game"),
            wordHintSource = URI.create("urn:occurrent:domain:wordguessinggame:wordhint"),
            pointsSource = URI.create("urn:occurrent:domain:wordguessinggame:points")
        )

    @Bean
    fun gameEventTagGenerator(): TagGenerator<GameEvent> = GameEventTagGenerator()

    @Bean
    fun dcbApplicationService(
        eventStore: SpringMongoEventStore,
        eventConverter: CloudEventConverter<GameEvent>,
        tagGenerator: TagGenerator<GameEvent>
    ): DcbApplicationService<GameEvent> = GenericDcbApplicationService(eventStore, eventConverter, tagGenerator)
}

fun main(args: Array<String>) {
    runApplication<Bootstrap>(*args)
}
