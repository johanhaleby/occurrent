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

package org.occurrent.example.domain.uno.es

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.mongodb.client.MongoClients
import org.occurrent.application.composition.command.partial
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore
import org.occurrent.example.domain.uno.*
import org.occurrent.example.domain.uno.Card.DigitCard
import org.occurrent.example.domain.uno.Color.*
import org.occurrent.example.domain.uno.Digit.*
import org.occurrent.filter.Filter.streamId
import org.occurrent.filter.Filter.type
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.retry.RetryStrategy
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.util.concurrent.Executors
import kotlin.system.exitProcess

private val log = LoggerFactory.getLogger("org.occurrent.example.domain.uno.es")

/**
 * Very crude Uno application example
 */
fun main() {
    log.info("Starting UNO application")
    val mongoClient = MongoClients.create("mongodb://localhost:27017")
    val database = mongoClient.getDatabase("test")

    val eventStore = MongoEventStore(mongoClient, database, database.getCollection("events"), EventStoreConfig(TimeRepresentation.DATE))
    val subscriptionModel = NativeMongoSubscriptionModel(database, "events", TimeRepresentation.DATE, Executors.newCachedThreadPool(), RetryStrategy.fixed(200))

    val objectMapper = jacksonObjectMapper()
    val cloudEventConverter = UnoCloudEventConverter(objectMapper)
    val applicationService = GenericApplicationService(eventStore, cloudEventConverter)

    subscriptionModel.subscribe("progress-tracker") { cloudEvent ->
        val domainEvent = cloudEventConverter.toDomainEvent(cloudEvent)
        val turnCount = eventStore.count(streamId(domainEvent.gameId.toString()).and(type(CardPlayed::class.type))).toInt()
        ProgressTracker.trackProgress(log::info, domainEvent, turnCount)
    }.waitUntilStarted()

    Runtime.getRuntime().addShutdownHook(Thread {
        log.info("Shutting down")
        subscriptionModel.shutdown()
        mongoClient.close()
    })

    // Simulate playing Uno
    val gameId = GameId.randomUUID()

    val commands = listOf(
            Uno::start.partial(gameId, Timestamp.now(), 4, DigitCard(Three, Red)),
            Uno::play.partial(Timestamp.now(), 0, DigitCard(Three, Blue)),
            Uno::play.partial(Timestamp.now(), 1, DigitCard(Eight, Blue)),
            Uno::play.partial(Timestamp.now(), 2, DigitCard(Eight, Yellow)),
            Uno::play.partial(Timestamp.now(), 0, DigitCard(Four, Green))
    )
    commands.forEach { command ->
        applicationService.execute(gameId, command)
    }

    sleep(1000) // Allow progress tracker some time to process all events before exiting
    exitProcess(0)
}