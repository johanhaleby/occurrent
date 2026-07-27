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

package org.occurrent.dsl.projection.reactor

import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.ConnectionString
import com.mongodb.reactivestreams.client.MongoClients
import io.cloudevents.CloudEvent
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.awaitility.Durations.FIVE_SECONDS
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions
import org.occurrent.dsl.projection.DcbProjection
import org.occurrent.dsl.projection.Projection
import org.occurrent.eventstore.api.EventStoreCapability
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension
import org.springframework.data.mongodb.ReactiveMongoTransactionManager
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import reactor.core.publisher.Mono
import java.net.URI
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Proves the Kotlin `DcbSubscriptions<E>.project` metadata-carrying overload (the sibling of the plain `(E) -> Mono<Void>`
 * one) threads real DCB delivery metadata into the update function, mirroring
 * [ReactiveDcbProjectionRunnerTest.the_bifunction_overload_exposes_real_dcb_metadata_to_the_update_function] for the Java
 * class-based runner. Uses a real DCB-capable [ReactorMongoEventStore] (Testcontainers), since the metadata only exists
 * once an event has actually round-tripped through a store.
 */
@Timeout(30)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbSubscriptionsProjectionExtensionsTest {

    companion object {
        @Container
        @JvmStatic
        private val mongoDBContainer = MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet().withReuse(true)
    }

    @RegisterExtension
    val flush = FlushMongoDBExtension(ConnectionString(mongoDBContainer.getReplicaSetUrl("projectiondcbextensions")))

    private lateinit var eventStore: ReactorMongoEventStore
    private lateinit var subscriptionModel: ReactorMongoSubscriptionModel
    private lateinit var converter: CloudEventConverter<DomainEvent>
    private lateinit var dcbSubscriptions: DcbSubscriptions<DomainEvent>

    @BeforeEach
    fun createInstances() {
        val connectionString = ConnectionString(mongoDBContainer.getReplicaSetUrl("projectiondcbextensions"))
        val mongoClient = MongoClients.create(connectionString)
        val mongoTemplate = ReactiveMongoTemplate(mongoClient, connectionString.database!!)
        val tx = ReactiveMongoTransactionManager(SimpleReactiveMongoDatabaseFactory(mongoClient, connectionString.database!!))
        val config = EventStoreConfig.Builder()
            .eventStoreCollectionName("events")
            .transactionConfig(tx)
            .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
            .eventStoreCapabilities(EventStoreCapability.STREAM, EventStoreCapability.DCB)
            .build()
        eventStore = ReactorMongoEventStore(mongoTemplate, config)
        subscriptionModel = ReactorMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING)
        converter = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        dcbSubscriptions = DcbSubscriptions(subscriptionModel, converter)
    }

    @Test
    fun the_metadata_carrying_overload_exposes_real_dcb_metadata_to_the_update_function() {
        val repo = ConcurrentHashMap<String, Long>()
        // The DcbProjection's own fold is irrelevant to this overload (it is never folded through); only the criteria
        // is used to scope the subscription.
        val dcbProjection: DcbProjection<Long, DomainEvent, String> = DcbProjection(
            Projection.builder<Long, DomainEvent, String>(0L).id { "singleton" }.build(),
            DcbCriteria.tags(Tag.parse("entity:dave"))
        )

        // getStreamVersion() throws on EventMetadata.empty(), so this only passes if real DCB metadata reaches the
        // update function instead of the plain, metadata-less subscribe path.
        // Named, because startAt is the last parameter and is itself a functional interface, so a trailing lambda binds
        // there rather than to update.
        val subscription = dcbSubscriptions.project("dave-projection", dcbProjection, update = { metadata, _ ->
            repo[metadata.streamId] = metadata.streamVersion
            Mono.empty()
        })
        subscription.waitUntilStarted().block()

        appendTagged(NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "dave", "Dave"), "entity:dave")

        await().atMost(FIVE_SECONDS).untilAsserted {
            assertThat(repo).hasSize(1)
            assertThat(repo.values).containsExactly(1L)
        }
    }

    private fun appendTagged(event: DomainEvent, tag: String) {
        val cloudEvent: CloudEvent = converter.toCloudEvent(event)
        eventStore.append(listOf(DcbCloudEvents.withTags(cloudEvent, listOf(Tag.parse(tag))))).block()
    }
}
