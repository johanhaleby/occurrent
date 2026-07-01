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

package org.occurrent.dsl.dcb.reactor

import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.ConnectionString
import com.mongodb.reactivestreams.client.MongoClients
import io.cloudevents.CloudEvent
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.awaitility.Durations.FIVE_SECONDS
import org.junit.jupiter.api.AfterEach
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
import org.occurrent.eventstore.api.EventStoreCapability.DCB
import org.occurrent.eventstore.api.EventStoreCapability.STREAM
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbQuery
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
import reactor.core.Disposable
import java.net.URI
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.stream.Stream

@Timeout(30)
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbReactorSubscriptionsTest {

    private lateinit var eventStore: ReactorMongoEventStore
    private lateinit var subscriptionModel: ReactorMongoSubscriptionModel
    private lateinit var converter: CloudEventConverter<DomainEvent>
    private lateinit var dcbSubscriptions: DcbSubscriptions<DomainEvent>
    private lateinit var time: LocalDateTime
    private val disposables = CopyOnWriteArrayList<Disposable>()

    @RegisterExtension
    val flush = FlushMongoDBExtension(ConnectionString(mongoDBContainer.replicaSetUrl + ".dcbreactorsub"))

    @BeforeEach
    fun create_instances() {
        val connectionString = ConnectionString(mongoDBContainer.replicaSetUrl + ".dcbreactorsub")
        val mongoClient = MongoClients.create(connectionString)
        val mongoTemplate = ReactiveMongoTemplate(mongoClient, connectionString.database!!)
        val transactionManager = ReactiveMongoTransactionManager(SimpleReactiveMongoDatabaseFactory(mongoClient, connectionString.database!!))
        val config = EventStoreConfig.Builder()
            .eventStoreCollectionName("events")
            .transactionConfig(transactionManager)
            .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
            .eventStoreCapabilities(STREAM, DCB)
            .build()
        eventStore = ReactorMongoEventStore(mongoTemplate, config)
        subscriptionModel = ReactorMongoSubscriptionModel(mongoTemplate, "events", TimeRepresentation.RFC_3339_STRING)
        converter = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        dcbSubscriptions = DcbSubscriptions(subscriptionModel, converter)
        time = LocalDateTime.now()
    }

    @AfterEach
    fun dispose() {
        disposables.forEach(Disposable::dispose)
    }

    @Test
    fun delivers_only_events_matching_the_query() {
        val received = CopyOnWriteArrayList<DomainEvent>()
        disposables.add(dcbSubscriptions.subscribe(DcbQuery.tags("entity:alice")).doOnNext(received::add).subscribe())
        Thread.sleep(500)

        appendTagged(NameDefined(UUID.randomUUID().toString(), time, "alice", "Alice"), "entity:alice")
        appendTagged(NameDefined(UUID.randomUUID().toString(), time, "bob", "Bob"), "entity:bob")

        await().atMost(FIVE_SECONDS).untilAsserted {
            assertThat(received).extracting<String> { (it as NameDefined).name }.containsExactly("Alice")
        }
    }

    @Test
    fun exposes_dcb_metadata() {
        val received = CopyOnWriteArrayList<DcbEvent<DomainEvent>>()
        disposables.add(dcbSubscriptions.subscribeWithMetadata(DcbQuery.tags("entity:carol")).doOnNext(received::add).subscribe())
        Thread.sleep(500)

        appendTagged(NameDefined(UUID.randomUUID().toString(), time, "carol", "Carol"), "entity:carol")

        await().atMost(FIVE_SECONDS).untilAsserted {
            assertThat(received).hasSize(1)
            val delivered = received[0]
            assertThat((delivered.event as NameDefined).name).isEqualTo("Carol")
            assertThat(delivered.metadata.dcbPosition().isPresent).isTrue()
            assertThat(delivered.metadata.dcbPosition().asLong).isGreaterThan(0)
            assertThat(delivered.metadata.dcbTags()).containsExactly("entity:carol")
        }
    }

    private fun appendTagged(event: DomainEvent, tag: String) {
        val cloudEvent: CloudEvent = converter.toCloudEvents(Stream.of(event)).toList()[0]
        eventStore.append(listOf(DcbCloudEvents.withTags(cloudEvent, setOf(tag)))).block()
    }

    companion object {
        @Container
        @JvmStatic
        val mongoDBContainer = MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
            .withReplicaSet()
            .withReuse(true)
    }
}
