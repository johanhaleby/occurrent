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

package org.occurrent.dsl.query.reactor

import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.ConnectionString
import com.mongodb.reactivestreams.client.MongoClients
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.application.composition.command.composeCommands
import org.occurrent.application.composition.command.partial
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.application.service.reactor.ApplicationService
import org.occurrent.application.service.reactor.executeList
import org.occurrent.application.service.reactor.generic.GenericApplicationService
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.eventstore.api.SortBy
import org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING
import org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore
import org.occurrent.filter.Filter
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension
import org.springframework.data.mongodb.ReactiveMongoTransactionManager
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import java.net.URI
import java.time.LocalDateTime

@Testcontainers
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class DomainEventQueriesKotlinTest {

    private lateinit var applicationService: ApplicationService<DomainEvent>
    private lateinit var domainEventQueries: DomainEventQueries<DomainEvent>

    @RegisterExtension
    val flush = FlushMongoDBExtension(ConnectionString(mongoDBContainer.replicaSetUrl + ".querydslkotlin"))

    @BeforeEach
    fun createInstances() {
        val connectionString = ConnectionString(mongoDBContainer.replicaSetUrl + ".querydslkotlin")
        val mongoClient = MongoClients.create(connectionString)
        val mongoTemplate = ReactiveMongoTemplate(mongoClient, connectionString.database!!)
        val tx = ReactiveMongoTransactionManager(SimpleReactiveMongoDatabaseFactory(mongoClient, connectionString.database!!))
        val config = EventStoreConfig.Builder()
            .eventStoreCollectionName("events")
            .transactionConfig(tx)
            .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
            .build()
        val eventStore = ReactorMongoEventStore(mongoTemplate, config)
        val cloudEventConverter: CloudEventConverter<DomainEvent> = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        applicationService = GenericApplicationService(eventStore, cloudEventConverter)
        domainEventQueries = DomainEventQueries(eventStore, cloudEventConverter)
    }

    @Test
    fun queryForList() {
        // Given
        val time = LocalDateTime.now()
        applicationService.executeList(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe")
            )
        ).block()

        // When
        val events = domainEventQueries.queryForList(skip = 1).block()

        // Then
        assertAll(
            { assertThat(events).hasSize(1) },
            { assertThat(events!!.first()).isEqualTo(NameWasChanged("eventId2", time, "name", "Jane Doe")) }
        )
    }

    @Test
    fun queryForListWithFilterAndSortBy() {
        // Given
        val time = LocalDateTime.now()
        applicationService.executeList(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe")
            )
        ).block()

        // When
        val events = domainEventQueries.queryForList(Filter.type(NameWasChanged::class.qualifiedName), SortBy.natural(ASCENDING)).block()

        // Then
        assertAll(
            { assertThat(events).hasSize(1) },
            { assertThat(events!!.first()).isEqualTo(NameWasChanged("eventId2", time, "name", "Jane Doe")) }
        )
    }

    @Test
    fun querySingleWithSpecificReifiedKClassType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.executeList(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe")
            )
        ).block()

        // When
        val event = domainEventQueries.queryOne<NameWasChanged>().block()

        // Then
        assertThat(event).isEqualTo(NameWasChanged("eventId2", time, "name", "Jane Doe"))
    }

    @Test
    fun querySingleWithSpecificKClassType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.executeList(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe")
            )
        ).block()

        // When
        val event = domainEventQueries.queryOne(NameWasChanged::class).block()

        // Then
        assertThat(event).isEqualTo(NameWasChanged("eventId2", time, "name", "Jane Doe"))
    }

    @Test
    fun queryWithKClassType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.executeList(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe")
            )
        ).block()

        // When
        val events = domainEventQueries.query(NameWasChanged::class).collectList().block()

        // Then
        assertAll(
            { assertThat(events).hasSize(1) },
            { assertThat(events!!.first()).isEqualTo(NameWasChanged("eventId2", time, "name", "Jane Doe")) }
        )
    }

    @Test
    fun queryWithMultipleKClassType() {
        // Given
        val time = LocalDateTime.now()
        applicationService.executeList(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe")
            )
        ).block()

        // When
        val events: List<DomainEvent> = domainEventQueries.query(NameDefined::class, NameWasChanged::class).collectList().block()!!

        // Then
        assertAll(
            { assertThat(events).hasSize(2) },
            { assertThat(events.map { it.eventId() }).containsOnly("eventId1", "eventId2") }
        )
    }

    @Test
    fun queryOneBasedOnReifiedClassTypeAndSortBy() {
        // Given
        val time = LocalDateTime.now()
        applicationService.executeList(
            "stream", composeCommands(
                Name::defineName.partial("eventId1", time, "name", "Some Doe"),
                Name::changeName.partial("eventId2", time, "name", "Jane Doe"),
                Name::changeName.partial("eventId3", time, "name", "Jane Doe2"),
            )
        ).block()

        // When
        val nameWasChanged = domainEventQueries.queryOne<NameWasChanged>(sortBy = SortBy.natural(DESCENDING)).block()

        // Then
        assertThat(nameWasChanged).isEqualTo(NameWasChanged("eventId3", time, "name", "Jane Doe2"))
    }

    companion object {
        @Container
        @JvmStatic
        private val mongoDBContainer = MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet().withReuse(true)
    }
}
