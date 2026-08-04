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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries
import org.occurrent.dsl.projection.dcbProjection
import org.occurrent.dsl.query.reactor.DomainEventQueries
import org.occurrent.eventstore.api.EventStoreCapability
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer
import org.springframework.data.mongodb.ReactiveMongoTransactionManager
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.net.URI
import java.time.LocalDateTime
import java.util.Date

/**
 * Proves issue #194's tag-scoped uniqueness projection on the reactor DCB stack, via the on-demand pull path: a
 * `dcbProjection` whose read boundary is a single tag folds only the events under that tag into a boolean. Uses a real
 * DCB-capable [ReactorMongoEventStore] (Testcontainers).
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores::class)
class ReactorDcbProjectionTest {

    companion object {
        @Container
        @JvmStatic
        private val mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true)
    }

    @RegisterExtension
    val flush = FlushMongoDBExtension(ConnectionString(mongoDBContainer.replicaSetUrl + ".projectiondcb"))

    private lateinit var eventStore: ReactorMongoEventStore
    private lateinit var converter: CloudEventConverter<DomainEvent>
    private lateinit var dcbQueries: DcbDomainEventQueries<DomainEvent>

    @BeforeEach
    fun createInstances() {
        val connectionString = ConnectionString(mongoDBContainer.replicaSetUrl + ".projectiondcb")
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
        converter = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        dcbQueries = DcbDomainEventQueries(DomainEventQueries(eventStore, converter))
    }

    // Issue #194: a projection that reports whether a name has been claimed, scoped to the name's tag.
    private fun isNameClaimed(name: String) = dcbProjection<Boolean, DomainEvent, String>(initialState = false) {
        tags("name:$name")
        id { name }
        on<NameDefined> { _, _ -> true }
    }

    @Test
    fun reports_a_name_as_claimed_once_its_defining_event_is_appended_under_its_tag() {
        append("name:johan", NameDefined("e1", now(), "name", "Johan"))

        assertThat(dcbQueries.project(isNameClaimed("johan")).block()).isTrue()
    }

    @Test
    fun reports_a_name_as_unclaimed_when_no_event_exists_under_its_tag() {
        append("name:johan", NameDefined("e1", now(), "name", "Johan"))

        assertThat(dcbQueries.project(isNameClaimed("jane")).block()).isFalse()
    }

    private fun append(tag: String, vararg events: DomainEvent) {
        val cloudEvents: List<CloudEvent> = converter.toCloudEvents(events.toList()).map { DcbCloudEvents.withTags(it, listOf(Tag.parse(tag))) }
        eventStore.append(cloudEvents).block()
    }

    private fun now(): LocalDateTime = LocalDateTime.now()
}
