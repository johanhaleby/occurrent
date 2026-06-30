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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.eventstore.api.EventStoreCapability.DCB
import org.occurrent.eventstore.api.EventStoreCapability.STREAM
import org.occurrent.eventstore.api.dcb.DcbAppendCondition
import org.occurrent.eventstore.api.dcb.DcbAppendResult
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbEventStream
import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.eventstore.api.dcb.DcbReadOptions
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbReactorDslTest {

    private lateinit var eventStore: ReactorMongoEventStore
    private lateinit var converter: CloudEventConverter<DomainEvent>
    private lateinit var applicationService: DcbApplicationService<DomainEvent>
    private lateinit var queries: DcbDomainEventQueries<DomainEvent>
    private lateinit var time: LocalDateTime

    @BeforeEach
    fun create_instances() {
        val connectionString = ConnectionString(mongoDBContainer.replicaSetUrl + ".dcbreactordsl")
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
        converter = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        applicationService = GenericDcbApplicationService(eventStore, converter, { event -> setOf(tagFor(event)) }, GenericDcbApplicationService.defaultRetry())
        queries = DcbDomainEventQueries(eventStore, converter)
        time = LocalDateTime.now()
    }

    @Test
    fun command_execution_appends_decider_produced_events() {
        val result = applicationService.execute(nameQuery("name"), DefineName("Jane Doe"), nameDecider()).block()

        assertThat(result).isNotNull
        assertThat(result!!.eventCount()).isEqualTo(1)
        assertThat(readNameEvents("name")).containsExactly(NameDefined("event-1", time, "name", "Jane Doe"))
    }

    @Test
    fun no_op_decisions_return_an_empty_mono() {
        append(NameDefined("event-0", time, "name", "Jane Doe"))

        val result = applicationService.execute(nameQuery("name"), DefineName("Jane Doe"), nameDecider()).block()

        assertThat(result).isNull()
        assertThat(readNameEvents("name")).containsExactly(NameDefined("event-0", time, "name", "Jane Doe"))
    }

    @Test
    fun executeAndReturnDecision_returns_folded_state_plus_new_events() {
        append(NameDefined("event-0", time, "name", "Jane Doe"))

        val decision = applicationService.executeAndReturnDecision(nameQuery("name"), ChangeName("John Doe"), nameDecider()).block()

        assertThat(decision!!.state).isEqualTo("John Doe")
        assertThat(decision.events).containsExactly(NameWasChanged("event-2", time, "name", "John Doe"))
    }

    @Test
    fun executeAndReturnEvents_returns_the_new_events() {
        append(NameDefined("event-0", time, "name", "Jane Doe"))

        val events = applicationService.executeAndReturnEvents(nameQuery("name"), ChangeName("Jane Roe"), nameDecider()).block()

        assertThat(events).containsExactly(NameWasChanged("event-2", time, "name", "Jane Roe"))
    }

    @Test
    fun query_returns_the_matching_domain_events() {
        append(NameDefined("event-0", time, "name", "Jane Doe"))

        val events = queries.query(nameQuery("name")).collectList().block()

        assertThat(events).containsExactly(NameDefined("event-0", time, "name", "Jane Doe"))
    }

    @Test
    fun queryWithPosition_returns_events_position_and_a_usable_consistency_token() {
        append(NameDefined("event-0", time, "name", "Jane Doe"))

        val stream = queries.queryWithPosition(nameQuery("name")).block()

        assertThat(stream!!.events()).containsExactly(NameDefined("event-0", time, "name", "Jane Doe"))
        assertThat(stream.lastSequencePosition()).isEqualTo(1)
        // The token reads cleanly and is non-null, usable for a later conditional append.
        assertThat(stream.consistencyToken()).isNotNull
    }

    @Test
    fun executeAndReturnDecision_returns_the_committed_decision_after_a_conflict_retry() {
        append(NameDefined("event-0", time, "name", "Jane Doe"))
        val interloper = DcbCloudEvents.withTags(converter.toCloudEvent(NameWasChanged("event-99", time, "name", "Interloper")), setOf("name:name"))
        val conflictingStore = ConflictingOnceDcbEventStore(eventStore, interloper)
        val service = GenericDcbApplicationService(conflictingStore, converter, { event -> setOf(tagFor(event)) }, GenericDcbApplicationService.defaultRetry())
        val deciderRuns = AtomicInteger()
        val countingDecider: Decider<NameCommand, String?, DomainEvent> = decider(
            initialState = null,
            decide = { command, _ ->
                deciderRuns.incrementAndGet()
                when (command) {
                    is DefineName -> listOf(NameDefined("event-1", time, "name", command.name))
                    is ChangeName -> listOf(NameWasChanged("event-2", time, "name", command.name))
                }
            },
            evolve = { _, event -> event.name() }
        )

        val decision = service.executeAndReturnDecision(nameQuery("name"), ChangeName("John Doe"), countingDecider).block()

        // The first append conflicts on the interloper, the retry reruns the decider against the fresh read, and
        // executeAndReturnDecision returns the committed attempt's decision, not the first attempt's.
        assertThat(deciderRuns).hasValue(2)
        assertThat(decision!!.events).containsExactly(NameWasChanged("event-2", time, "name", "John Doe"))
        assertThat(decision.state).isEqualTo("John Doe")
    }

    private fun nameDecider(): Decider<NameCommand, String?, DomainEvent> =
        decider(
            initialState = null,
            decide = { command, state ->
                when (command) {
                    is DefineName -> if (state == null) listOf(NameDefined("event-1", time, "name", command.name)) else emptyList()
                    is ChangeName -> listOf(NameWasChanged("event-2", time, "name", command.name))
                }
            },
            evolve = { _, event -> event.name() }
        )

    private fun append(vararg events: DomainEvent) {
        val cloudEvents = converter.toCloudEvents(Stream.of(*events))
            .map { event -> DcbCloudEvents.withTags(event, setOf("name:name")) }
            .toList()
        eventStore.append(cloudEvents).block()
    }

    private fun readNameEvents(nameId: String): List<DomainEvent> =
        queries.query(nameQuery(nameId)).collectList().block()!!

    private fun nameQuery(nameId: String): DcbQuery = DcbQuery.tags("name:$nameId")

    private fun tagFor(event: DomainEvent): String = "name:${event.userId()}"

    private sealed interface NameCommand
    private data class DefineName(val name: String) : NameCommand
    private data class ChangeName(val name: String) : NameCommand

    /**
     * A reactive DCB event store that, on the first conditional append, first commits a conflicting matching event so
     * the carried consistency token is stale and the append fails once, then delegates normally on later attempts.
     */
    private class ConflictingOnceDcbEventStore(
        private val delegate: DcbEventStore,
        private val conflictingEvent: CloudEvent
    ) : DcbEventStore {
        private val conflictInserted = AtomicBoolean()

        override fun read(query: DcbQuery, options: DcbReadOptions): Mono<DcbEventStream> = delegate.read(query, options)

        override fun append(events: MutableList<CloudEvent>): Mono<DcbAppendResult> = delegate.append(events)

        override fun append(events: MutableList<CloudEvent>, condition: DcbAppendCondition): Mono<DcbAppendResult> =
            if (conflictInserted.compareAndSet(false, true)) {
                delegate.append(listOf(conflictingEvent)).then(delegate.append(events, condition))
            } else {
                delegate.append(events, condition)
            }
    }

    companion object {
        @Container
        @JvmStatic
        private val mongoDBContainer: MongoDBContainer = MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet().apply {
            withReuse(true)
            portBindings = listOf("27017:27017")
        }
    }

    @RegisterExtension
    val flushMongoDBExtension = FlushMongoDBExtension(ConnectionString(mongoDBContainer.replicaSetUrl + ".dcbreactordsl"))
}
