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

package org.occurrent.dsl.snapshot.reactor

import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.ConnectionString
import com.mongodb.reactivestreams.client.MongoClients
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService
import org.occurrent.application.service.reactor.generic.GenericApplicationService
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.dcb.toDcb
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.dsl.snapshot.SnapshotOptions
import org.occurrent.dsl.snapshot.SnapshotPolicy
import org.occurrent.eventstore.api.EventStoreCapability.DCB
import org.occurrent.eventstore.api.EventStoreCapability.STREAM
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore
import org.occurrent.mongodb.timerepresentation.TimeRepresentation
import org.occurrent.testing.mongodb.OccurrentMongoFlush
import org.occurrent.testsupport.mongodb.MongoTestDatabase
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer
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

// javac checks no JSpecify, so a change to the nullability of the Java signatures fails only when Kotlin compiles these calls. The state
// type is nullable and the declared Mono<String> is non-null, which stops compiling if a return type goes back to Mono<S>.
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores::class)
class ReactiveSnapshotExecuteAndReturnStateSignatureTest {

    private lateinit var snapshotService: ReactiveSnapshotDeciderApplicationService<DomainEvent>
    private lateinit var snapshotDcbService: ReactiveSnapshotDcbDeciderApplicationService<DomainEvent>
    private lateinit var time: LocalDateTime

    @BeforeEach
    fun create_instances() {
        val connectionString = ConnectionString(mongoDBContainer.replicaSetUrl + ".reactivesnapshotsignature")
        val mongoClient = MongoClients.create(connectionString)
        val mongoTemplate = ReactiveMongoTemplate(mongoClient, connectionString.database!!)
        val transactionManager = ReactiveMongoTransactionManager(SimpleReactiveMongoDatabaseFactory(mongoClient, connectionString.database!!))
        val config = EventStoreConfig.Builder()
            .eventStoreCollectionName("events")
            .transactionConfig(transactionManager)
            .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
            .eventStoreCapabilities(STREAM, DCB)
            .build()
        val eventStore = ReactorMongoEventStore(mongoTemplate, config)
        val converter = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        snapshotService = ReactiveSnapshotDeciderApplicationService(GenericApplicationService(eventStore, converter))
        snapshotDcbService = ReactiveSnapshotDcbDeciderApplicationService(
            GenericDcbApplicationService(eventStore, converter, { _ -> setOf(TAG) }, GenericDcbApplicationService.defaultRetry())
        )
        time = LocalDateTime.now()
    }

    @Test
    fun ReactiveSnapshotDeciderApplicationService_executeAndReturnState_with_a_String_stream_id_and_a_command_emits_a_non_null_state() {
        val snapshotDecider = snapshotDecider()

        val state: Mono<String> = snapshotService.executeAndReturnState(UUID.randomUUID().toString(), Define("Jane"), snapshotDecider)

        assertThat(state.block()).isEqualTo("Jane")
    }

    @Test
    fun ReactiveSnapshotDeciderApplicationService_executeAndReturnState_with_a_UUID_stream_id_and_a_command_emits_a_non_null_state() {
        val snapshotDecider = snapshotDecider()

        val state: Mono<String> = snapshotService.executeAndReturnState(UUID.randomUUID(), Define("Jane"), snapshotDecider)

        assertThat(state.block()).isEqualTo("Jane")
    }

    @Test
    fun ReactiveSnapshotDeciderApplicationService_executeAndReturnState_with_a_String_stream_id_and_a_command_list_emits_a_non_null_state() {
        val snapshotDecider = snapshotDecider()

        val state: Mono<String> = snapshotService.executeAndReturnState(UUID.randomUUID().toString(), listOf(Define("Jane"), Change("John")), snapshotDecider)

        assertThat(state.block()).isEqualTo("John")
    }

    @Test
    fun ReactiveSnapshotDeciderApplicationService_executeAndReturnState_with_a_UUID_stream_id_and_a_command_list_emits_a_non_null_state() {
        val snapshotDecider = snapshotDecider()

        val state: Mono<String> = snapshotService.executeAndReturnState(UUID.randomUUID(), listOf(Define("Jane"), Change("John")), snapshotDecider)

        assertThat(state.block()).isEqualTo("John")
    }

    @Test
    fun ReactiveSnapshotDcbDeciderApplicationService_executeAndReturnState_with_a_command_emits_a_non_null_state() {
        val snapshotDcbDecider = snapshotDcbDecider()

        val state: Mono<String> = snapshotDcbService.executeAndReturnState(Define("Jane"), snapshotDcbDecider)

        assertThat(state.block()).isEqualTo("Jane")
    }

    @Test
    fun ReactiveSnapshotDcbDeciderApplicationService_executeAndReturnState_with_a_command_list_emits_a_non_null_state() {
        val snapshotDcbDecider = snapshotDcbDecider()

        val state: Mono<String> = snapshotDcbService.executeAndReturnState(listOf(Define("Jane"), Change("John")), snapshotDcbDecider)

        assertThat(state.block()).isEqualTo("John")
    }

    // Null until created, so S is String? and the service must still hand back a Mono<String>
    private fun nameDecider(): Decider<NameCommand, String?, DomainEvent> =
        decider(
            initialState = null,
            decide = { command, _ ->
                when (command) {
                    is Define -> listOf(NameDefined(UUID.randomUUID().toString(), time, "name", command.name))
                    is Change -> listOf(NameWasChanged(UUID.randomUUID().toString(), time, "name", command.name))
                }
            },
            evolve = { _, event -> event.name() }
        )

    private fun snapshotDecider(): ReactiveSnapshotDecider<NameCommand, String?, DomainEvent> =
        ReactiveSnapshotDecider.from(nameDecider(), ReactiveSnapshotStore.inMemory(), SnapshotOptions.of(1, SnapshotPolicy.always()))

    private fun snapshotDcbDecider(): ReactiveSnapshotDcbDecider<NameCommand, String?, DomainEvent> =
        ReactiveSnapshotDcbDecider.from(
            nameDecider().toDcb(criteria = { DcbCriteria.tags(TAG) }, tags = { setOf(TAG) }),
            ReactiveSnapshotStore.inMemory(),
            SnapshotOptions.of(1, SnapshotPolicy.always())
        )

    private sealed interface NameCommand
    private data class Define(val name: String) : NameCommand
    private data class Change(val name: String) : NameCommand

    companion object {
        private val TAG = Tag.of("name", "acct")

        @Container
        @JvmStatic
        private val mongoDBContainer: MongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().apply {
            withReuse(true)
        }
    }

    @RegisterExtension
    val flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer))
}
