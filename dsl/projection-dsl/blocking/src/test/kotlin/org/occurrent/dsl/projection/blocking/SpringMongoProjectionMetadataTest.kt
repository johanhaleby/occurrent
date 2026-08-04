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

package org.occurrent.dsl.projection.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.ConnectionString
import com.mongodb.client.MongoClients
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.application.service.blocking.TransactionExecutor
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.projection.projection
import org.occurrent.dsl.projection.singletonProjection
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findById
import org.springframework.data.mongodb.core.mapping.Document
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import java.net.URI
import java.util.Date
import java.util.UUID

@Document(collection = "stream-keyed-name")
data class StreamKeyedName(@Id val streamId: String, val name: String, val streamVersion: Long)

@Document(collection = "name-count")
data class NameCount(@Id val key: String, val count: Int)

/**
 * Covers the gap this fix closes: the Spring Mongo `project(...)` helper could not key a view instance by event
 * metadata, and a metadata-keyed projection fed empty metadata failed with a misdirecting message instead of the
 * accurate one from [ProjectionKeys][org.occurrent.dsl.projection.internal.ProjectionKeys].
 */
@DisplayNameGeneration(ReplaceUnderscores::class)
@Testcontainers
class SpringMongoProjectionMetadataTest {

    @RegisterExtension
    val flushMongoDBExtension: FlushMongoDBExtension = FlushMongoDBExtension(ConnectionString(mongoDBContainer.getReplicaSetUrl("spring-mongo-projection-metadata-test")))

    companion object {
        @Suppress("unused")
        @Container
        val mongoDBContainer: MongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true)
    }

    private fun mongoOperations(): MongoOperations {
        val connectionString = ConnectionString(mongoDBContainer.getReplicaSetUrl("spring-mongo-projection-metadata-test"))
        val mongoClient = MongoClients.create(connectionString)
        return MongoTemplate(mongoClient, connectionString.database!!)
    }

    private data class Fixture(val mongoOperations: MongoOperations, val subscriptions: Subscriptions<DomainEvent>, val applicationService: GenericApplicationService<DomainEvent>)

    private fun newFixture(): Fixture {
        val mongoOperations = mongoOperations()
        val converter = jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent:test"), DomainEvent::eventId)
        val eventStore = InMemoryEventStore()
        val synchronousSubscriptions = SynchronousSubscriptionModel()
        val subscriptions = Subscriptions(synchronousSubscriptions, converter)
        val applicationService = GenericApplicationService.builder(eventStore, converter)
            .synchronousSubscriptions(synchronousSubscriptions)
            .transactionExecutor(TransactionExecutor.noTransaction())
            .build()
        return Fixture(mongoOperations, subscriptions, applicationService)
    }

    // Keyed and folded from EventMetadata.getStreamId()/getStreamVersion(), which throw on empty metadata rather than
    // returning null, so a broken wiring that drops the metadata fails loudly instead of quietly matching by luck.
    private fun streamKeyedProjection() =
        projection<StreamKeyedName, DomainEvent, String>(initialState = StreamKeyedName("", "", -1L)) {
            id { metadata, _ -> metadata.getStreamId() }
            on<NameDefined> { _, metadata, e -> StreamKeyedName(metadata.getStreamId(), e.name(), metadata.getStreamVersion()) }
            on<NameWasChanged> { state, metadata, e -> state.copy(name = e.name(), streamVersion = metadata.getStreamVersion()) }
        }

    @Test
    fun `keys the materialized view by the event's stream id and folds with its stream version`() {
        val (mongoOperations, subscriptions, applicationService) = newFixture()

        // The Mongo-materializing project(...) overload under test: it must resolve and fold with the real metadata.
        subscriptions.project("stream-keyed-name", streamKeyedProjection(), mongoOperations)

        val userId = UUID.randomUUID().toString()
        applicationService.execute(userId) { _ -> listOf(NameDefined(UUID.randomUUID().toString(), Date(), userId, "Johan")) }
        applicationService.execute(userId) { _ -> listOf(NameWasChanged(UUID.randomUUID().toString(), Date(), userId, "Johan Haleby")) }

        // Document id is the stream id from the metadata, not something derived from the event alone. The version is 2
        // because the second event carries its own metadata, so folding the first event's version again would fail here.
        val saved = mongoOperations.findById<StreamKeyedName>(userId)
        assertThat(saved).isEqualTo(StreamKeyedName(userId, "Johan Haleby", 2L))
    }

    @Test
    fun `a projection keyed by metadata fails with an accurate message instead of silently skipping the event when fed empty metadata`() {
        val mongoOperations = mongoOperations()
        val repository = mongoViewStateRepository<Int, Long>(mongoOperations)
        // Keyed by the global position, which (unlike getStreamId()) returns null rather than throwing on empty
        // metadata, so this reproduces exactly the silent-drop scenario the guard exists for.
        val positionKeyedProjection = projection<Int, DomainEvent, Long>(initialState = 0) {
            id { metadata, _ -> metadata.getPosition() }
            on<NameDefined> { state, _ -> state + 1 }
        }
        val materializedView = Projections.materializedView(positionKeyedProjection, repository, "singleton")

        val thrown = catchThrowable {
            materializedView.update(NameDefined(UUID.randomUUID().toString(), Date(), "irrelevant", "Johan"))
        }

        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("keyed by event metadata")
    }

    @Test
    fun `a view state whose id differs from the projection's id fails instead of never accumulating`() {
        val mongoOperations = mongoOperations()
        val repository = mongoViewStateRepository<NameCount, String>(mongoOperations)

        // The fold leaves the state's @Id empty while the repository is keyed by "some-key", so reads would look up one
        // document and writes would create another, and the count would restart from the initial state on every event.
        val thrown = catchThrowable { repository.save("some-key", NameCount("", 1)) }

        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("would never accumulate")
    }

    @Test
    fun `a single-instance projection still keys its one document by the subscription id`() {
        val (mongoOperations, subscriptions, applicationService) = newFixture()

        // The state carries "name-count" as its own @Id, because mongoOperations.save(state) derives the document id from
        // the state rather than from the key the repository was given. See the note on the assertion below.
        val nameCount = singletonProjection<NameCount, DomainEvent>(initialState = NameCount("name-count", 0)) {
            on<NameDefined> { state, _ -> state.copy(count = state.count + 1) }
        }
        subscriptions.project("name-count", nameCount, mongoOperations)

        applicationService.execute(UUID.randomUUID().toString()) { _ ->
            listOf(NameDefined(UUID.randomUUID().toString(), Date(), "someone", "Johan"))
        }
        applicationService.execute(UUID.randomUUID().toString()) { _ ->
            listOf(NameDefined(UUID.randomUUID().toString(), Date(), "someone else", "Tina"))
        }

        // A single-instance projection has no id function, so the repository is keyed by the subscription id. The helper
        // used to cast the subscription id to the id type itself and now delegates, so this pins that the key is
        // unchanged. Counting to 2 is the part that matters: it only reaches 2 if the second update read back the state
        // the first one saved, which is what proves the read key and the written document id agree.
        assertThat(mongoOperations.findById<NameCount>("name-count")).isEqualTo(NameCount("name-count", 2))
    }
}
