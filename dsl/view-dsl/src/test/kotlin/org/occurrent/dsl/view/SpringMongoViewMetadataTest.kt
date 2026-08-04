/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.view

import com.mongodb.ConnectionString
import com.mongodb.client.MongoClients
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.cloudevents.EventMetadata
import org.occurrent.cloudevents.OccurrentCloudEventExtension
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.view.testsupport.nameChanged
import org.occurrent.dsl.view.testsupport.nameDefined
import org.occurrent.testing.mongodb.OccurrentMongoFlush
import org.occurrent.testsupport.mongodb.MongoTestDatabase
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findById
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.repository.CrudRepository
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import java.util.Optional
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Document(collection = "stream-keyed-name-state")
data class StreamKeyedNameState(@Id val streamId: String, val name: String, val streamVersion: Long)

/**
 * Fake used only for the CrudRepository-backed materialized(...) tests, so they don't need a Mongo repository bean.
 * Keyed with an externally supplied id extractor since CrudRepository has no generic id accessor.
 */
class FakeCrudRepository<T : Any, ID : Any>(private val idOf: (T) -> ID) : CrudRepository<T, ID> {
    val store = ConcurrentHashMap<ID, T>()
    override fun <S : T> save(entity: S): S {
        store[idOf(entity)] = entity
        return entity
    }

    override fun <S : T> saveAll(entities: Iterable<S>): Iterable<S> = entities.onEach { save(it) }
    override fun findById(id: ID): Optional<T> = Optional.ofNullable(store[id])
    override fun existsById(id: ID): Boolean = store.containsKey(id)
    override fun findAll(): Iterable<T> = store.values.toList()
    override fun findAllById(ids: Iterable<ID>): Iterable<T> = ids.mapNotNull { store[it] }
    override fun count(): Long = store.size.toLong()
    override fun deleteById(id: ID) {
        store.remove(id)
    }

    override fun delete(entity: T) {
        store.values.remove(entity)
    }

    override fun deleteAllById(ids: Iterable<ID>) {
        ids.forEach { store.remove(it) }
    }

    override fun deleteAll(entities: Iterable<T>) {
        entities.forEach { store.values.remove(it) }
    }

    override fun deleteAll() {
        store.clear()
    }
}

private fun metadata(streamId: String, streamVersion: Long): EventMetadata {
    val data: Map<String, Any> = mapOf(
        OccurrentCloudEventExtension.STREAM_ID to streamId,
        OccurrentCloudEventExtension.STREAM_VERSION to streamVersion
    )
    return EventMetadata(data)
}

/**
 * Covers the gap this fix closes: the Spring Mongo `materialized(...)` helper had no id function that could see the
 * event's [EventMetadata], so a view instance could not be keyed by the stream id or stream version.
 */
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
@Testcontainers
class SpringMongoViewMetadataTest {

    companion object {
        @Suppress("unused")
        @Container
        val mongoDBContainer: MongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true)

        // Keyed and folded purely from the metadata, so only the metadata-aware materialized(...) overload can drive it.
        private val streamKeyedView: View<StreamKeyedNameState?, DomainEvent> = view<StreamKeyedNameState?, DomainEvent>(null) { s, m, e ->
            when (e) {
                is NameDefined -> StreamKeyedNameState(m.getStreamId(), e.name, m.getStreamVersion())
                is NameWasChanged -> s!!.copy(name = e.name, streamVersion = m.getStreamVersion())
                else -> s
            }
        }

        // Keyed from the event alone (VIEW_ID unrelated to metadata), but the fold still reads the metadata, so the
        // event-only materialized(...) overload must carry it through rather than substitute EventMetadata.empty().
        private val eventKeyedView: View<StreamKeyedNameState, DomainEvent> = view(StreamKeyedNameState("", "", -1L)) { s, m, e ->
            when (e) {
                is NameDefined -> StreamKeyedNameState(m.getStreamId(), e.name, m.getStreamVersion())
                else -> s
            }
        }
    }

    @RegisterExtension
    val flushMongoDBExtension: OccurrentMongoFlush = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, "stream-keyed-events"))

    lateinit var mongoOperations: MongoOperations

    private fun mongoOperations(): MongoOperations {
        val connectionString = ConnectionString(mongoDBContainer.getReplicaSetUrl("stream-keyed-events"))
        val mongoClient = MongoClients.create(connectionString)
        return MongoTemplate(mongoClient, connectionString.database!!)
    }

    private fun materializedView(): MaterializedView<DomainEvent> {
        mongoOperations = mongoOperations()
        return streamKeyedView.materialized(mongoOperations) { m: EventMetadata, _: DomainEvent -> m.getStreamId() }
    }

    @Test
    fun `keys the view instance by the stream id from the metadata and folds with the stream version`() {
        val materializedView = materializedView()
        val streamId = UUID.randomUUID().toString()

        materializedView.update(metadata(streamId, 0L), nameDefined(streamId, "Johan"))
        materializedView.update(metadata(streamId, 1L), nameChanged(streamId, "Johan Haleby"))

        val saved = mongoOperations.findById<StreamKeyedNameState>(streamId)
        assertThat(saved).isEqualTo(StreamKeyedNameState(streamId, "Johan Haleby", 1L))
    }

    @Test
    fun `a document whose id differs from the derived view id fails instead of never accumulating`() {
        val mongoOperations = mongoOperations()
        // Folds to a document whose @Id is a constant while the view id comes from the stream id, so reads would look up
        // one document and writes would create another, and the fold would restart from null on every event.
        val mismatchedView = view<StreamKeyedNameState?, DomainEvent>(null) { _, m, e ->
            StreamKeyedNameState("always-the-same", (e as NameDefined).name, m.getStreamVersion())
        }
        val materializedView = mismatchedView.materialized(mongoOperations) { m: EventMetadata, _: DomainEvent -> m.getStreamId() }
        val streamId = UUID.randomUUID().toString()

        val thrown = catchThrowable { materializedView.update(metadata(streamId, 0L), nameDefined(streamId, "Johan")) }

        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("would never")
    }

    @Test
    fun `event-only update throws because the key needs metadata that never arrived`() {
        val materializedView = materializedView()

        val thrown = catchThrowable { materializedView.update(nameDefined(name = "Johan")) }

        assertThat(thrown).isInstanceOf(NullPointerException::class.java)
            .hasMessageContaining("streamId extension is absent")
    }

    @Test
    fun `MongoOperations-backed event-only key still folds with the real metadata`() {
        mongoOperations = mongoOperations()
        val materializedView = eventKeyedView.materialized(mongoOperations) { e: DomainEvent -> e.userId() }
        val streamId = UUID.randomUUID().toString()

        materializedView.update(metadata(streamId, 3L), nameDefined(streamId, "Johan"))

        val saved = mongoOperations.findById<StreamKeyedNameState>(streamId)
        assertThat(saved).isEqualTo(StreamKeyedNameState(streamId, "Johan", 3L))
    }

    @Test
    fun `CrudRepository-backed event-only key still folds with the real metadata`() {
        val repository = FakeCrudRepository<StreamKeyedNameState, String> { it.streamId }
        val materializedView = eventKeyedView.materialized(repository) { e: DomainEvent -> e.userId() }
        val streamId = UUID.randomUUID().toString()

        materializedView.update(metadata(streamId, 3L), nameDefined(streamId, "Johan"))

        assertThat(repository.store[streamId]).isEqualTo(StreamKeyedNameState(streamId, "Johan", 3L))
    }
}
