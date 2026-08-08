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

package org.occurrent.dsl.view

import com.mongodb.ConnectionString
import com.mongodb.client.MongoClients
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.mapping.Document
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer
import java.util.UUID

@Document(collection = "bulk-plain")
data class PlainBulkState(@Id val id: String, val value: String)

@Document(collection = "bulk-versioned")
data class VersionedBulkState(@Id val id: String, val value: String, @Version val version: Long? = null)

@Document(collection = "bulk-unique")
data class UniqueFieldState(@Id val id: String, val uniqueValue: String)

/**
 * The [mongoOperationsViewStateRepository] built ViewStateRepository is what
 * [SpringMongoViewExtensions.materialized(MongoOperations, ..)][materialized] wires internally, but that function
 * never calls findAllById/saveAll itself, so this exercises the repository directly to prove the bulk overrides in
 * [org.occurrent.dsl.view.internal.MongoBulkViewStateOperations] behave exactly like the looping defaults, just
 * batched.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores::class)
@Timeout(60)
class MongoOperationsViewStateRepositoryBulkTest {

    companion object {
        @Suppress("unused")
        @Container
        val mongoDBContainer: MongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true)
    }

    private fun mongoOperations(): MongoOperations {
        val connectionString = ConnectionString(mongoDBContainer.getReplicaSetUrl("mongo-ops-bulk-${UUID.randomUUID()}"))
        return MongoTemplate(MongoClients.create(connectionString), connectionString.database!!)
    }

    private val identityConverter = object : StateConverter<PlainBulkState, PlainBulkState> {
        override fun toDTO(viewState: PlainBulkState): PlainBulkState = viewState
        override fun fromDTO(dto: PlainBulkState): PlainBulkState = dto
    }

    private val versionedConverter = object : StateConverter<VersionedBulkState, VersionedBulkState> {
        override fun toDTO(viewState: VersionedBulkState): VersionedBulkState = viewState
        override fun fromDTO(dto: VersionedBulkState): VersionedBulkState = dto
    }

    // --- findAllById --------------------------------------------------------------------------------------------

    @Test
    fun `findAllById returns exactly what the same number of single findById calls would return, including missing ids`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<PlainBulkState, PlainBulkState, String>(mongoOperations, identityConverter, PlainBulkState::class.java)
        val present = (1..5).map { i -> PlainBulkState("id-$i", "value-$i") }
        present.forEach { state -> repository.save(state.id, state) }
        val missing = listOf("missing-1", "missing-2")
        // Deliberately not insertion order (and not sorted), so a result keyed by DB/natural order rather than the
        // requested ids' own order would be caught here instead of accidentally matching by coincidence.
        val ids = listOf("id-4", "missing-1", "id-1", "id-5", "missing-2", "id-3", "id-2")
        check(ids.toSet() == (present.map { it.id } + missing).toSet())

        val bulkResult = repository.findAllById(ids)

        val loopedResult = LinkedHashMap<String, PlainBulkState>()
        ids.forEach { id -> repository.findById(id).ifPresent { state -> loopedResult[id] = state } }

        assertThat(bulkResult).isEqualTo(loopedResult)
        assertThat(bulkResult.keys).doesNotContainAnyElementsOf(missing)
        // The default findAllById javadoc promises the same shape as looping findById one id at a time: a map in
        // the ids' own iteration order, an absent id simply missing.
        assertThat(bulkResult.keys.toList()).isEqualTo(ids.filterNot { it in missing })
    }

    @Test
    fun `findAllById on an empty id collection returns an empty map without querying`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<PlainBulkState, PlainBulkState, String>(mongoOperations, identityConverter, PlainBulkState::class.java)

        assertThat(repository.findAllById(emptyList())).isEmpty()
    }

    // --- saveAll: same persisted state as N single saves --------------------------------------------------------

    @Test
    fun `saveAll persists exactly what the same number of single save calls would persist`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<PlainBulkState, PlainBulkState, String>(mongoOperations, identityConverter, PlainBulkState::class.java)
        val looped = (1..4).map { i -> PlainBulkState("looped-$i", "value-$i") }
        val bulked = (1..4).map { i -> PlainBulkState("bulked-$i", "value-$i") }

        looped.forEach { state -> repository.save(state.id, state) }
        repository.saveAll(bulked.associateBy { it.id })

        val loopedPersisted = looped.map { mongoOperations.findById(it.id, PlainBulkState::class.java) }
        val bulkedPersisted = bulked.map { mongoOperations.findById(it.id, PlainBulkState::class.java) }
        assertThat(bulkedPersisted).isEqualTo(bulked)
        assertThat(bulkedPersisted.map { it?.value }).isEqualTo(loopedPersisted.map { it?.value })
    }

    @Test
    fun `saveAll upserts existing entries the same way save does`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<PlainBulkState, PlainBulkState, String>(mongoOperations, identityConverter, PlainBulkState::class.java)
        repository.save("id-1", PlainBulkState("id-1", "original"))

        repository.saveAll(mapOf("id-1" to PlainBulkState("id-1", "updated")))

        assertThat(mongoOperations.findById("id-1", PlainBulkState::class.java)).isEqualTo(PlainBulkState("id-1", "updated"))
    }

    @Test
    fun `saveAll on an empty map is a no-op`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<PlainBulkState, PlainBulkState, String>(mongoOperations, identityConverter, PlainBulkState::class.java)

        assertThatCode { repository.saveAll(emptyMap()) }.doesNotThrowAnyException()
    }

    // --- saveAll: @Version-carrying state, same version bookkeeping as save -----------------------------------

    @Test
    fun `saveAll initializes a fresh versioned entry's version the same way a single save does`() {
        val mongoOperations = mongoOperations()
        val bulkRepository = mongoOperationsViewStateRepository<VersionedBulkState, VersionedBulkState, String>(mongoOperations, versionedConverter, VersionedBulkState::class.java)
        val loopedRepository = mongoOperationsViewStateRepository<VersionedBulkState, VersionedBulkState, String>(mongoOperations, versionedConverter, VersionedBulkState::class.java)

        loopedRepository.save("looped-1", VersionedBulkState("looped-1", "v1"))
        bulkRepository.saveAll(mapOf("bulked-1" to VersionedBulkState("bulked-1", "v1")))

        val loopedPersisted = mongoOperations.findById("looped-1", VersionedBulkState::class.java)
        val bulkedPersisted = mongoOperations.findById("bulked-1", VersionedBulkState::class.java)
        assertThat(bulkedPersisted?.version).isEqualTo(loopedPersisted?.version)
        assertThat(bulkedPersisted?.version).isEqualTo(0L)
    }

    @Test
    fun `saveAll increments an existing versioned entry's version the same way a single save does`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<VersionedBulkState, VersionedBulkState, String>(mongoOperations, versionedConverter, VersionedBulkState::class.java)
        repository.save("id-1", VersionedBulkState("id-1", "v1"))
        val afterFirstSave = mongoOperations.findById("id-1", VersionedBulkState::class.java)!!

        repository.saveAll(mapOf("id-1" to afterFirstSave.copy(value = "v2")))

        val afterBulkSave = mongoOperations.findById("id-1", VersionedBulkState::class.java)!!
        assertThat(afterBulkSave.value).isEqualTo("v2")
        assertThat(afterBulkSave.version).isEqualTo(afterFirstSave.version!! + 1)
    }

    // --- exception translation: OptimisticLockingFailureException -------------------------------------------

    @Test
    fun `saveAll surfaces OptimisticLockingFailureException when an entry's version has moved on, same as save`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<VersionedBulkState, VersionedBulkState, String>(mongoOperations, versionedConverter, VersionedBulkState::class.java)
        repository.save("id-1", VersionedBulkState("id-1", "v1"))
        val staleRead = mongoOperations.findById("id-1", VersionedBulkState::class.java)!!
        // A concurrent writer moves the version on before this saveAll executes.
        repository.save("id-1", staleRead.copy(value = "v2-from-elsewhere"))

        val thrown = catchThrowable { repository.saveAll(mapOf("id-1" to staleRead.copy(value = "v2-stale"))) }

        assertThat(thrown).isInstanceOf(OptimisticLockingFailureException::class.java)
        // The document actually stored is the concurrent writer's, not this stale attempt's.
        assertThat(mongoOperations.findById("id-1", VersionedBulkState::class.java)?.value).isEqualTo("v2-from-elsewhere")
    }

    @Test
    fun `single save also throws OptimisticLockingFailureException for the same stale version, proving the bulk path matches it`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<VersionedBulkState, VersionedBulkState, String>(mongoOperations, versionedConverter, VersionedBulkState::class.java)
        repository.save("id-1", VersionedBulkState("id-1", "v1"))
        val staleRead = mongoOperations.findById("id-1", VersionedBulkState::class.java)!!
        repository.save("id-1", staleRead.copy(value = "v2-from-elsewhere"))

        val thrown = catchThrowable { repository.save("id-1", staleRead.copy(value = "v2-stale")) }

        assertThat(thrown).isInstanceOf(OptimisticLockingFailureException::class.java)
    }

    // --- exception translation: DuplicateKeyException ----------------------------------------------------------

    @Test
    fun `saveAll surfaces DuplicateKeyException, not the raw bulk-write wrapper, for a unique index violation`() {
        val mongoOperations = mongoOperations()
        mongoOperations.indexOps(UniqueFieldState::class.java).ensureIndex(Index().on("uniqueValue", org.springframework.data.domain.Sort.Direction.ASC).unique())
        val converter = object : StateConverter<UniqueFieldState, UniqueFieldState> {
            override fun toDTO(viewState: UniqueFieldState): UniqueFieldState = viewState
            override fun fromDTO(dto: UniqueFieldState): UniqueFieldState = dto
        }
        val repository = mongoOperationsViewStateRepository<UniqueFieldState, UniqueFieldState, String>(mongoOperations, converter, UniqueFieldState::class.java)
        // Two new (never persisted) entries whose unique-indexed field collides, so the bulk insert batch itself
        // trips the index, the same way two individual inserts would.
        val states = mapOf(
            "id-1" to UniqueFieldState("id-1", "same-value"),
            "id-2" to UniqueFieldState("id-2", "same-value"),
        )

        val thrown = catchThrowable { repository.saveAll(states) }

        assertThat(thrown).isInstanceOf(DuplicateKeyException::class.java)
    }

    @Test
    fun `single save also throws DuplicateKeyException for the same unique index violation, proving the bulk path matches it`() {
        val mongoOperations = mongoOperations()
        mongoOperations.indexOps(UniqueFieldState::class.java).ensureIndex(Index().on("uniqueValue", org.springframework.data.domain.Sort.Direction.ASC).unique())
        mongoOperations.insert(UniqueFieldState("id-1", "same-value"))

        val thrown = catchThrowable { mongoOperations.insert(UniqueFieldState("id-2", "same-value")) }

        assertThat(thrown).isInstanceOf(DuplicateKeyException::class.java)
    }

    // --- requireMatchingDocumentId preserved in the bulk save path ---------------------------------------------

    @Test
    fun `saveAll rejects a mismatched document id the same way save does, before writing anything in the batch`() {
        val mongoOperations = mongoOperations()
        val repository = mongoOperationsViewStateRepository<PlainBulkState, PlainBulkState, String>(mongoOperations, identityConverter, PlainBulkState::class.java)
        // "wrong-id" resolves to a different key than the state's own @Id ("id-1"), which requireMatchingDocumentId
        // must reject exactly as save(id, state) already does.
        val states = linkedMapOf(
            "id-1" to PlainBulkState("id-1", "fine"),
            "wrong-id" to PlainBulkState("id-2", "mismatched"),
        )

        val thrown = catchThrowable { repository.saveAll(states) }

        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
        // Fails fast: nothing in the batch was written, including the entry that validated fine.
        assertThat(mongoOperations.findById("id-1", PlainBulkState::class.java)).isNull()
    }
}
