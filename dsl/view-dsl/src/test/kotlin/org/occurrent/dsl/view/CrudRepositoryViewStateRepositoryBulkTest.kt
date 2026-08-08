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
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory
import org.springframework.data.repository.CrudRepository
import org.springframework.data.domain.Sort.Direction.ASC
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import java.util.UUID

@Document(collection = "crud-bulk-plain")
data class CrudPlainState(@Id val id: String, val value: String)

@Document(collection = "crud-bulk-versioned")
data class CrudVersionedState(@Id val id: String, val value: String, @Version val version: Long? = null)

@Document(collection = "crud-bulk-unique")
data class CrudUniqueFieldState(@Id val id: String, val uniqueValue: String)

interface CrudPlainStateRepository : CrudRepository<CrudPlainState, String>
interface CrudVersionedStateRepository : CrudRepository<CrudVersionedState, String>
interface CrudUniqueFieldStateRepository : CrudRepository<CrudUniqueFieldState, String>

/**
 * As [MongoOperationsViewStateRepositoryBulkTest], but for [crudRepositoryViewStateRepository], the
 * ViewStateRepository the CrudRepository-taking [materialized] overload wires. The repository beans are built
 * through [MongoRepositoryFactory] directly rather than a full Spring context, so this stays a fast, focused test of
 * the bulk behaviour rather than of Spring Data repository bean wiring.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores::class)
@Timeout(60)
class CrudRepositoryViewStateRepositoryBulkTest {

    companion object {
        @Suppress("unused")
        @Container
        val mongoDBContainer: MongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true)
    }

    private fun mongoOperations(): MongoOperations {
        val connectionString = ConnectionString(mongoDBContainer.getReplicaSetUrl("crud-repo-bulk-${UUID.randomUUID()}"))
        return MongoTemplate(MongoClients.create(connectionString), connectionString.database!!)
    }

    private val plainConverter = object : StateConverter<CrudPlainState, CrudPlainState> {
        override fun toDTO(viewState: CrudPlainState): CrudPlainState = viewState
        override fun fromDTO(dto: CrudPlainState): CrudPlainState = dto
    }

    private val versionedConverter = object : StateConverter<CrudVersionedState, CrudVersionedState> {
        override fun toDTO(viewState: CrudVersionedState): CrudVersionedState = viewState
        override fun fromDTO(dto: CrudVersionedState): CrudVersionedState = dto
    }

    // --- findAllById --------------------------------------------------------------------------------------------

    @Test
    fun `findAllById returns exactly what the same number of single findById calls would return, including missing ids`() {
        val mongoOperations = mongoOperations()
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudPlainStateRepository::class.java)
        val repository = crudRepositoryViewStateRepository<CrudPlainState, CrudPlainState, String>(crudRepository, plainConverter)
        val present = (1..5).map { i -> CrudPlainState("id-$i", "value-$i") }
        present.forEach { state -> repository.save(state.id, state) }
        val missing = listOf("missing-1", "missing-2")
        // Not insertion order, so a result keyed by DB/natural order rather than the requested ids' own order would
        // be caught here.
        val ids = listOf("id-4", "missing-1", "id-1", "id-5", "missing-2", "id-3", "id-2")

        val bulkResult = repository.findAllById(ids)

        val loopedResult = LinkedHashMap<String, CrudPlainState>()
        ids.forEach { id -> repository.findById(id).ifPresent { state -> loopedResult[id] = state } }

        assertThat(bulkResult).isEqualTo(loopedResult)
        assertThat(bulkResult.keys).doesNotContainAnyElementsOf(missing)
        assertThat(bulkResult.keys.toList()).isEqualTo(ids.filterNot { it in missing })
    }

    @Test
    fun `findAllById on an empty id collection returns an empty map without querying`() {
        val mongoOperations = mongoOperations()
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudPlainStateRepository::class.java)
        val repository = crudRepositoryViewStateRepository<CrudPlainState, CrudPlainState, String>(crudRepository, plainConverter)

        assertThat(repository.findAllById(emptyList())).isEmpty()
    }

    // --- saveAll: same persisted state as N single saves --------------------------------------------------------

    @Test
    fun `saveAll persists exactly what the same number of single save calls would persist`() {
        val mongoOperations = mongoOperations()
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudPlainStateRepository::class.java)
        val repository = crudRepositoryViewStateRepository<CrudPlainState, CrudPlainState, String>(crudRepository, plainConverter)
        val looped = (1..4).map { i -> CrudPlainState("looped-$i", "value-$i") }
        val bulked = (1..4).map { i -> CrudPlainState("bulked-$i", "value-$i") }

        looped.forEach { state -> repository.save(state.id, state) }
        repository.saveAll(bulked.associateBy { it.id })

        val bulkedPersisted = bulked.map { mongoOperations.findById(it.id, CrudPlainState::class.java) }
        assertThat(bulkedPersisted).isEqualTo(bulked)
    }

    @Test
    fun `saveAll upserts existing entries the same way save does`() {
        val mongoOperations = mongoOperations()
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudPlainStateRepository::class.java)
        val repository = crudRepositoryViewStateRepository<CrudPlainState, CrudPlainState, String>(crudRepository, plainConverter)
        repository.save("id-1", CrudPlainState("id-1", "original"))

        repository.saveAll(mapOf("id-1" to CrudPlainState("id-1", "updated")))

        assertThat(mongoOperations.findById("id-1", CrudPlainState::class.java)).isEqualTo(CrudPlainState("id-1", "updated"))
    }

    @Test
    fun `saveAll on an empty map is a no-op`() {
        val mongoOperations = mongoOperations()
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudPlainStateRepository::class.java)
        val repository = crudRepositoryViewStateRepository<CrudPlainState, CrudPlainState, String>(crudRepository, plainConverter)

        assertThatCode { repository.saveAll(emptyMap()) }.doesNotThrowAnyException()
    }

    // --- saveAll: all-new batch is a real bulk insert, mixed batch matches the looping default --------------------

    @Test
    fun `saveAll of an all-new batch persists the same states a bulk insert plus looping save would`() {
        val mongoOperations = mongoOperations()
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudVersionedStateRepository::class.java)
        val repository = crudRepositoryViewStateRepository<CrudVersionedState, CrudVersionedState, String>(crudRepository, versionedConverter)
        val bulked = (1..3).map { i -> CrudVersionedState("bulked-$i", "value-$i") }

        repository.saveAll(bulked.associateBy { it.id })

        val persisted = bulked.map { mongoOperations.findById(it.id, CrudVersionedState::class.java) }
        assertThat(persisted.map { it?.value }).isEqualTo(bulked.map { it.value })
        // SimpleMongoRepository.saveAll initializes a fresh @Version the same way a single insert does.
        assertThat(persisted.map { it?.version }).containsOnly(0L)
    }

    @Test
    fun `saveAll of a batch mixing new and existing entries increments an existing entry's version the same way save does`() {
        val mongoOperations = mongoOperations()
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudVersionedStateRepository::class.java)
        val repository = crudRepositoryViewStateRepository<CrudVersionedState, CrudVersionedState, String>(crudRepository, versionedConverter)
        repository.save("existing-1", CrudVersionedState("existing-1", "v1"))
        val afterFirstSave = mongoOperations.findById("existing-1", CrudVersionedState::class.java)!!

        repository.saveAll(
            mapOf(
                "existing-1" to afterFirstSave.copy(value = "v2"),
                "new-1" to CrudVersionedState("new-1", "v1"),
            )
        )

        val existingAfterBulk = mongoOperations.findById("existing-1", CrudVersionedState::class.java)!!
        assertThat(existingAfterBulk.value).isEqualTo("v2")
        assertThat(existingAfterBulk.version).isEqualTo(afterFirstSave.version!! + 1)
        assertThat(mongoOperations.findById("new-1", CrudVersionedState::class.java)?.version).isEqualTo(0L)
    }

    // --- exception translation: OptimisticLockingFailureException, DuplicateKeyException --------------------------

    @Test
    fun `saveAll surfaces OptimisticLockingFailureException when an entry's version has moved on, same as save`() {
        val mongoOperations = mongoOperations()
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudVersionedStateRepository::class.java)
        val repository = crudRepositoryViewStateRepository<CrudVersionedState, CrudVersionedState, String>(crudRepository, versionedConverter)
        repository.save("id-1", CrudVersionedState("id-1", "v1"))
        val staleRead = mongoOperations.findById("id-1", CrudVersionedState::class.java)!!
        repository.save("id-1", staleRead.copy(value = "v2-from-elsewhere"))

        val thrown = catchThrowable { repository.saveAll(mapOf("id-1" to staleRead.copy(value = "v2-stale"))) }

        assertThat(thrown).isInstanceOf(OptimisticLockingFailureException::class.java)
        assertThat(mongoOperations.findById("id-1", CrudVersionedState::class.java)?.value).isEqualTo("v2-from-elsewhere")
    }

    @Test
    fun `saveAll surfaces DuplicateKeyException for a unique index violation, same as an individual insert`() {
        val mongoOperations = mongoOperations()
        mongoOperations.indexOps(CrudUniqueFieldState::class.java).ensureIndex(Index().on("uniqueValue", ASC).unique())
        val crudRepository = MongoRepositoryFactory(mongoOperations).getRepository(CrudUniqueFieldStateRepository::class.java)
        val converter = object : StateConverter<CrudUniqueFieldState, CrudUniqueFieldState> {
            override fun toDTO(viewState: CrudUniqueFieldState): CrudUniqueFieldState = viewState
            override fun fromDTO(dto: CrudUniqueFieldState): CrudUniqueFieldState = dto
        }
        val repository = crudRepositoryViewStateRepository<CrudUniqueFieldState, CrudUniqueFieldState, String>(crudRepository, converter)
        val states = mapOf(
            "id-1" to CrudUniqueFieldState("id-1", "same-value"),
            "id-2" to CrudUniqueFieldState("id-2", "same-value"),
        )

        val thrown = catchThrowable { repository.saveAll(states) }

        assertThat(thrown).isInstanceOf(DuplicateKeyException::class.java)
    }
}
