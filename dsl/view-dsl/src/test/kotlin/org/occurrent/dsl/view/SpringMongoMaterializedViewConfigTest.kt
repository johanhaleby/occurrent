/*
 *
 *  Copyright 2025 Johan Haleby
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
import io.github.serpro69.kfaker.faker
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.view.DuplicateKeyHandling.Companion.ignore
import org.occurrent.dsl.view.SpringMongoViewConfig.Companion.config
import org.occurrent.dsl.view.testsupport.NameState
import org.occurrent.dsl.view.testsupport.nameChanged
import org.occurrent.dsl.view.testsupport.nameDefined
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findById
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference


@Timeout(10)
@Testcontainers
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class SpringMongoMaterializedViewConfigTest {

    companion object {
        @JvmStatic
        @Container
        val mongoDBContainer: MongoDBContainer = MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReuse(true)


        private val nameView: View<NameState?, DomainEvent> = view<NameState?, DomainEvent>(null) { s, e ->
            when (e) {
                is NameDefined -> NameState(e.userId(), e.name)
                is NameWasChanged -> s!!.copy(name = e.name)
            }
        }
    }

    val faker = faker { }

    @RegisterExtension
    var flushMongoDBExtension: FlushMongoDBExtension = FlushMongoDBExtension(ConnectionString(mongoDBContainer.replicaSetUrl))

    lateinit var mongoOperations: MongoOperations

    @BeforeEach
    fun initializeMongoOperations() {
        val connectionString = ConnectionString(mongoDBContainer.replicaSetUrl + ".events")
        val mongoClient = MongoClients.create(connectionString)
        mongoOperations = MongoTemplate(mongoClient, connectionString.database!!)
    }

    @Nested
    @DisplayName("OptimisticLockingHandling")
    inner class OptimisticLockingHandlingTests {

        @Nested
        @DisplayName("default")
        inner class Default {

            @Test
            fun `retries OptimisticLockingFailureException`() {
                // Given
                val (userId) = saveState()
                val materializedView = materializedView(config())

                // When
                val names = materializedView.updateNameInParallel(userId)

                // Then
                val state = getState(userId)
                assertAll(
                    { assertThat(state.name).isIn(names) },
                    { assertThat(state.version).isEqualTo(2) },
                )
            }
        }

        @Nested
        @DisplayName("ignore")
        inner class Ignore {

            @Test
            fun `ignores OptimisticLockingFailureException`() {
                // Given
                val ref = AtomicReference<OptimisticLockingFailureException?>(null)
                val (userId) = saveState()
                val materializedView = materializedView(config(optimisticLockingHandling = OptimisticLockingHandling.ignore { e ->
                    ref.set(e)
                }))

                // When
                val names = materializedView.updateNameInParallel(userId)

                // Then
                val state = getState(userId)
                assertAll(
                    { assertThat(state.name).isIn(names) },
                    { assertThat(state.version).isEqualTo(1L) },
                    { assertThat(ref.get()).isExactlyInstanceOf(OptimisticLockingFailureException::class.java) },
                )
            }
        }

        @Nested
        @DisplayName("rethrow")
        inner class Rethrow {

            @Test
            fun `rethrows OptimisticLockingFailureException`() {
                // Given
                val (userId) = saveState()
                val materializedView = materializedView(config(optimisticLockingHandling = OptimisticLockingHandling.rethrow()))

                // When
                val throwable = catchThrowable { materializedView.updateNameInParallel(userId) }

                // Then
                val state = getState(userId)
                assertAll(
                    { assertThat(state.version).isEqualTo(1L) }, // Only one name update
                    { assertThat(throwable).isExactlyInstanceOf(OptimisticLockingFailureException::class.java) },
                )
            }
        }

        private fun MaterializedView<DomainEvent>.updateNameInParallel(userId: String): List<String> {
            val barrier = CyclicBarrier(2)
            val name1 = randomName()
            val name2 = randomName()
            val pool = Executors.newFixedThreadPool(2)

            val f1 = pool.submit<String> {
                barrier.await()
                update(nameChanged(userId, name1))
                name1
            }
            val f2 = pool.submit<String> {
                barrier.await()
                update(nameChanged(userId, name2))
                name2
            }

            return try {
                listOf(f1.get(), f2.get())
            } catch (e: ExecutionException) {
                val cause = e.cause ?: e
                throw cause
            } finally {
                pool.shutdown()
            }
        }
    }

    @Nested
    @DisplayName("DuplicateKeyHandling")
    inner class DuplicateKeyHandlingTests {

        @Nested
        @DisplayName("default")
        inner class Default {

            @Test
            fun `ignores DuplicateKeyException`() {
                // Given
                val (userId, name) = saveState()
                val materializedView = materializedView(config())

                // When
                materializedView.update(nameDefined(userId, name))

                // Then
                assertThat(getState(userId)).isEqualTo(NameState(userId, name, version = 0))
            }
        }

        @Nested
        @DisplayName("ignore")
        inner class Ignore {

            @Test
            fun `ignores DuplicateKeyException`() {
                // Given
                val ignored = AtomicBoolean(false)
                val (userId, name) = saveState()
                val materializedView = materializedView(config(duplicateKeyHandling = ignore {
                    ignored.set(true)
                }))

                // When
                materializedView.update(nameDefined(userId, name))

                // Then
                assertAll(
                    { assertThat(getState(userId)).isEqualTo(NameState(userId, name, version = 0)) },
                    { assertThat(ignored.get()).isTrue() }
                )
            }
        }

        @Nested
        @DisplayName("rethrow")
        inner class RethrowDuplicateKey {

            @Test
            fun `rethrows DuplicateKeyException`() {
                // Given
                val (userId, name) = saveState()
                val materializedView = materializedView(config(duplicateKeyHandling = DuplicateKeyHandling.rethrow()))

                // When
                val throwable = catchThrowable { materializedView.update(nameDefined(userId, name)) }

                // Then
                assertAll(
                    { assertThat(getState(userId)).isEqualTo(NameState(userId, name, version = 0)) },
                    { assertThat(throwable).isExactlyInstanceOf(DuplicateKeyException::class.java) }
                )
            }
        }
    }

    private fun materializedView(config: SpringMongoViewConfig): MaterializedView<DomainEvent> = nameView.materialized(mongoOperations, config, DomainEvent::userId)

    private fun saveState(userId: String = UUID.randomUUID().toString(), name: String = randomName(), version: Long? = null): NameState = mongoOperations.save(NameState(userId, name, version))

    private fun randomName(): String = faker.name.name()
    private fun getState(userId: String): NameState = mongoOperations.findById<NameState>(userId)!!
}


