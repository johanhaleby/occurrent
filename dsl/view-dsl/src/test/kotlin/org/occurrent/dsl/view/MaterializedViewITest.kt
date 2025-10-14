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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.RegisterExtension
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.view.testsupport.NameState
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findOne
import org.springframework.data.mongodb.core.query.Criteria.where
import org.springframework.data.mongodb.core.query.Query.query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*

@DisplayName("MaterializedView")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
@Testcontainers
class MaterializedViewITest {

    @RegisterExtension
    val flushMongoDBExtension: FlushMongoDBExtension = FlushMongoDBExtension(ConnectionString(mongoDBContainer.replicaSetUrl))

    companion object {
        @Suppress("unused")
        @Container
        val mongoDBContainer: MongoDBContainer = MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReuse(true)

        private val view: View<NameState?, DomainEvent> = view<NameState?, DomainEvent>(null) { s, e ->
            when (e) {
                is NameDefined -> NameState(e.userId(), e.name)
                is NameWasChanged -> s!!.copy(name = e.name)
            }
        }
    }

    lateinit var mongoOperations: MongoOperations

    lateinit var materializedView: MaterializedView<DomainEvent>

    @BeforeEach
    fun `create materialized view`() {
        val connectionString = ConnectionString(mongoDBContainer.replicaSetUrl + ".events")
        val mongoClient = MongoClients.create(connectionString)
        mongoOperations = MongoTemplate(mongoClient, connectionString.database!!)
        materializedView = view.materialized(mongoOperations) { e ->
            e.userId()
        }
    }

    @Test
    fun `update from empty`() {
        // Given
        val userId = UUID.randomUUID().toString()
        val nameDefined = NameDefined(UUID.randomUUID().toString(), Date(), userId, "Name")

        // When
        materializedView.update(nameDefined)

        // Then
        val savedState = findState(userId)
        assertThat(savedState).isEqualTo(NameState(userId, "Name", version = 0))
    }

    @Test
    fun `update from existing`() {
        // Given
        val userId = UUID.randomUUID().toString()
        mongoOperations.save(NameState(userId, "Name"))
        val nameWasChanged = NameWasChanged(UUID.randomUUID().toString(), Date(), userId, "New Name")

        // When
        materializedView.update(nameWasChanged)

        // Then
        val savedState = findState(userId)
        assertThat(savedState).isEqualTo(NameState(userId, "New Name", version = 1))
    }


    private fun MaterializedViewITest.findState(userId: String): NameState? {
        val savedState = mongoOperations.findOne<NameState>(query(where("userId").isEqualTo(userId)))
        return savedState
    }
}