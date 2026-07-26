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
import org.bson.types.ObjectId
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.dsl.view.internal.requireMatchingDocumentId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findById
import org.springframework.data.mongodb.core.mapping.Document
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer

@Document(collection = "coercion-objectid")
data class ObjectIdDoc(@Id val id: ObjectId, val name: String)

@Document(collection = "coercion-long")
data class LongIdDoc(@Id val id: Long, val name: String)

@Document(collection = "coercion-generated")
data class GeneratedIdDoc(@Id val id: String?, val name: String)

/**
 * Pins the id pairings the document-id guard must NOT reject. Spring Data converts a lookup id to the id property's
 * declared type, so a hex String against an `ObjectId` id and an Int against a Long are working pairings. An earlier
 * version of the guard compared them raw and rejected both, with a message that read as though two identical values
 * differed.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores::class)
class DocumentIdCoercionTest {

    companion object {
        @Suppress("unused")
        @Container
        val mongoDBContainer: MongoDBContainer =
            MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReuse(true)
    }

    private fun mongoOperations(): MongoOperations {
        val connectionString = ConnectionString(mongoDBContainer.getReplicaSetUrl("document-id-coercion"))
        return MongoTemplate(MongoClients.create(connectionString), connectionString.database!!)
    }

    @Test
    fun `a hex String resolved against an ObjectId id is a working pairing, not a mismatch`() {
        val mongoOperations = mongoOperations()
        val oid = ObjectId()
        mongoOperations.save(ObjectIdDoc(oid, "Johan"))

        // Spring Data finds it by the hex form, so the guard must not treat the pairing as a mismatch.
        assertThat(mongoOperations.findById<ObjectIdDoc>(oid.toHexString())).isEqualTo(ObjectIdDoc(oid, "Johan"))
        assertThatCode {
            requireMatchingDocumentId(mongoOperations, ObjectIdDoc::class.java, ObjectIdDoc(oid, "Johan"), oid.toHexString())
        }.doesNotThrowAnyException()
    }

    @Test
    fun `an Int resolved against a Long id is a working pairing, not a mismatch`() {
        val mongoOperations = mongoOperations()

        assertThatCode {
            requireMatchingDocumentId(mongoOperations, LongIdDoc::class.java, LongIdDoc(1L, "Johan"), 1)
        }.doesNotThrowAnyException()
    }

    @Test
    fun `a document carrying no id value is left alone rather than rejected`() {
        val mongoOperations = mongoOperations()

        assertThatCode {
            requireMatchingDocumentId(mongoOperations, GeneratedIdDoc::class.java, GeneratedIdDoc(null, "Johan"), "some-key")
        }.doesNotThrowAnyException()
    }

    @Test
    fun `two ids of the same type that differ are still rejected`() {
        val mongoOperations = mongoOperations()

        assertThatCode {
            requireMatchingDocumentId(mongoOperations, LongIdDoc::class.java, LongIdDoc(1L, "Johan"), 2L)
        }.isInstanceOf(IllegalStateException::class.java)
    }
}
