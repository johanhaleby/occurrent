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
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findById
import org.springframework.data.mongodb.core.mapping.Document
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import java.util.UUID

@Document(collection = "stream-keyed-name-state")
data class StreamKeyedNameState(@Id val streamId: String, val name: String, val streamVersion: Long)

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
        val mongoDBContainer: MongoDBContainer = MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReuse(true)

        // Keyed and folded purely from the metadata, so only the metadata-aware materialized(...) overload can drive it.
        private val streamKeyedView: View<StreamKeyedNameState?, DomainEvent> = view<StreamKeyedNameState?, DomainEvent>(null) { s, m, e ->
            when (e) {
                is NameDefined -> StreamKeyedNameState(m.getStreamId(), e.name, m.getStreamVersion())
                is NameWasChanged -> s!!.copy(name = e.name, streamVersion = m.getStreamVersion())
                else -> s
            }
        }
    }

    @RegisterExtension
    val flushMongoDBExtension: FlushMongoDBExtension = FlushMongoDBExtension(ConnectionString(mongoDBContainer.replicaSetUrl))

    lateinit var mongoOperations: MongoOperations

    private fun mongoOperations(): MongoOperations {
        val connectionString = ConnectionString(mongoDBContainer.replicaSetUrl + ".stream-keyed-events")
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
}
