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

package org.occurrent.dsl.dcb.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import io.cloudevents.CloudEvent
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.subscription.blocking.EventMetadata
import org.occurrent.dsl.subscription.blocking.subscriptions
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.eventstore.api.dcb.DcbReadOptions
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.net.URI
import java.time.LocalDateTime
import java.util.concurrent.CopyOnWriteArrayList
import java.util.stream.Stream

@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbDslKotlinTest {

    private lateinit var eventStore: InMemoryEventStore
    private lateinit var subscriptionModel: InMemorySubscriptionModel
    private lateinit var cloudEventConverter: CloudEventConverter<DomainEvent>
    private lateinit var time: LocalDateTime

    @BeforeEach
    fun createInstances() {
        subscriptionModel = InMemorySubscriptionModel()
        eventStore = InMemoryEventStore(subscriptionModel)
        cloudEventConverter = JacksonCloudEventConverter.Builder<DomainEvent>(ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build()
        time = LocalDateTime.now()
    }

    @AfterEach
    fun shutdownSubscriptionModel() {
        subscriptionModel.shutdown()
    }

    @Test
    fun queryForSequence_and_queryForList_mirror_the_Java_query_methods() {
        val nameDefined = NameDefined("eventId1", time, "name", "Some Doe")
        val nameWasChanged = NameWasChanged("eventId2", time, "name", "Jane Doe")
        append("name:1", nameDefined, nameWasChanged)

        assertThat(eventStore.queryForSequence(DcbQuery.tagsAllOf("name:1"), cloudEventConverter).toList()).containsExactly(nameDefined, nameWasChanged)
        assertThat(eventStore.queryForList(DcbQuery.type(NameDefined::class.qualifiedName!!), cloudEventConverter)).containsExactly(nameDefined)
        assertThat(eventStore.queryForList(DcbQuery.tagsAllOf("name:1"), cloudEventConverter, DcbReadOptions.afterSequencePosition(1))).containsExactly(nameWasChanged)
    }

    @Test
    fun queryWithPosition_for_KClass_keeps_the_DCB_sequence_position() {
        val nameDefined = NameDefined("eventId1", time, "name", "Some Doe")
        append("other:1", NameWasChanged("eventId2", time, "name", "Jane Doe"))
        append("name:1", nameDefined)

        val eventStream = eventStore.queryWithPosition(DcbQuery.type(NameDefined::class.qualifiedName!!), cloudEventConverter)

        assertThat(eventStream.events()).containsExactly(nameDefined)
        assertThat(eventStream.lastSequencePosition()).isEqualTo(2)
    }

    @Test
    fun dcb_subscription_invokes_callback_only_for_matching_dcb_events() {
        val received = CopyOnWriteArrayList<DomainEvent>()

        subscriptionModel.subscribeDcb(
            subscriptionId = "subscription",
            cloudEventConverter = cloudEventConverter,
            query = DcbQuery.tagsAllOfExcludingTypes(listOf("name:1"), listOf(NameWasChanged::class.qualifiedName!!))
        ) {
            received.add(it)
        }

        val nameDefined = NameDefined("eventId1", time, "name", "Some Doe")
        append("name:1", nameDefined, NameWasChanged("eventId2", time, "name", "Jane Doe"))
        append("other:1", NameDefined("eventId3", time, "name", "Other Doe"))

        await().untilAsserted {
            assertThat(received).containsExactly(nameDefined)
        }
    }

    @Test
    fun dcb_subscription_all_query_ignores_normal_stream_events() {
        val received = CopyOnWriteArrayList<DomainEvent>()

        subscriptionModel.subscribeDcb("subscription", cloudEventConverter, DcbQuery.all()) {
            received.add(it)
        }

        writeStreamEvent(NameDefined("eventId1", time, "name", "Stream Doe"))
        val dcbEvent = NameDefined("eventId2", time, "name", "Dcb Doe")
        append("name:1", dcbEvent)

        await().untilAsserted {
            assertThat(received).containsExactly(dcbEvent)
        }
    }

    @Test
    fun dcb_subscription_metadata_exposes_stream_and_dcb_metadata() {
        val metadata = CopyOnWriteArrayList<EventMetadata>()

        subscriptionModel.subscribeDcb("subscription", cloudEventConverter, DcbQuery.tagsAllOf("name:1")) { eventMetadata, _ ->
            metadata.add(eventMetadata)
        }

        append(listOf("tenant:1", "name:1"), NameDefined("eventId1", time, "name", "Some Doe"))

        await().untilAsserted {
            assertThat(metadata).hasSize(1)
            assertThat(metadata[0].streamId).isEqualTo("dcb:partition:0")
            assertThat(metadata[0].streamVersion).isEqualTo(1)
            assertThat(metadata[0].dcbPosition).isEqualTo(1)
            assertThat(metadata[0].dcbTags).containsExactlyInAnyOrder("name:1", "tenant:1")
        }
    }

    @Test
    fun normal_subscription_metadata_can_read_dcb_metadata_when_dcb_extensions_are_imported() {
        val metadata = CopyOnWriteArrayList<EventMetadata>()

        subscriptions(subscriptionModel, cloudEventConverter) {
            subscribe("subscription") { eventMetadata: EventMetadata, _: DomainEvent ->
                metadata.add(eventMetadata)
            }
        }

        append("name:1", NameDefined("eventId1", time, "name", "Some Doe"))

        await().untilAsserted {
            assertThat(metadata).hasSize(1)
            assertThat(metadata[0].streamId).isEqualTo("dcb:partition:0")
            assertThat(metadata[0].streamVersion).isEqualTo(1)
            assertThat(metadata[0].dcbPosition).isEqualTo(1)
            assertThat(metadata[0].dcbTags).containsExactly("name:1")
        }
    }

    @Test
    fun normal_subscription_metadata_reports_missing_dcb_metadata_for_stream_events() {
        val metadata = CopyOnWriteArrayList<EventMetadata>()

        subscriptions(subscriptionModel, cloudEventConverter) {
            subscribe("subscription") { eventMetadata: EventMetadata, _: DomainEvent ->
                metadata.add(eventMetadata)
            }
        }

        writeStreamEvent(NameDefined("eventId1", time, "name", "Stream Doe"))

        await().untilAsserted {
            assertThat(metadata).hasSize(1)
            assertThat(metadata[0].streamId).isEqualTo("stream")
            assertThat(metadata[0].streamVersion).isEqualTo(1)
            assertThat(metadata[0].dcbPosition).isNull()
            assertThat(metadata[0].dcbTags).isEmpty()
        }
    }

    private fun append(tag: String, vararg events: DomainEvent) {
        append(listOf(tag), *events)
    }

    private fun append(tags: List<String>, vararg events: DomainEvent) {
        val cloudEvents: List<CloudEvent> = cloudEventConverter.toCloudEvents(Stream.of(*events))
            .map { event -> DcbCloudEvents.withTags(event, tags) }
            .toList()
        eventStore.append("dcb:partition:0", cloudEvents)
    }

    private fun writeStreamEvent(event: DomainEvent) {
        eventStore.write("stream", cloudEventConverter.toCloudEvents(Stream.of(event)))
    }
}
