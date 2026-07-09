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

package org.occurrent.dsl.subscription.blocking

import com.fasterxml.jackson.databind.ObjectMapper
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
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.net.URI
import java.time.LocalDateTime
import java.util.concurrent.CopyOnWriteArrayList
import java.util.stream.Stream

@DisplayNameGeneration(ReplaceUnderscores::class)
class DefaultSubscriptionIdTest {

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
    fun default_subscription_id_is_the_cloud_event_type_not_the_class_simple_name() {
        val streamSubscriptions = StreamSubscriptions(subscriptionModel, cloudEventConverter)

        val derivedId = streamSubscriptions.defaultSubscriptionId(NameDefined::class)

        assertThat(derivedId).isEqualTo(cloudEventConverter.getCloudEventType(NameDefined::class.java))
        // The default cloud event type mapping uses the fully qualified class name, so the derived id must not be the
        // bare simple name that the old default used.
        assertThat(derivedId).isNotEqualTo(NameDefined::class.simpleName)
    }

    @Test
    fun default_subscription_id_is_stable_across_calls_so_a_checkpoint_key_survives() {
        val first = StreamSubscriptions(subscriptionModel, cloudEventConverter).defaultSubscriptionId(NameDefined::class)
        val second = StreamSubscriptions(subscriptionModel, cloudEventConverter).defaultSubscriptionId(NameDefined::class)

        // A subscription resumes from its checkpoint only if the same event type keeps mapping to the same id. The
        // derived id is deterministic, so a restart reconnects to the existing checkpoint rather than orphaning it.
        assertThat(first).isEqualTo(second)
    }

    @Test
    fun reified_subscribe_without_an_id_delivers_events_under_the_derived_id() {
        val received = CopyOnWriteArrayList<DomainEvent>()
        val nameDefined = NameDefined("eventId1", time, "name", "Some Doe")

        streamSubscriptions(subscriptionModel, cloudEventConverter) {
            subscribe<NameDefined> { received.add(it) }
        }

        eventStore.write("stream", cloudEventConverter.toCloudEvents(listOf<DomainEvent>(nameDefined)))

        await().untilAsserted {
            assertThat(received).containsExactly(nameDefined)
        }
    }
}
