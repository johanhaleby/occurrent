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
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.CheckpointAwareCloudEvent
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.StringBasedCheckpoint
import org.occurrent.subscription.SubscriptionFilter
import org.occurrent.subscription.api.blocking.Subscribable
import org.occurrent.subscription.api.blocking.Subscription
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.net.URI
import java.time.LocalDateTime
import java.util.Optional
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.Consumer

@DisplayNameGeneration(ReplaceUnderscores::class)
class SubscriptionMetadataCheckpointTest {

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
    fun the_stream_dsl_hands_the_subscriber_the_checkpoint_the_subscription_model_delivered_the_event_with() {
        // Given
        val checkpoints = CopyOnWriteArrayList<Optional<String>>()
        streamSubscriptions(deliveringCheckpoint("position:1;origin:0;liveFrom:token"), cloudEventConverter) {
            subscribe<NameDefined>("subscription") { metadata, _ -> checkpoints.add(metadata.getCheckpoint()) }.waitUntilStarted()
        }

        // When
        write(NameDefined("eventId1", time, "name", "Some Doe"))

        // Then
        await().untilAsserted { assertThat(checkpoints).containsExactly(Optional.of("position:1;origin:0;liveFrom:token")) }
    }

    @Test
    fun the_capability_agnostic_dsl_hands_the_subscriber_the_checkpoint_the_subscription_model_delivered_the_event_with() {
        // Given
        val checkpoints = CopyOnWriteArrayList<Optional<String>>()
        subscriptions(deliveringCheckpoint("position:1;origin:0;liveFrom:token"), cloudEventConverter) {
            subscribe<NameDefined>("subscription") { metadata, _ -> checkpoints.add(metadata.getCheckpoint()) }.waitUntilStarted()
        }

        // When
        write(NameDefined("eventId1", time, "name", "Some Doe"))

        // Then
        await().untilAsserted { assertThat(checkpoints).containsExactly(Optional.of("position:1;origin:0;liveFrom:token")) }
    }

    @Test
    fun the_stream_dsl_hands_the_subscriber_no_checkpoint_when_the_subscription_model_delivers_none() {
        // Given
        val checkpoints = CopyOnWriteArrayList<Optional<String>>()
        streamSubscriptions(subscriptionModel, cloudEventConverter) {
            subscribe<NameDefined>("subscription") { metadata, _ -> checkpoints.add(metadata.getCheckpoint()) }.waitUntilStarted()
        }

        // When
        write(NameDefined("eventId1", time, "name", "Some Doe"))

        // Then
        await().untilAsserted { assertThat(checkpoints).containsExactly(Optional.empty()) }
    }

    private fun write(event: DomainEvent) {
        eventStore.write("stream", cloudEventConverter.toCloudEvents(listOf(event)))
    }

    // Hands every event to the subscriber the way a catch-up model hands one it replays
    private fun deliveringCheckpoint(checkpoint: String): Subscribable = object : Subscribable {
        override fun subscribe(subscriptionId: String, filter: SubscriptionFilter?, startAt: StartAt, action: Consumer<CloudEvent>): Subscription =
            subscriptionModel.subscribe(subscriptionId, filter, startAt) { action.accept(CheckpointAwareCloudEvent(it, StringBasedCheckpoint(checkpoint))) }
    }
}
