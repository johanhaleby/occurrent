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

package org.occurrent.dsl.projection.blocking.docs

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.application.service.blocking.TransactionExecutor
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.projection.Projection
import org.occurrent.dsl.projection.projection
import org.occurrent.dsl.projection.blocking.ProjectionRunner
// The on-demand fold is an extension on DomainEventQueries declared in org.occurrent.dsl.projection.blocking. This test
// sits in a subpackage of that, and Kotlin does not inherit extensions from a parent package, so it must be imported.
import org.occurrent.dsl.projection.blocking.project
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.view.viewStateRepository
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel
import java.net.URI
import java.util.Date
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * The projections the documentation's Testing chapter shows, kept compiling and passing here so a published snippet
 * cannot drift from the API. Covers the pure fold with no store, the asynchronous subscription-fed store, the
 * read-your-writes synchronous model, and the agreement between the push-fed store and the on-demand pull query.
 */
@DisplayName("DocumentedProjection (Kotlin)")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class DocumentedProjectionKotlinTest {

    @Nested
    @DisplayName("when folding purely, with no store or subscription")
    inner class WhenFoldingPurelyWithNoStoreOrSubscription {

        @Test
        fun `folds the initial state through the registered handlers`() {
            // Given
            val view = currentNameProjection().view()

            // When
            val afterDefine = view.evolve(view.initialState(), NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"))
            val afterChange = view.evolve(afterDefine, NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"))

            // Then
            assertThat(afterChange).isEqualTo("Johan Haleby")
        }

        @Test
        fun `an event type with no registered handler leaves the state unchanged`() {
            // Given
            val onlyDefinedProjection: Projection<String, DomainEvent, String> = projection(initialState = "unset") {
                id { it.userId() }
                on<NameDefined> { _, e -> e.name() }
            }
            val view = onlyDefinedProjection.view()

            // When
            val state = view.evolve(view.initialState(), NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"))

            // Then
            assertThat(state).isEqualTo("unset")
        }
    }

    @Nested
    @DisplayName("when projected into a store through a subscription")
    inner class WhenProjectedIntoAStoreThroughASubscription {

        private lateinit var subscriptionModel: InMemorySubscriptionModel
        private lateinit var eventStore: InMemoryEventStore
        private lateinit var converter: CloudEventConverter<DomainEvent>

        @BeforeEach
        fun setup() {
            subscriptionModel = InMemorySubscriptionModel()
            eventStore = InMemoryEventStore(subscriptionModel)
            converter = jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent:projection-docs"), DomainEvent::eventId)
        }

        @AfterEach
        fun shutdown() {
            subscriptionModel.shutdown()
        }

        @Test
        fun `the store eventually holds the folded state`() {
            // Given
            val store = ConcurrentHashMap<String, String>()
            val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })
            ProjectionRunner.agnostic(subscriptionModel, converter).project("current-name", currentNameProjection(), repository)

            // When
            write("johan",
                NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"),
                NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"),
            )

            // Then
            assertThat(subscriptionModel.waitUntilAllEventsProcessed()).isTrue()
            assertThat(store["johan"]).isEqualTo("Johan Haleby")
        }

        private fun write(streamId: String, vararg events: DomainEvent) {
            eventStore.write(streamId, events.toList().map(converter::toCloudEvent))
        }
    }

    @Nested
    @DisplayName("when read after write on the synchronous subscription model")
    inner class WhenReadAfterWriteIsSynchronous {

        @Test
        fun `the projection is visible immediately after execute returns`() {
            // Given
            val converter = jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent:projection-docs"), DomainEvent::eventId)
            val eventStore = InMemoryEventStore()
            val synchronousSubscriptions = SynchronousSubscriptionModel()

            val store = ConcurrentHashMap<String, String>()
            val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })

            ProjectionRunner.agnostic(synchronousSubscriptions, converter).project("current-name", currentNameProjection(), repository)

            val applicationService = GenericApplicationService.builder(eventStore, converter)
                .synchronousSubscriptions(synchronousSubscriptions)
                .transactionExecutor(TransactionExecutor.noTransaction())
                .build()

            // When
            applicationService.execute("johan") { _ ->
                listOf(NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"))
            }

            // Then
            // No await: the projection was updated synchronously, within execute(...), so it must already be visible.
            // An await here would pass whether the update was synchronous or merely fast, and would not prove the point.
            assertThat(store["johan"]).isEqualTo("Johan Haleby")
        }
    }

    @Nested
    @DisplayName("when push and pull are compared for the same projection")
    inner class WhenPushAndPullAgreeOnTheSameProjection {

        private lateinit var subscriptionModel: InMemorySubscriptionModel
        private lateinit var eventStore: InMemoryEventStore
        private lateinit var converter: CloudEventConverter<DomainEvent>

        @BeforeEach
        fun setup() {
            subscriptionModel = InMemorySubscriptionModel()
            eventStore = InMemoryEventStore(subscriptionModel)
            converter = jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent:projection-docs"), DomainEvent::eventId)
        }

        @AfterEach
        fun shutdown() {
            subscriptionModel.shutdown()
        }

        @Test
        fun `the pushed store state equals the pulled query state for the same instance`() {
            // Given
            val store = ConcurrentHashMap<String, String>()
            val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })
            ProjectionRunner.agnostic(subscriptionModel, converter).project("current-name", currentNameProjection(), repository)

            // When
            write("johan",
                NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"),
                NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"),
            )
            // A second instance, so the pull side has to scope to one of them. With a single instance the scoping is a
            // no-op and this test cannot tell a correctly scoped fold from one that folds everything.
            write("eve", NameDefined(UUID.randomUUID().toString(), Date(), "eve", "Eve"))
            assertThat(subscriptionModel.waitUntilAllEventsProcessed()).isTrue()
            assertThat(store["johan"]).isEqualTo("Johan Haleby")

            val queries = DomainEventQueries(eventStore, converter)
            val pulled = queries.project(currentNameProjection(), "johan")

            // Then
            // The pull folds the same events on demand, so it must agree with what the push side already materialized
            // rather than merely equal a hardcoded expectation.
            assertThat(pulled).isEqualTo(store["johan"])
        }

        private fun write(streamId: String, vararg events: DomainEvent) {
            eventStore.write(streamId, events.toList().map(converter::toCloudEvent))
        }
    }

    companion object {

        private fun currentNameProjection(): Projection<String, DomainEvent, String> =
            projection(initialState = "") {
                id { it.userId() }
                on<NameDefined> { _, e -> e.name() }
                on<NameWasChanged> { _, e -> e.name() }
            }
    }
}
