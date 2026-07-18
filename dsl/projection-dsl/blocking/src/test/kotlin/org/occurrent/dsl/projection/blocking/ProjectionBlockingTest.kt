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

package org.occurrent.dsl.projection.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.projection.Projection
import org.occurrent.dsl.projection.projection
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.subscription.blocking.streamSubscriptions
import org.occurrent.dsl.subscription.blocking.subscriptions
import org.occurrent.dsl.view.viewStateRepository
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.filter.Filter
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.net.URI
import java.util.Date
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@DisplayNameGeneration(ReplaceUnderscores::class)
class ProjectionBlockingTest {

    private lateinit var subscriptionModel: InMemorySubscriptionModel
    private lateinit var eventStore: InMemoryEventStore
    private lateinit var converter: CloudEventConverter<DomainEvent>

    @BeforeEach
    fun setup() {
        subscriptionModel = InMemorySubscriptionModel()
        eventStore = InMemoryEventStore(subscriptionModel)
        converter = jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent:projection"), DomainEvent::eventId)
    }

    @AfterEach
    fun shutdown() {
        subscriptionModel.shutdown()
    }

    private fun currentNameProjection(): Projection<String, DomainEvent, String> =
        projection(initialState = "") {
            id { it.userId() }
            on<NameDefined> { _, e -> e.name() }
            on<NameWasChanged> { _, e -> e.name() }
        }

    private fun write(streamId: String, vararg events: DomainEvent) {
        eventStore.write(streamId, events.toList().map(converter::toCloudEvent))
    }

    @Test
    fun agnostic_project_creates_the_subscription_and_materializes_the_view() {
        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })

        subscriptions(subscriptionModel, converter) {
            project("current-name", currentNameProjection(), repository)
        }

        write(
            "johan",
            NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"),
            NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"),
        )

        await untilAsserted { assertThat(store["johan"]).isEqualTo("Johan Haleby") }
    }

    @Test
    fun stream_project_creates_the_subscription_and_materializes_the_view() {
        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })

        streamSubscriptions(subscriptionModel, converter) {
            project("current-name", currentNameProjection(), repository)
        }

        write("johan", NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"))

        await untilAsserted { assertThat(store["johan"]).isEqualTo("Johan") }
    }

    @Test
    fun the_subscription_filter_is_derived_from_the_registered_handlers() {
        // The projection handles only NameWasChanged, so NameDefined must not reach the view.
        val onlyChangesProjection: Projection<String, DomainEvent, String> = projection(initialState = "unset") {
            id { it.userId() }
            on<NameWasChanged> { _, e -> e.name() }
        }
        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })

        subscriptions(subscriptionModel, converter) {
            project("only-changes", onlyChangesProjection, repository)
        }

        write("johan", NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"))
        write("johan", NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"))

        // The first NameWasChanged folds from the initial state, not from a NameDefined the projection never saw.
        await untilAsserted { assertThat(store["johan"]).isEqualTo("Johan Haleby") }
    }

    @Test
    fun an_explicit_filter_overrides_the_type_derived_selector() {
        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })
        val projectionWithFilter: Projection<String, DomainEvent, String> = projection(initialState = "") {
            id { it.userId() }
            on<NameDefined> { _, e -> e.name() }
            on<NameWasChanged> { _, e -> e.name() }
            filter(Filter.streamId("johan"))
        }

        subscriptions(subscriptionModel, converter) {
            project("only-johan", projectionWithFilter, repository)
        }

        write("johan", NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"))
        write("eve", NameDefined(UUID.randomUUID().toString(), Date(), "eve", "Eve"))

        await untilAsserted { assertThat(store["johan"]).isEqualTo("Johan") }
        assertThat(store).doesNotContainKey("eve")
    }

    @Test
    fun id_scoped_pull_projection_folds_the_matching_events_on_demand_and_matches_the_pushed_state() {
        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })
        subscriptions(subscriptionModel, converter) {
            project("current-name", currentNameProjection(), repository)
        }

        write(
            "johan",
            NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"),
            NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"),
        )
        await untilAsserted { assertThat(store["johan"]).isEqualTo("Johan Haleby") }

        val queries = DomainEventQueries(eventStore, converter)
        val pulled = queries.project(currentNameProjection(), "johan")

        assertThat(pulled).isEqualTo("Johan Haleby").isEqualTo(store["johan"])
    }

    @Test
    fun id_scoped_pull_projection_folds_only_the_requested_instance_when_multiple_instances_exist() {
        write("johan", NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"), NameWasChanged(UUID.randomUUID().toString(), Date(), "johan", "Johan Haleby"))
        write("eve", NameDefined(UUID.randomUUID().toString(), Date(), "eve", "Eve"))

        val queries = DomainEventQueries(eventStore, converter)

        assertThat(queries.project(currentNameProjection(), "johan")).isEqualTo("Johan Haleby")
        assertThat(queries.project(currentNameProjection(), "eve")).isEqualTo("Eve")
    }

    @Test
    fun unqualified_pull_projection_throws_for_a_keyed_projection() {
        write("johan", NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"))

        val queries = DomainEventQueries(eventStore, converter)

        assertThatThrownBy { queries.project(currentNameProjection()) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
