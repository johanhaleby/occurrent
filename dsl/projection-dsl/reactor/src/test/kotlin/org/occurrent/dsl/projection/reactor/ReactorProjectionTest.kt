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

package org.occurrent.dsl.projection.reactor

import com.fasterxml.jackson.databind.ObjectMapper
import io.cloudevents.CloudEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.projection.projection
import org.occurrent.dsl.query.reactor.DomainEventQueries
import org.occurrent.dsl.subscription.reactor.streamSubscriptions
import org.occurrent.dsl.subscription.reactor.subscriptions
import org.occurrent.dsl.view.viewStateRepository
import org.occurrent.eventstore.api.SortBy
import org.occurrent.eventstore.api.reactor.EventStoreQueries
import org.occurrent.filter.Filter
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI
import java.util.Date
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Docker-free reactor tests for the projection runners. The push tests register a projection on the in-memory reactor
 * [SynchronousSubscriptionModel] and drive events through its dispatch, which composes the reactive update into the
 * returned `Mono`, so blocking on dispatch deterministically awaits materialization (this is also exactly the
 * read-your-writes path, the same model wired as a reactive application service's synchronous dispatcher). The pull test
 * folds a query, backed by a tiny in-memory [EventStoreQueries] stub, and asserts it equals the pushed state.
 */
@DisplayNameGeneration(ReplaceUnderscores::class)
class ReactorProjectionTest {

    private val converter: CloudEventConverter<DomainEvent> =
        jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent:projection"), DomainEvent::eventId)

    private fun currentNameProjection() = projection<String, DomainEvent, String>(initialState = "") {
        id { it.userId() }
        on<NameDefined> { _, e -> e.name() }
        on<NameWasChanged> { _, e -> e.name() }
    }

    @Test
    fun agnostic_push_materializes_the_read_model_from_dispatched_events() {
        val sync = SynchronousSubscriptionModel()
        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })

        subscriptions(sync, converter) {
            project("current-name", currentNameProjection(), repository)
        }

        sync.dispatch(cloudEvents(NameDefined(id(), Date(), "johan", "Johan"), NameWasChanged(id(), Date(), "johan", "Johan Haleby"))).block()

        assertThat(store["johan"]).isEqualTo("Johan Haleby")
    }

    @Test
    fun stream_push_materializes_the_read_model_from_dispatched_events() {
        val sync = SynchronousSubscriptionModel()
        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })

        streamSubscriptions(sync, converter) {
            project("current-name", currentNameProjection(), repository)
        }

        sync.dispatch(cloudEvents(NameDefined(id(), Date(), "johan", "Johan"))).block()

        assertThat(store["johan"]).isEqualTo("Johan")
    }

    @Test
    fun reactive_primitive_update_is_applied_synchronously_within_dispatch() {
        val sync = SynchronousSubscriptionModel()
        val store = ConcurrentHashMap<String, String>()
        val projection = currentNameProjection()

        // The reactive-primitive overload: the update composes into the writer's chain (the read-your-writes path).
        subscriptions(sync, converter) {
            project("current-name", projection, { e ->
                Mono.fromRunnable {
                    val current = store[e.userId()] ?: projection.view().initialState()
                    store[e.userId()] = projection.view().evolve(current, e)
                }
            })
        }

        sync.dispatch(cloudEvents(NameDefined(id(), Date(), "johan", "Johan Haleby"))).block()

        // No await: the update ran inside the dispatch Mono we blocked on.
        assertThat(store["johan"]).isEqualTo("Johan Haleby")
    }

    @Test
    fun an_explicit_filter_narrows_which_events_reach_the_projection() {
        val sync = SynchronousSubscriptionModel()
        val store = ConcurrentHashMap<String, String>()
        val repository = viewStateRepository<String, String>({ store[it] }, { id, s -> store[id] = s })
        // Handlers cover both event types, but the explicit filter selects only NameDefined, so NameWasChanged is
        // filtered out at the subscription and never reaches the fold.
        val projection = projection<String, DomainEvent, String>(initialState = "") {
            id { it.userId() }
            on<NameDefined> { _, e -> e.name() }
            on<NameWasChanged> { _, e -> e.name() }
            filter(Filter.type(converter.getCloudEventType(NameDefined::class.java)))
        }

        subscriptions(sync, converter) {
            project("current-name", projection, repository)
        }

        sync.dispatch(cloudEvents(NameDefined(id(), Date(), "johan", "Johan"), NameWasChanged(id(), Date(), "johan", "Johan Haleby"))).block()

        assertThat(store["johan"]).isEqualTo("Johan")
    }

    @Test
    fun pull_folds_a_query_into_the_same_state_as_push() {
        val defined = NameDefined(id(), Date(), "johan", "Johan")
        val changed = NameWasChanged(id(), Date(), "johan", "Johan Haleby")
        val queries = DomainEventQueries(InMemoryEventStoreQueries(cloudEvents(defined, changed)), converter)

        val state: String? = queries.project(currentNameProjection()).block()

        assertThat(state).isEqualTo("Johan Haleby")
    }

    private fun cloudEvents(vararg events: DomainEvent): List<CloudEvent> = events.map { converter.toCloudEvent(it) }

    private fun id(): String = UUID.randomUUID().toString()

    /** Minimal in-memory [EventStoreQueries]: returns all supplied events in order (the fold no-ops on unhandled types). */
    private class InMemoryEventStoreQueries(private val events: List<CloudEvent>) : EventStoreQueries {
        override fun query(filter: Filter, skip: Int, limit: Int, sortBy: SortBy): Flux<CloudEvent> = Flux.fromIterable(events)
        override fun count(filter: Filter): Mono<Long> = Mono.just(events.size.toLong())
        override fun exists(filter: Filter): Mono<Boolean> = Mono.just(events.isNotEmpty())
    }
}
