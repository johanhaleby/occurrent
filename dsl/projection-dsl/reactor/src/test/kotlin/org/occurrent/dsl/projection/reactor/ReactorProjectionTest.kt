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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.jacksonCloudEventConverter
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.projection.projection
import org.occurrent.dsl.projection.singletonProjection
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
import reactor.test.StepVerifier
import java.net.URI
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

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
    fun id_scoped_pull_folds_a_query_into_the_same_state_as_push() {
        val defined = NameDefined(id(), Date(), "johan", "Johan")
        val changed = NameWasChanged(id(), Date(), "johan", "Johan Haleby")
        val queries = DomainEventQueries(InMemoryEventStoreQueries(cloudEvents(defined, changed)), converter)

        val state: String? = queries.project(currentNameProjection(), "johan").block()

        assertThat(state).isEqualTo("Johan Haleby")
    }

    @Test
    fun id_scoped_pull_folds_only_the_requested_instance_when_multiple_instances_exist() {
        val johanDefined = NameDefined(id(), Date(), "johan", "Johan")
        val johanChanged = NameWasChanged(id(), Date(), "johan", "Johan Haleby")
        val eveDefined = NameDefined(id(), Date(), "eve", "Eve")
        val queries = DomainEventQueries(InMemoryEventStoreQueries(cloudEvents(johanDefined, johanChanged, eveDefined)), converter)

        assertThat(queries.project(currentNameProjection(), "johan").block()).isEqualTo("Johan Haleby")
        assertThat(queries.project(currentNameProjection(), "eve").block()).isEqualTo("Eve")
    }

    @Test
    fun unqualified_pull_errors_for_a_keyed_projection() {
        val defined = NameDefined(id(), Date(), "johan", "Johan")
        val queries = DomainEventQueries(InMemoryEventStoreQueries(cloudEvents(defined)), converter)

        assertThatThrownBy { queries.project(currentNameProjection()).block() }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun unqualified_pull_defers_the_keyed_rejection_until_the_mono_is_subscribed_to() {
        val queries = DomainEventQueries(InMemoryEventStoreQueries(cloudEvents(NameDefined(id(), Date(), "johan", "Johan"))), converter)

        // Assembling the Mono for a keyed projection must not throw on the spot, only when subscribed to.
        val state = queries.project(currentNameProjection())

        StepVerifier.create(state).expectError(IllegalArgumentException::class.java).verify()
    }

    @Test
    fun unqualified_pull_completes_empty_when_a_nullable_state_projection_folds_to_null() {
        // Given a single-instance projection whose state models absence as null, and whose last fold lands back on null
        val projection = singletonProjection<String?, DomainEvent>(initialState = null) {
            on<NameDefined> { _, e -> e.name() }
            on<NameWasChanged> { _, _ -> null }
        }
        val queries = DomainEventQueries(
            InMemoryEventStoreQueries(
                cloudEvents(
                    NameDefined(id(), Date(), "johan", "Johan"),
                    NameWasChanged(id(), Date(), "johan", "Johan Haleby")
                )
            ),
            converter
        )

        // When the projection is folded on demand
        val state = queries.project(projection)

        // Then the Mono completes empty, since a Mono cannot carry the null the fold produced
        StepVerifier.create(state).verifyComplete()
    }

    @Test
    fun id_scoped_pull_completes_empty_when_a_nullable_state_projection_folds_to_null() {
        // Given a keyed nullable-state projection where one instance folds to null and another does not
        val projection = projection<String?, DomainEvent, String>(initialState = null) {
            id { it.userId() }
            on<NameDefined> { _, e -> e.name() }
            on<NameWasChanged> { _, _ -> null }
        }
        val queries = DomainEventQueries(
            InMemoryEventStoreQueries(
                cloudEvents(
                    NameDefined(id(), Date(), "johan", "Johan"),
                    NameWasChanged(id(), Date(), "johan", "Johan Haleby"),
                    NameDefined(id(), Date(), "eve", "Eve")
                )
            ),
            converter
        )

        // When each instance is folded on its own
        // Then johan completes empty and eve still emits, so the null is per instance rather than swallowing the read
        StepVerifier.create(queries.project(projection, "johan")).verifyComplete()
        StepVerifier.create(queries.project(projection, "eve")).expectNext("Eve").verifyComplete()
    }

    @Test
    fun unqualified_pull_folds_each_event_as_the_query_emits_it_rather_than_reading_them_all_first() {
        // Given a query that counts how much it has emitted, and a fold that records that count every time it runs
        val emitted = AtomicInteger()
        val countsSeenByTheFold = mutableListOf<Int>()
        val projection = singletonProjection<Int, DomainEvent>(initialState = 0) {
            on<NameDefined> { state, _ ->
                countsSeenByTheFold += emitted.get()
                state + 1
            }
        }
        val events = cloudEvents(
            NameDefined(id(), Date(), "a", "A"),
            NameDefined(id(), Date(), "b", "B"),
            NameDefined(id(), Date(), "c", "C")
        )
        val queries = DomainEventQueries(CountingEventStoreQueries(events, emitted), converter)

        // When the projection is folded on demand
        val folded = queries.project(projection).block()

        // Then every fold ran while the query was still emitting. Reading the whole history into a list first would
        // fold only after all three had arrived, and each fold would see 3.
        assertThat(folded).isEqualTo(3)
        assertThat(countsSeenByTheFold).containsExactly(1, 2, 3)
    }

    private fun cloudEvents(vararg events: DomainEvent): List<CloudEvent> = events.map { converter.toCloudEvent(it) }

    private fun id(): String = UUID.randomUUID().toString()

    /** Minimal in-memory [EventStoreQueries]: returns all supplied events in order (the fold no-ops on unhandled types). */
    private class InMemoryEventStoreQueries(private val events: List<CloudEvent>) : EventStoreQueries {
        override fun query(filter: Filter, skip: Int, limit: Int, sortBy: SortBy): Flux<CloudEvent> = Flux.fromIterable(events)
        override fun count(filter: Filter): Mono<Long> = Mono.just(events.size.toLong())
        override fun exists(filter: Filter): Mono<Boolean> = Mono.just(events.isNotEmpty())
    }

    /** As [InMemoryEventStoreQueries], but counts emissions so a test can tell an incremental fold from a buffered one. */
    private class CountingEventStoreQueries(private val events: List<CloudEvent>, private val emitted: AtomicInteger) : EventStoreQueries {
        override fun query(filter: Filter, skip: Int, limit: Int, sortBy: SortBy): Flux<CloudEvent> =
            Flux.fromIterable(events).doOnNext { emitted.incrementAndGet() }

        override fun count(filter: Filter): Mono<Long> = Mono.just(events.size.toLong())
        override fun exists(filter: Filter): Mono<Boolean> = Mono.just(events.isNotEmpty())
    }
}
