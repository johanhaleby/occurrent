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
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions
import org.occurrent.dsl.projection.DcbProjection
import org.occurrent.dsl.projection.dcbProjection
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.view.viewStateRepository
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.net.URI
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * Proves the DCB projection runner against issue #194: a single-instance, tag-scoped boolean projection built with the
 * `dcbProjection { ... }` DSL (initial state + per-event-type handlers + a tag filter), run both push (subscription-fed)
 * and pull (query-folded).
 */
@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbProjectionBlockingTest {

    private lateinit var subscriptionModel: InMemorySubscriptionModel
    private lateinit var eventStore: InMemoryEventStore
    private lateinit var converter: CloudEventConverter<DomainEvent>
    private lateinit var dcbSubscriptions: DcbSubscriptions<DomainEvent>
    private lateinit var dcbQueries: DcbDomainEventQueries<DomainEvent>

    @BeforeEach
    fun setup() {
        subscriptionModel = InMemorySubscriptionModel()
        eventStore = InMemoryEventStore(subscriptionModel)
        converter = jacksonCloudEventConverter(ObjectMapper(), URI.create("urn:occurrent:projection"), DomainEvent::eventId)
        dcbSubscriptions = DcbSubscriptions(subscriptionModel, converter)
        dcbQueries = DcbDomainEventQueries(DomainEventQueries(eventStore, converter))
    }

    @AfterEach
    fun shutdown() {
        subscriptionModel.shutdown()
    }

    // Issue #194: createProjection({ initialState, handlers, tagFilter }), parameterized per key.
    private fun isNameClaimedProjection(name: String): DcbProjection<Boolean, DomainEvent, String> =
        dcbProjection(initialState = false) {
            tags("name:$name")
            id { name }
            on<NameDefined> { _, _ -> true }
        }

    private fun append(tag: String, vararg events: DomainEvent) {
        val parsed = listOf(Tag.parse(tag))
        val cloudEvents = converter.toCloudEvents(events.toList()).map { DcbCloudEvents.withTags(it, parsed) }
        eventStore.append(cloudEvents)
    }

    @Test
    fun dcb_push_projection_materializes_the_tag_scoped_boolean() {
        val store = ConcurrentHashMap<String, Boolean>()
        val repository = viewStateRepository<Boolean, String>({ store[it] }, { id, s -> store[id] = s })

        dcbSubscriptions.project("is-name-claimed", isNameClaimedProjection("johan"), repository)

        append("name:johan", NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"))

        await untilAsserted { assertThat(store["johan"]).isTrue() }
    }

    @Test
    fun dcb_pull_projection_folds_the_tag_scoped_events_on_demand() {
        append("name:johan", NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"))

        assertThat(dcbQueries.project(isNameClaimedProjection("johan"))).isTrue()
    }

    @Test
    fun dcb_pull_projection_is_the_initial_state_when_the_tag_is_unclaimed() {
        append("name:johan", NameDefined(UUID.randomUUID().toString(), Date(), "johan", "Johan"))

        // "eve" was never claimed, so its tag-scoped read matches nothing and the fold stays at the initial state.
        assertThat(dcbQueries.project(isNameClaimedProjection("eve"))).isFalse()
    }
}
