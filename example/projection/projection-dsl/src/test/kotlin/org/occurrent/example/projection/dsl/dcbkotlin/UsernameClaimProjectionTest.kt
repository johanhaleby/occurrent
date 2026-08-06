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

package org.occurrent.example.projection.dsl.dcbkotlin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions
import org.occurrent.dsl.projection.blocking.project
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.view.viewStateRepository
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.util.concurrent.ConcurrentHashMap

/**
 * Issue #194: a single-instance, tag-scoped boolean projection built with `dcbProjection { ... }`, run both push
 * (subscription-fed) and pull (query-folded on demand) from the same descriptor.
 */
@DisplayNameGeneration(ReplaceUnderscores::class)
class UsernameClaimProjectionTest {

    private lateinit var subscriptionModel: InMemorySubscriptionModel
    private lateinit var eventStore: InMemoryEventStore
    private lateinit var converter: CloudEventConverter<AccountEvent>
    private lateinit var dcbSubscriptions: DcbSubscriptions<AccountEvent>
    private lateinit var dcbQueries: DcbDomainEventQueries<AccountEvent>

    @BeforeEach
    fun setup() {
        subscriptionModel = InMemorySubscriptionModel()
        eventStore = InMemoryEventStore(subscriptionModel)
        converter = JacksonCloudEventConverter.Builder<AccountEvent>(jacksonObjectMapper(), URI.create("urn:occurrent:example:projection-dsl"))
            .typeMapper(ReflectionCloudEventTypeMapper.simple(AccountEvent::class.java))
            .idMapper { java.util.UUID.randomUUID().toString() }
            .build()
        dcbSubscriptions = DcbSubscriptions(subscriptionModel, converter)
        dcbQueries = DcbDomainEventQueries(DomainEventQueries(eventStore, converter))
    }

    @AfterEach
    fun shutdown() {
        subscriptionModel.shutdown()
    }

    @Test
    fun `push - the projection materializes the tag-scoped claimed flag`() {
        val store = ConcurrentHashMap<String, Boolean>()
        val repository = viewStateRepository<Boolean, String>({ store[it] }, { id, state -> store[id] = state })

        dcbSubscriptions.project("is-username-claimed", isUsernameClaimedProjection("johan"), repository)

        append("username:johan", AccountRegistered("johan"))

        // A single-instance projection keys its one slot by the subscription id, not the tagged username.
        subscriptionModel.waitUntilAllEventsProcessed()
        assertThat(store["is-username-claimed"]).isTrue()
    }

    @Test
    fun `pull - folding the tag-scoped events on demand reports the claim`() {
        append("username:johan", AccountRegistered("johan"))

        assertThat(dcbQueries.project(isUsernameClaimedProjection("johan"))).isTrue()
    }

    @Test
    fun `pull - a closed account releases the username`() {
        append("username:johan", AccountRegistered("johan"))
        append("username:johan", AccountClosed("johan"))

        assertThat(dcbQueries.project(isUsernameClaimedProjection("johan"))).isFalse()
    }

    @Test
    fun `pull - an unclaimed username is the initial state`() {
        append("username:johan", AccountRegistered("johan"))

        // "eve" was never mentioned, so its tag-scoped read matches nothing and the fold stays at the initial state.
        assertThat(dcbQueries.project(isUsernameClaimedProjection("eve"))).isFalse()
    }

    private fun append(tag: String, vararg events: AccountEvent) {
        val tags = listOf(Tag.parse(tag))
        val cloudEvents = converter.toCloudEvents(events.toList()).map { DcbCloudEvents.withTags(it, tags) }
        eventStore.append(cloudEvents)
    }
}
