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

package org.occurrent.testing.junit.blocking

import io.cloudevents.CloudEvent
import io.cloudevents.core.v1.CloudEventBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.lang.reflect.Proxy
import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

@DisplayNameGeneration(ReplaceUnderscores::class)
class OccurrentSubscriptionsExtensionsTest {

    private lateinit var subscriptionModel: InMemorySubscriptionModel
    private lateinit var eventStore: InMemoryEventStore

    @BeforeEach
    fun createStoreAndSubscriptionModel() {
        subscriptionModel = InMemorySubscriptionModel()
        eventStore = InMemoryEventStore(subscriptionModel)
    }

    @AfterEach
    fun shutdown() = subscriptionModel.shutdown()

    @Test
    fun `stoppedByDefault on the subscription model builds an extension that stops everything`() {
        val received = CopyOnWriteArrayList<CloudEvent>()
        subscriptionModel.subscribe("orders") { received.add(it) }

        val subscriptions = subscriptionModel.stoppedByDefault()
        subscriptions.beforeEach(unusedExtensionContext())

        eventStore.write("stream1", listOf(event()))
        subscriptionModel.waitUntilAllEventsProcessed()

        assertThat(received).isEmpty()
    }

    @Test
    fun `an extension built from the receiver still starts the subscription a test names`() {
        val received = CopyOnWriteArrayList<CloudEvent>()
        subscriptionModel.subscribe("orders") { received.add(it) }

        val subscriptions = subscriptionModel.stoppedByDefault()
        subscriptions.beforeEach(unusedExtensionContext())
        subscriptions.start("orders")

        eventStore.write("stream1", listOf(event()))
        subscriptionModel.waitUntilAllEventsProcessed()

        assertThat(received).hasSize(1)
    }

    @Test
    fun `alwaysStart chains off the receiver form`() {
        val received = CopyOnWriteArrayList<CloudEvent>()
        subscriptionModel.subscribe("orders") { received.add(it) }

        subscriptionModel.stoppedByDefault().alwaysStart("orders").beforeEach(unusedExtensionContext())

        eventStore.write("stream1", listOf(event()))
        subscriptionModel.waitUntilAllEventsProcessed()

        assertThat(received).hasSize(1)
    }

    // beforeEach does not read the ExtensionContext today, but a null argument would silently hide it if it started
    // to. A proxy that throws on any access fails the test loudly instead.
    private fun unusedExtensionContext(): ExtensionContext =
        Proxy.newProxyInstance(
            OccurrentSubscriptionsExtensionsTest::class.java.classLoader,
            arrayOf(ExtensionContext::class.java)
        ) { _, method, _ ->
            throw UnsupportedOperationException("Did not expect ExtensionContext#${method.name} to be called in this test")
        } as ExtensionContext

    private fun event(): CloudEvent = CloudEventBuilder()
        .withId(UUID.randomUUID().toString())
        .withSubject("subject")
        .withType("type1")
        .withSource(URI.create("urn:source"))
        .withTime(OffsetDateTime.now())
        .build()
}
