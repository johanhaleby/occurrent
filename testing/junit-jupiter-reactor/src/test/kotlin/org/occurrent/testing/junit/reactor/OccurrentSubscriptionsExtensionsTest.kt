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

package org.occurrent.testing.junit.reactor

import io.cloudevents.CloudEvent
import io.cloudevents.core.v1.CloudEventBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel
import reactor.core.publisher.Mono
import java.lang.reflect.Proxy
import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

@DisplayNameGeneration(ReplaceUnderscores::class)
class OccurrentSubscriptionsExtensionsTest {

    @Test
    fun `stoppedByDefault on the subscription model builds an extension that stops everything`() {
        val model = SynchronousSubscriptionModel()
        val received = CopyOnWriteArrayList<CloudEvent>()
        model.subscribe("orders") { received.add(it); Mono.empty() }

        val subscriptions = model.stoppedByDefault()
        subscriptions.beforeEach(unusedExtensionContext())

        model.dispatch(listOf(event())).block()

        assertThat(received).isEmpty()
        model.shutdown()
    }

    @Test
    fun `an extension built from the receiver still starts the subscription a test names`() {
        val model = SynchronousSubscriptionModel()
        val received = CopyOnWriteArrayList<CloudEvent>()
        model.subscribe("orders") { received.add(it); Mono.empty() }

        val subscriptions = model.stoppedByDefault()
        subscriptions.beforeEach(unusedExtensionContext())
        subscriptions.start("orders")

        model.dispatch(listOf(event())).block()

        assertThat(received).hasSize(1)
        model.shutdown()
    }

    @Test
    fun `alwaysStart chains off the receiver form`() {
        val model = SynchronousSubscriptionModel()
        val received = CopyOnWriteArrayList<CloudEvent>()
        model.subscribe("orders") { received.add(it); Mono.empty() }

        model.stoppedByDefault().alwaysStart("orders").beforeEach(unusedExtensionContext())

        model.dispatch(listOf(event())).block()

        assertThat(received).hasSize(1)
        model.shutdown()
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
