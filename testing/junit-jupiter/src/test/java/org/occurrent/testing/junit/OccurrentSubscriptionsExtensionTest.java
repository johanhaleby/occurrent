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

package org.occurrent.testing.junit;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OccurrentSubscriptionsExtensionTest {

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;

    @BeforeEach
    void createStoreAndSubscriptionModel() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
    }

    @Test
    void subscription_registered_before_the_test_does_not_receive_an_event_written_during_the_test() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("orders", received::add);

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel);
        runBeforeEach(extension);

        eventStore.write("stream1", List.of(event()));
        subscriptionModel.waitUntilAllEventsProcessed();

        assertThat(received).isEmpty();
    }

    @Test
    void after_start_the_subscription_receives_events_written_after() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("orders", received::add);

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel);
        runBeforeEach(extension);

        extension.start("orders");

        eventStore.write("stream1", List.of(event()));
        subscriptionModel.waitUntilAllEventsProcessed();

        assertThat(received).hasSize(1);
    }

    @Test
    void start_on_an_unknown_id_fails_with_a_message_naming_the_known_ids() {
        subscriptionModel.subscribe("orders", event -> {
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension
                .stoppedByDefault(subscriptionModel)
                .alwaysStart("orders");
        runBeforeEach(extension);

        assertThatThrownBy(() -> extension.start("shipments"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shipments")
                .hasMessageContaining("orders");
    }

    @Test
    void always_start_resumes_automatically_in_before_each() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("orders", received::add);

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension
                .stoppedByDefault(subscriptionModel)
                .alwaysStart("orders");
        runBeforeEach(extension);

        eventStore.write("stream1", List.of(event()));
        subscriptionModel.waitUntilAllEventsProcessed();

        assertThat(received).hasSize(1);
    }

    @Test
    void a_test_that_needs_several_subscriptions_names_each_of_them() {
        CopyOnWriteArrayList<CloudEvent> ordersReceived = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> shipmentsReceived = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("orders", ordersReceived::add);
        subscriptionModel.subscribe("shipments", shipmentsReceived::add);

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel);
        runBeforeEach(extension);

        extension.start("orders");
        extension.start("shipments");

        eventStore.write("stream1", List.of(event()));
        subscriptionModel.waitUntilAllEventsProcessed();

        assertThat(ordersReceived).hasSize(1);
        assertThat(shipmentsReceived).hasSize(1);
    }

    @Test
    void a_subscription_that_was_never_named_stays_stopped_while_a_named_one_runs() {
        CopyOnWriteArrayList<CloudEvent> ordersReceived = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> shipmentsReceived = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("orders", ordersReceived::add);
        subscriptionModel.subscribe("shipments", shipmentsReceived::add);

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel);
        runBeforeEach(extension);

        extension.start("orders");

        eventStore.write("stream1", List.of(event()));
        subscriptionModel.waitUntilAllEventsProcessed();

        assertThat(ordersReceived).hasSize(1);
        assertThat(shipmentsReceived).isEmpty();
    }

    @Test
    void after_each_stops_every_subscription_so_it_does_not_leak_into_the_next_test() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("orders", received::add);

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel);
        runBeforeEach(extension);
        extension.start("orders");

        extension.afterEach(unusedExtensionContext());

        eventStore.write("stream1", List.of(event()));
        subscriptionModel.waitUntilAllEventsProcessed();

        assertThat(received).isEmpty();
    }

    private static void runBeforeEach(OccurrentSubscriptionsExtension extension) {
        extension.beforeEach(unusedExtensionContext());
    }

    // beforeEach/afterEach do not read the ExtensionContext today, but a null argument would silently hide it if
    // they started to. A proxy that throws on any access fails the test loudly instead.
    private static ExtensionContext unusedExtensionContext() {
        return (ExtensionContext) Proxy.newProxyInstance(
                OccurrentSubscriptionsExtensionTest.class.getClassLoader(),
                new Class<?>[]{ExtensionContext.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException("Did not expect ExtensionContext#" + method.getName() + " to be called in this test");
                });
    }

    private static CloudEvent event() {
        return new CloudEventBuilder()
                .withId(UUID.randomUUID().toString())
                .withSubject("subject")
                .withType("type1")
                .withSource(URI.create("urn:source"))
                .withTime(OffsetDateTime.now())
                .withData("test".getBytes(UTF_8))
                .build();
    }
}
