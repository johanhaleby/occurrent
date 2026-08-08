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

package org.occurrent.testing.springboot;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that {@link EnableOccurrentTesting} hands back a working extension over the context's own subscription model,
 * and that the extension it hands back behaves the same as one constructed by hand. A plain
 * {@link AnnotationConfigApplicationContext} is enough here, no Spring Boot test slice and no container.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class EnableOccurrentTestingTest {

    @Test
    void the_extension_bean_stops_the_contexts_subscriptions_and_starts_the_one_a_test_names() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestApp.class)) {
            InMemorySubscriptionModel subscriptionModel = context.getBean(InMemorySubscriptionModel.class);
            InMemoryEventStore eventStore = context.getBean(InMemoryEventStore.class);
            OccurrentSubscriptionsExtension subscriptions = context.getBean(OccurrentSubscriptionsExtension.class);

            CopyOnWriteArrayList<CloudEvent> orders = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> shipments = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe("orders", orders::add);
            subscriptionModel.subscribe("shipments", shipments::add);

            subscriptions.beforeEach(unusedExtensionContext());
            subscriptions.start("orders");

            eventStore.write("stream1", List.of(event()));
            subscriptionModel.waitUntilAllEventsProcessed();

            assertThat(orders).hasSize(1);
            assertThat(shipments).isEmpty();
        }
    }

    @Test
    void each_test_class_gets_its_own_extension_so_named_ids_do_not_leak_between_them() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestApp.class)) {
            OccurrentSubscriptionsExtension first = context.getBean(OccurrentSubscriptionsExtension.class);
            OccurrentSubscriptionsExtension second = context.getBean(OccurrentSubscriptionsExtension.class);

            assertThat(first).isNotSameAs(second);
        }
    }

    @Test
    void the_extension_bean_is_wired_to_the_contexts_subscription_model() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestApp.class)) {
            InMemorySubscriptionModel subscriptionModel = context.getBean(InMemorySubscriptionModel.class);
            subscriptionModel.subscribe("orders", event -> {
            });

            context.getBean(OccurrentSubscriptionsExtension.class).beforeEach(unusedExtensionContext());

            assertThat(subscriptionModel.isPaused("orders")).isTrue();
        }
    }

    // Reproduces #636: the context holds exactly one CheckpointStorage bean, which is the case a hand-wired
    // clearingCheckpoints(..) call exists to cover, yet the bean @EnableOccurrentTesting hands back today never
    // learns about it. A checkpoint a prior test left behind survives into this one without the workaround the
    // issue's docs show, an @Autowired method that mutates the prototype-scoped extension bean itself.
    @Test
    void the_extension_bean_clears_the_contexts_lone_checkpoint_storage_before_each_test_with_no_hand_wiring() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppWithCheckpointStorage.class)) {
            InMemorySubscriptionModel subscriptionModel = context.getBean(InMemorySubscriptionModel.class);
            subscriptionModel.subscribe("orders", event -> {
            });

            CheckpointStorage checkpointStorage = context.getBean(CheckpointStorage.class);
            checkpointStorage.save("orders", new StringBasedCheckpoint("left-behind-by-a-previous-test"));

            OccurrentSubscriptionsExtension subscriptions = context.getBean(OccurrentSubscriptionsExtension.class);
            subscriptions.beforeEach(unusedExtensionContext());

            assertThat(checkpointStorage.exists("orders"))
                    .as("checkpoint left behind by an earlier test must be cleared with no hand wiring beyond @EnableOccurrentTesting")
                    .isFalse();
        }
    }

    // beforeEach does not read the ExtensionContext today, but a null argument would silently hide it if it started
    // to. A proxy that throws on any access fails the test loudly instead.
    private static ExtensionContext unusedExtensionContext() {
        return (ExtensionContext) Proxy.newProxyInstance(
                EnableOccurrentTestingTest.class.getClassLoader(),
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
                .build();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentTesting
    static class TestApp {

        // InMemorySubscriptionModel already implements SubscriptionModelLifeCycle, so this one bean is what
        // OccurrentTestingConfiguration injects.
        @Bean
        InMemorySubscriptionModel subscriptionModel() {
            return new InMemorySubscriptionModel();
        }

        @Bean
        InMemoryEventStore eventStore(InMemorySubscriptionModel subscriptionModel) {
            return new InMemoryEventStore(subscriptionModel);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentTesting
    static class AppWithCheckpointStorage {

        @Bean
        InMemorySubscriptionModel subscriptionModel() {
            return new InMemorySubscriptionModel();
        }

        @Bean
        InMemoryEventStore eventStore(InMemorySubscriptionModel subscriptionModel) {
            return new InMemoryEventStore(subscriptionModel);
        }

        @Bean
        CheckpointStorage checkpointStorage() {
            return new InMemoryCheckpointStorage();
        }
    }
}
