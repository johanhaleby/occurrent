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
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that {@link EnableOccurrentTesting} works when the reactor testing artifact is on the classpath instead of
 * the blocking one, and when both are, which is the case {@link OccurrentTestingImportSelector} exists for. A plain
 * {@link AnnotationConfigApplicationContext} is enough here, no Spring Boot test slice and no container.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class EnableOccurrentTestingReactorAndMixedTest {

    @Test
    void a_reactive_only_context_starts_cleanly_and_the_reactor_extension_stops_and_starts_the_model() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ReactiveOnlyApp.class)) {
            SynchronousSubscriptionModel model = context.getBean(SynchronousSubscriptionModel.class);
            org.occurrent.testing.junit.reactor.OccurrentSubscriptionsExtension subscriptions =
                    context.getBean(org.occurrent.testing.junit.reactor.OccurrentSubscriptionsExtension.class);

            CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
            model.subscribe("order-projection", event -> {
                received.add(event);
                return Mono.empty();
            });

            subscriptions.beforeEach(unusedExtensionContext());
            subscriptions.start("order-projection");

            model.dispatch(List.of(event())).block();

            assertThat(received).hasSize(1);
        }
    }

    @Test
    void a_mixed_context_gets_both_extensions_and_each_stops_only_its_own_stack() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(MixedApp.class)) {
            InMemorySubscriptionModel blockingModel = context.getBean(InMemorySubscriptionModel.class);
            SynchronousSubscriptionModel reactorModel = context.getBean(SynchronousSubscriptionModel.class);
            org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension blockingSubscriptions =
                    context.getBean(org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension.class);
            org.occurrent.testing.junit.reactor.OccurrentSubscriptionsExtension reactorSubscriptions =
                    context.getBean(org.occurrent.testing.junit.reactor.OccurrentSubscriptionsExtension.class);

            CopyOnWriteArrayList<CloudEvent> reactorReceived = new CopyOnWriteArrayList<>();
            reactorModel.subscribe("order-projection", event -> {
                reactorReceived.add(event);
                return Mono.empty();
            });
            blockingModel.subscribe("orders", event -> {
            });

            blockingSubscriptions.beforeEach(unusedExtensionContext());
            reactorSubscriptions.beforeEach(unusedExtensionContext());

            assertThat(blockingModel.isPaused("orders")).isTrue();
            assertThat(reactorModel.isPaused("order-projection")).isTrue();

            reactorSubscriptions.start("order-projection");
            reactorModel.dispatch(List.of(event())).block();

            assertThat(reactorReceived)
                    .as("starting the reactor extension must not resume the blocking model it shares a context with")
                    .hasSize(1);
            assertThat(blockingModel.isPaused("orders")).isTrue();

            blockingModel.shutdown();
        }
    }

    // beforeEach does not read the ExtensionContext today, but a null argument would silently hide it if it started
    // to. A proxy that throws on any access fails the test loudly instead.
    private static ExtensionContext unusedExtensionContext() {
        return (ExtensionContext) Proxy.newProxyInstance(
                EnableOccurrentTestingReactorAndMixedTest.class.getClassLoader(),
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
    static class ReactiveOnlyApp {

        @Bean
        SynchronousSubscriptionModel subscriptionModel() {
            return new SynchronousSubscriptionModel();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentTesting
    static class MixedApp {

        @Bean
        InMemorySubscriptionModel blockingSubscriptionModel() {
            return new InMemorySubscriptionModel();
        }

        @Bean
        InMemoryEventStore eventStore(InMemorySubscriptionModel blockingSubscriptionModel) {
            return new InMemoryEventStore(blockingSubscriptionModel);
        }

        @Bean
        SynchronousSubscriptionModel reactorSubscriptionModel() {
            return new SynchronousSubscriptionModel();
        }
    }
}
