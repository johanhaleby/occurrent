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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves that {@code @EnableOccurrentTesting(clearState = true)} reaches the context's {@link MongoTemplate} at all,
 * without a container: the point under test is the wiring path from the annotation attribute through
 * {@link OccurrentTestingImportSelector} and {@link OccurrentMongoFlushTestingConfiguration} to
 * {@code clearingStateWith(..)}, not what {@code OccurrentMongoFlush} does with a real database, which
 * {@code OccurrentMongoFlushTest} in {@code occurrent-testing-mongodb} already covers.
 * <p>
 * A {@link MongoTemplate} over an address nothing listens on lets {@code beforeEach} run far enough to attempt the
 * flush and fail fast on server selection, which only happens if the wiring actually handed the extension a clearer
 * backed by this template. Without {@link #clearState()}, {@code beforeEach} would return normally instead, since
 * there would be nothing to flush.
 *
 * @see EnableOccurrentTestingClearStateTest
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class EnableOccurrentTestingClearStateWiringTest {

    @Test
    void clear_state_true_wires_the_extension_to_attempt_a_flush_against_the_contexts_mongo_template() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppWithUnreachableMongoTemplate.class)) {
            OccurrentSubscriptionsExtension subscriptions = context.getBean(OccurrentSubscriptionsExtension.class);

            assertThatThrownBy(() -> subscriptions.beforeEach(unusedExtensionContext()))
                    .as("beforeEach must reach the wired clearer and attempt a flush against the context's MongoTemplate")
                    .isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(MongoTimeoutException.class);
        }
    }

    // beforeEach does not read the ExtensionContext today, but a null argument would silently hide it if it started
    // to. A proxy that throws on any access fails the test loudly instead.
    private static ExtensionContext unusedExtensionContext() {
        return (ExtensionContext) Proxy.newProxyInstance(
                EnableOccurrentTestingClearStateWiringTest.class.getClassLoader(),
                new Class<?>[]{ExtensionContext.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException("Did not expect ExtensionContext#" + method.getName() + " to be called in this test");
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentTesting(clearState = true)
    static class AppWithUnreachableMongoTemplate {

        @Bean
        InMemorySubscriptionModel subscriptionModel() {
            return new InMemorySubscriptionModel();
        }

        @Bean
        InMemoryEventStore eventStore(InMemorySubscriptionModel subscriptionModel) {
            return new InMemoryEventStore(subscriptionModel);
        }

        // Nothing listens on this port, and server selection is bounded well below JUnit's own timeout, so a flush
        // attempt against it fails fast with MongoTimeoutException instead of hanging.
        @Bean(destroyMethod = "close")
        MongoClient mongoClient() {
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> builder
                            .hosts(List.of(new ServerAddress("127.0.0.1", 1)))
                            .serverSelectionTimeout(1, java.util.concurrent.TimeUnit.SECONDS))
                    .applyToSocketSettings(builder -> builder.connectTimeout((int) Duration.ofSeconds(1).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS))
                    .build();
            return MongoClients.create(settings);
        }

        @Bean
        MongoTemplate mongoTemplate(MongoClient mongoClient) {
            return new MongoTemplate(mongoClient, "unreachable");
        }
    }
}
