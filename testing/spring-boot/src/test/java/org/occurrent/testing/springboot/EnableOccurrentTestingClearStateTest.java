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

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that {@code @EnableOccurrentTesting(clearState = true)} flushes the context's {@link MongoTemplate}
 * database before each test with no hand-written {@code clearingStateWith(..)} call, the second half of #636 the
 * plain {@link EnableOccurrentTestingTest} coverage does not reach because it never touches a real store.
 * <p>
 * Not shared with other MongoDB test classes in this repository, per the same reasoning
 * {@code OccurrentMongoFlushTest} in {@code occurrent-testing-mongodb} gives: a flush test cannot tolerate a
 * container another class drops out from under it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class EnableOccurrentTestingClearStateTest {

    private static final String DATABASE = "occurrent-testing-spring-boot";
    private static final String ORDERS = "orders";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private AnnotationConfigApplicationContext context;
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void startContext() {
        context = new AnnotationConfigApplicationContext(AppWithMongoTemplate.class);
        mongoTemplate = context.getBean(MongoTemplate.class);
    }

    @AfterEach
    void stopContext() {
        context.close();
    }

    @Test
    void a_document_left_behind_by_a_previous_test_is_gone_before_the_next_one_with_no_hand_wiring() {
        MongoCollection<Document> orders = mongoTemplate.getDb().getCollection(ORDERS);
        orders.insertOne(new Document("_id", "left-behind-by-a-previous-test"));

        OccurrentSubscriptionsExtension subscriptions = context.getBean(OccurrentSubscriptionsExtension.class);
        subscriptions.beforeEach(unusedExtensionContext());

        assertThat(orders.countDocuments())
                .as("clearState = true must flush the store with no hand wiring beyond @EnableOccurrentTesting")
                .isZero();
    }

    // beforeEach does not read the ExtensionContext today, but a null argument would silently hide it if it started
    // to. A proxy that throws on any access fails the test loudly instead.
    private static ExtensionContext unusedExtensionContext() {
        return (ExtensionContext) Proxy.newProxyInstance(
                EnableOccurrentTestingClearStateTest.class.getClassLoader(),
                new Class<?>[]{ExtensionContext.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException("Did not expect ExtensionContext#" + method.getName() + " to be called in this test");
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentTesting(clearState = true)
    static class AppWithMongoTemplate {

        @Bean
        InMemorySubscriptionModel subscriptionModel() {
            return new InMemorySubscriptionModel();
        }

        @Bean
        InMemoryEventStore eventStore(InMemorySubscriptionModel subscriptionModel) {
            return new InMemoryEventStore(subscriptionModel);
        }

        @Bean
        MongoTemplate mongoTemplate() {
            return new MongoTemplate(MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE)), DATABASE);
        }
    }
}
