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

package org.occurrent.springboot.mongo.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. The store of a {@code @Saga} that declares none runs its transactions on the {@code MongoDatabaseFactory}
 * of a {@link MongoTemplate}, so it is built on the template bean, as the event store of this configuration is, and an
 * application's {@code @Primary} {@link MongoOperations} that wraps the template and is not one is left out of it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class MongoSagaStateStoreProviderTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    record Payment(long amount) {
    }

    private static final AtomicInteger CALLS_TO_THE_DECORATOR = new AtomicInteger();

    @Configuration(proxyBeanMethods = false)
    static class TemplateNextToAPrimaryDecorator {

        @Bean
        MongoTemplate mongoTemplate() {
            ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-provider-" + UUID.randomUUID().toString().substring(0, 8)));
            return new MongoTemplate(MongoClients.create(connectionString), connectionString.getDatabase());
        }

        // A MongoOperations that is not a MongoTemplate, such as an application's tracing or tenant-routing wrapper
        @Bean
        @Primary
        MongoOperations decorator(MongoTemplate delegate) {
            return (MongoOperations) Proxy.newProxyInstance(MongoOperations.class.getClassLoader(), new Class<?>[]{MongoOperations.class}, (proxy, method, args) -> {
                CALLS_TO_THE_DECORATOR.incrementAndGet();
                try {
                    return method.invoke(delegate, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        }
    }

    @Test
    void builds_the_store_on_the_mongo_template_bean_when_a_primary_mongo_operations_decorator_is_next_to_it() {
        CALLS_TO_THE_DECORATOR.set(0);
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TemplateNextToAPrimaryDecorator.class)) {
            MongoSagaStateStoreProvider provider = new MongoSagaStateStoreProvider(context);

            SagaStateStore<Payment> store = provider.createDefaultSagaStateStore("payment", Payment.class);
            Optional<StoredSaga> found = store.find("absent");

            assertAll(
                    () -> assertThat(store).isInstanceOf(SpringMongoSagaStateStore.class),
                    () -> assertThat(found).isEmpty(),
                    () -> assertThat(CALLS_TO_THE_DECORATOR).as("calls the store made through the decorator").hasValue(0)
            );
        }
    }
}
