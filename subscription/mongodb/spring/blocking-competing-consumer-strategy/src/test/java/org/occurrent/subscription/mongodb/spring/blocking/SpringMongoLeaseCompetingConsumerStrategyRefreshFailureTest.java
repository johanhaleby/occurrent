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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.mongodb.blocking.ccs.LeaseRefreshFailure;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The strategy hands out what its lease refresh reports, which is tested where the refresh runs. This asserts that the
 * public method of the Spring strategy answers with it, through a refresh that fails on the schedule the strategy runs.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("a Spring MongoDB lease strategy reporting its failing lease refresh")
@Timeout(60)
class SpringMongoLeaseCompetingConsumerStrategyRefreshFailureTest {

    private static final String DATABASE = "springleaserefreshfailure";
    private static final String SUBSCRIPTION = "a-subscription";
    private static final String NODE = "the-node";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoOperations mongoOperations;

    private final AtomicBoolean mongoDbRefuses = new AtomicBoolean();
    private SpringMongoLeaseCompetingConsumerStrategy strategy;

    @BeforeAll
    static void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        mongoOperations = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void aStrategyHoldingALeaseWhoseRefreshIsScheduled() {
        strategy = new SpringMongoLeaseCompetingConsumerStrategy.Builder(refusingWhenAsked(mongoOperations))
                .collectionName("competing-consumer-locks-" + UUID.randomUUID())
                .leaseTime(Duration.ofSeconds(1))
                .retryStrategy(RetryStrategy.none())
                .build();
        assertThat(strategy.registerCompetingConsumer(SUBSCRIPTION, NODE)).isTrue();
    }

    @AfterEach
    void shutdown() {
        strategy.shutdown();
    }

    @Test
    void reports_no_failure_while_the_lease_refresh_works() {
        assertThat(strategy.leaseRefreshFailure()).isEmpty();
    }

    @Test
    void reports_the_failure_of_the_lease_refresh_while_it_fails_and_no_failure_once_it_works_again() {
        mongoDbRefuses.set(true);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(strategy.leaseRefreshFailure()).as("lease refresh failure").isPresent());
        LeaseRefreshFailure failure = strategy.leaseRefreshFailure().orElseThrow();
        assertThat(failure.cause()).hasMessage("MongoDB refuses the lease refresh");

        mongoDbRefuses.set(false);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(strategy.leaseRefreshFailure()).as("lease refresh failure").isEmpty());
    }

    // Refuses the calls that run against the lock collection, which the scheduled refresh makes through execute
    private MongoOperations refusingWhenAsked(MongoOperations delegate) {
        return (MongoOperations) Proxy.newProxyInstance(MongoOperations.class.getClassLoader(), new Class<?>[]{MongoOperations.class}, (proxy, method, args) -> {
            if (mongoDbRefuses.get() && method.getName().equals("execute")) {
                throw new IllegalStateException("MongoDB refuses the lease refresh");
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }
}
