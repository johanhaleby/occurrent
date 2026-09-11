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

package org.occurrent.dsl.saga.mongodb.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. {@code SagaExecution.onCloudEvent} catches whatever a saga's transition throws and writes a
 * first-failure record that starts the instance's quarantine budget. A MongoDB flap inside a read the store retries
 * away must never reach that catch and must never start the clock on an instance that was never actually broken. The
 * contrasting test, with retries turned off, shows what a failure record from a genuine store outage looks like,
 * which is what makes the first test's absence of one mean something.
 * <p>
 * The saga used here never throws on its own. The only failure in either test is the one injected into MongoDB.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreQuarantineBudgetTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-quarantine-budget";
    private static final String STREAM = "orders";

    sealed interface OrderEvent permits OrderPlaced {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ShipOrder(String orderId) {
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private final List<ShipOrder> dispatched = new CopyOnWriteArrayList<>();
    private @Nullable SagaSubscription subscription;

    @BeforeEach
    void createInstances() {
        // RetryStrategy.none(), because InMemorySubscriptionModel's own default redelivers a failing event on a fixed
        // 200 ms delay, and a redelivery arriving once the single injected MongoDB failure has been used up succeeds
        // and overwrites the very failure record these tests read. Redelivery is not what either test is about.
        subscriptionModel = new InMemorySubscriptionModel(RetryStrategy.none());
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
    }

    private MongoOperations mongoOperations() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-qb-" + UUID.randomUUID()));
        return new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
    }

    private static Saga<OrderEvent, String, ShipOrder> shippingSaga() {
        return Saga.<OrderEvent, String, ShipOrder>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .build();
    }

    /**
     * {@link InMemorySubscriptionModel} does not implement {@link HistoryRetainingSubscriptions}, and
     * {@code SagaRunner} switches quarantine off entirely for a subscription model it cannot ask, regardless of
     * {@link SagaRunnerConfig#quarantineAfter()}. Wrapping it with an always-retains answer is what makes the
     * quarantine budget (and so the failure record these tests read) reachable at all.
     */
    private record RetainsEverything(InMemorySubscriptionModel delegate) implements Subscribable, HistoryRetainingSubscriptions {

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    /** Wraps {@code real}, throwing a {@link DataAccessResourceFailureException} on the first {@code failures} calls to the method named {@code methodName} and delegating every other call and every other method unchanged. */
    private static MongoOperations failing(MongoOperations real, String methodName, int failures) {
        AtomicInteger remaining = new AtomicInteger(failures);
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().equals(methodName) && remaining.getAndDecrement() > 0) {
                throw new DataAccessResourceFailureException("simulated transient MongoDB failure in " + methodName);
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (MongoOperations) Proxy.newProxyInstance(MongoOperations.class.getClassLoader(),
                new Class<?>[]{MongoOperations.class}, handler);
    }

    @Test
    void a_transient_store_failure_does_not_start_the_instance_s_quarantine_budget() {
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<String> stateStore = new SpringMongoSagaStateStore<>(failing(mongoOperations, "findById", 1), COLLECTION, String.class);
        subscription = SagaRunner.<OrderEvent, ShipOrder>agnostic(new RetainsEverything(subscriptionModel), converter)
                .run(STREAM, shippingSaga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5)));

        eventStore.write("order-1", converter.toCloudEvents(List.of(new OrderPlaced("order-1"))));

        SagaStateStore<String> readStore = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
        // dontCatchUncaughtExceptions(), so a store failure that is not retried away fails this test on the record it
        // wrote rather than on the exception it threw on the subscription's delivery thread. The record is the defect.
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Optional<SagaEnvelope<String>> found = readStore.findWithoutState("order-1");
            assertThat(found).isPresent();
            assertAll(
                    () -> assertThat(found.get().failure())
                            .as("a transient MongoDB failure must leave no failure record, because the record is what starts the instance's quarantine budget and spends it on an outage the instance was never responsible for")
                            .isNull(),
                    () -> assertThat(found.get().status())
                            .as("the instance must still be running, not stopped by a store outage")
                            .isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(dispatched)
                            .as("the saga must have handled the event, since the store failure was retried away before it ever reached the saga")
                            .containsExactly(new ShipOrder("order-1"))
            );
        });
    }

    @Test
    void a_genuine_store_failure_does_start_the_instance_s_quarantine_budget() {
        MongoOperations mongoOperations = mongoOperations();
        SagaStateStore<String> stateStore = new SpringMongoSagaStateStore<>(failing(mongoOperations, "findById", 1), COLLECTION, String.class, null, RetryStrategy.none());
        subscription = SagaRunner.<OrderEvent, ShipOrder>agnostic(new RetainsEverything(subscriptionModel), converter)
                .run(STREAM, shippingSaga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5)));

        eventStore.write("order-2", converter.toCloudEvents(List.of(new OrderPlaced("order-2"))));

        SagaStateStore<String> readStore = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
        // dontCatchUncaughtExceptions(): the budget is not spent on one failure, so onCloudEvent rethrows after
        // writing the record, uncaught on the subscription's delivery thread. That is the documented behaviour, not a
        // bug this poll should surface.
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Optional<SagaEnvelope<String>> found = readStore.findWithoutState("order-2");
            assertThat(found).isPresent();
            assertThat(found.get().failure())
                    .as("with retries off, the same single MongoDB failure must reach the runner and start the quarantine budget, which is what the other test proves the default strategy prevents")
                    .isNotNull();
        });

        SagaFailure failure = readStore.findWithoutState("order-2").orElseThrow().failure();
        assertThat(failure.firstFailedAt()).isNotNull();
    }
}
