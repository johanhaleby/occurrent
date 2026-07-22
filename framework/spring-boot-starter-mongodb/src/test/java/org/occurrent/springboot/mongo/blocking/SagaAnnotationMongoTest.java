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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Saga;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.bson.Document;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.command.CommandDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that a {@link Saga @Saga} factory bean is registered by the framework and driven end-to-end against real
 * MongoDB: an event triggers a command through a {@code CommandDispatcher}, and a timeout fires via the poller. The
 * command dispatcher here is a plain recording lambda, showing the decider-free path is first-class. Docker-based.
 */
@DisplayName("Saga annotation (blocking, Mongo state store, polled timers)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = SagaAnnotationMongoTest.SagaApplication.class,
        properties = {
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:saga-annotation-test",
                "occurrent.saga.timer-poll-interval=150ms"
        }
)
@Import(SagaAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(90)
class SagaAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:saga-annotation-test");

    @Autowired
    private ApplicationService<OrderEvent> applicationService;
    @Autowired
    private RecordingDispatcher recordingDispatcher;
    @Autowired
    private MongoOperations mongoOperations;

    @Test
    void reacts_to_an_event_by_issuing_a_command() {
        applicationService.execute("order-1", events -> List.of(new OrderPlaced("order-1")));
        applicationService.execute("order-1", events -> List.of(new PaymentReserved("order-1")));

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(recordingDispatcher.issued).contains(new ShipOrder("order-1")));
        assertThat(recordingDispatcher.issued).doesNotContain(new CancelOrder("order-1"));
    }

    @Test
    void fires_a_timeout_through_the_poller_when_the_awaited_event_never_arrives() {
        applicationService.execute("order-2", events -> List.of(new OrderPlaced("order-2")));

        // The payment timeout (2s) fires because no PaymentReserved arrives; the poller delivers it.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(recordingDispatcher.issued).contains(new CancelOrder("order-2")));
        assertThat(recordingDispatcher.issued).doesNotContain(new ShipOrder("order-2"));
    }

    @Test
    void gates_the_timer_poller_with_a_lease_keyed_apart_from_the_event_subscription() {
        // The saga registers its timer lease on startup. It must coexist with the event subscription's own lease as a
        // separate document, so both the raw id (event subscription) and the saga-timer: key (poller) are present.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(hasLeaseDocument("saga-timer:order-fulfillment")).isTrue();
            assertThat(hasLeaseDocument("order-fulfillment")).isTrue();
        });
    }

    private boolean hasLeaseDocument(String id) {
        return mongoOperations.getCollection("competing-consumer-locks").countDocuments(new Document("_id", id)) > 0;
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class SagaApplication {
        @Bean
        CloudEventTypeMapper<OrderEvent> orderEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<OrderEvent> orderEventCloudEventConverter(CloudEventTypeMapper<OrderEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(OrderEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        // A plain recording dispatcher: the decider-free command-dispatch path is first-class.
        @Bean
        RecordingDispatcher recordingDispatcher() {
            return new RecordingDispatcher();
        }

        @Bean
        OrderFulfillmentSaga orderFulfillmentSaga() {
            return new OrderFulfillmentSaga();
        }
    }

    static class RecordingDispatcher implements CommandDispatcher<OrderCommand> {
        final List<OrderCommand> issued = new CopyOnWriteArrayList<>();

        @Override
        public void dispatch(OrderCommand command) {
            issued.add(command);
        }
    }

    static class OrderFulfillmentSaga {
        @Saga(id = "order-fulfillment")
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderSagaState, OrderCommand> orderFulfillment() {
            return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderSagaState, OrderCommand>builder(new OrderSagaState("NEW"))
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, event) -> new OrderSagaState("AWAITING_PAYMENT"))
                    .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.startTimeout("payment", Duration.ofSeconds(2))))
                    .evolve(PaymentReserved.class, (state, event) -> new OrderSagaState("COMPLETED"))
                    .react(PaymentReserved.class, (state, event) -> List.of(SagaEffect.issue(new ShipOrder(event.orderId()))))
                    .evolveOnTimeout("payment", (state, timeout) -> new OrderSagaState("CANCELLED"))
                    .reactOnTimeout("payment", (state, timeout) -> List.of(SagaEffect.issue(new CancelOrder(timeout.sagaId()))))
                    .isTerminal(state -> state.phase().equals("COMPLETED") || state.phase().equals("CANCELLED"))
                    .build();
        }
    }

    record OrderSagaState(String phase) {
    }

    sealed interface OrderEvent {
        String orderId();

        String eventId();

        Date timestamp();
    }

    record OrderPlaced(String orderId, String eventId, Date timestamp) implements OrderEvent {
        OrderPlaced(String orderId) {
            this(orderId, UUID.randomUUID().toString(), new Date());
        }
    }

    record PaymentReserved(String orderId, String eventId, Date timestamp) implements OrderEvent {
        PaymentReserved(String orderId) {
            this(orderId, UUID.randomUUID().toString(), new Date());
        }
    }

    sealed interface OrderCommand {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record CancelOrder(String orderId) implements OrderCommand {
    }
}
