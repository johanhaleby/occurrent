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
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.springboot.common.internal.StartupGap;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
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
 * Proves that a saga timer that is already due when the application starts fires after the startup scan, not while it
 * still runs. The timer poller starts together with the saga's other asynchronous starts, and what a timer dispatches
 * can write through the application service, which is refused until the scan has registered every synchronous handler.
 * <p>
 * The first boot stores the timer and stops before it is due. The second boot starts after it is, and a bean that is
 * slow to build makes the startup scan run long enough after the saga registers that a poller started with the saga
 * would fire inside it.
 */
@DisplayName("Saga timers at startup")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class SagaTimerStartsAfterDiscoveryMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:saga-timer-after-discovery");
    private static final Duration TIMEOUT = ofSeconds(2);

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_saga_timer_that_is_due_when_the_application_starts_fires_after_the_startup_scan_not_during_it() {
        String database = "timer-due-at-startup";
        storeATimerThatFallsDueWhileTheApplicationIsStopped(database);

        try (ConfigurableApplicationContext context = boot(SlowScanApplication.class, database)) {
            RecordingDispatcher dispatcher = context.getBean(RecordingDispatcher.class);

            await().atMost(ofSeconds(30)).pollInterval(ofMillis(50)).untilAsserted(() ->
                    assertThat(dispatcher.dispatched).extracting(Dispatched::command).as("[commands the timer dispatched]").contains(new CancelOrder("order-1")));
            assertThat(dispatcher.dispatched).extracting(Dispatched::admittedWrites)
                    .as("[whether the application service admitted writes when the timer dispatched]").containsOnly(true);
        }
    }

    private static void storeATimerThatFallsDueWhileTheApplicationIsStopped(String database) {
        Instant due;
        try (ConfigurableApplicationContext context = boot(TimerApplication.class, database)) {
            due = Instant.now().plus(TIMEOUT).plusMillis(500);
            applicationServiceOf(context).execute("order-1", __ -> List.of(new OrderPlaced("order-1")));
            MongoOperations mongo = context.getBean(MongoOperations.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(50)).until(() -> mongo.collectionExists("saga-orderSaga"));
        }
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(50)).until(() -> Instant.now().isAfter(due));
    }

    @SuppressWarnings("unchecked")
    private static ApplicationService<OrderEvent> applicationServiceOf(ConfigurableApplicationContext context) {
        return context.getBean(ApplicationService.class);
    }

    private static ConfigurableApplicationContext boot(Class<?> application, String database) {
        return SpringApplication.run(application,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.saga.timer-poll-interval=50ms",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    record Dispatched(OrderCommand command, boolean admittedWrites) {
    }

    static class RecordingDispatcher implements CommandDispatcher<OrderCommand> {
        final List<Dispatched> dispatched = new CopyOnWriteArrayList<>();
        private final ObjectProvider<StartupGap> startupGaps;

        RecordingDispatcher(ObjectProvider<StartupGap> startupGaps) {
            this.startupGaps = startupGaps;
        }

        @Override
        public void dispatch(OrderCommand command) {
            dispatched.add(new Dispatched(command, startupGaps.orderedStream().allMatch(StartupGap::admitsWrite)));
        }
    }

    static class OrderSaga {
        @Saga(id = "orderSaga", startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderSagaState, OrderCommand> orderSaga() {
            return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderSagaState, OrderCommand>builder(new OrderSagaState("NEW"))
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, event) -> new OrderSagaState("AWAITING_PAYMENT"))
                    .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.startTimeout("payment", TIMEOUT)))
                    .evolveOnTimeout("payment", (state, timeout) -> new OrderSagaState("CANCELLED"))
                    .reactOnTimeout("payment", (state, timeout) -> List.of(SagaEffect.issue(new CancelOrder(timeout.sagaId()))))
                    .isTerminal(state -> state.phase().equals("CANCELLED"))
                    .build();
        }
    }

    // Built by the startup scan once the saga has registered, and slow, so the scan is still running when a timer poller
    // that started with the saga polls
    static class SlowToBuild {
        SlowToBuild() throws InterruptedException {
            Thread.sleep(3_000);
        }

        @Subscription(id = "slowToBuild", startAt = StartPosition.NOW)
        void on(OrderPlaced event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EventConverters {

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

        @Bean
        RecordingDispatcher recordingDispatcher(ObjectProvider<StartupGap> startupGaps) {
            return new RecordingDispatcher(startupGaps);
        }

        @Bean
        OrderSaga orderSaga() {
            return new OrderSaga();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class TimerApplication {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class SlowScanApplication {

        @Bean
        @Lazy
        SlowToBuild slowToBuild() throws InterruptedException {
            return new SlowToBuild();
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

    sealed interface OrderCommand {
    }

    record CancelOrder(String orderId) implements OrderCommand {
    }
}
