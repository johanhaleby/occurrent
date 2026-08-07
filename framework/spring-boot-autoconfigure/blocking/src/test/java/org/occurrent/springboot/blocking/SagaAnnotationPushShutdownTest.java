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

package org.occurrent.springboot.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * {@code @Saga(source = PUSH, startupMode = BACKGROUND)}: the catch-up replay stops when the context does.
 * <p>
 * The replay runs on a thread of its own, and the only thing that can stop it is the catch-up model in front of the
 * feed. The registrar creates that model, so it has to keep it: a saga whose replay outlives the context keeps folding
 * history, and issuing the commands that come out of it, into an event store that is closing underneath it. The
 * registrar used to drop the model on the floor, which left {@code close()} stopping the timer pollers and nothing
 * else.
 * <p>
 * Container-free, the same way {@link ProjectionPushStartupModeTest} is: a fake reader and the real push model.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationPushShutdownTest {

    // Held by the reader bean, since the Spring context is what builds it. The replay parks on the second event, so a
    // test can observe a replay that is genuinely under way and still has history left to fold.
    private static final CountDownLatch[] REPLAY_PARKED = {new CountDownLatch(1)};
    private static final CountDownLatch[] RELEASE_REPLAY = {new CountDownLatch(1)};
    private static final CopyOnWriteArrayList<OrderCommand> ISSUED = new CopyOnWriteArrayList<>();

    @Test
    void closing_the_context_stops_a_background_replay_before_it_folds_the_rest_of_the_history() throws Exception {
        REPLAY_PARKED[0] = new CountDownLatch(1);
        RELEASE_REPLAY[0] = new CountDownLatch(1);
        ISSUED.clear();

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(BackgroundPushSagaConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // The replay is under way with two events left to fold, which is what makes the count below mean
                    // something: a replay that is stopped and one that is merely finished are otherwise identical.
                    assertThat(REPLAY_PARKED[0].await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(ISSUED).containsExactly(new ShipOrder("order-1"));

                    // Released after the close has started, so the replay reaches its next event with the shutdown
                    // already signalled. Releasing it before would let it run to the end whatever close() does, and
                    // holding it through the close would only pin how long shutdown waits.
                    Thread.ofVirtual().start(() -> {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        RELEASE_REPLAY[0].countDown();
                    });
                });

        // ApplicationContextRunner closes the context once the lambda returns, so this lands after close(). The
        // remaining history stays unfolded, and stays that way: a replay that survived the close would fold it within
        // milliseconds of being released.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            assertThat(ISSUED).containsExactly(new ShipOrder("order-1"));
            Thread.sleep(50);
        }
    }

    // --- Fixtures ---

    sealed interface OrderEvent {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record OrderState(String orderId) {
    }

    private static CloudEvent orderPlaced(String eventId, String orderId) {
        return CloudEventBuilder.v1(TestConverter.INSTANCE.toCloudEvent(new OrderPlaced(eventId, orderId)))
                .withExtension(new OccurrentCloudEventExtension(orderId, 1L))
                .build();
    }

    static final class RecordingDispatcher implements CommandDispatcher<OrderCommand> {
        @Override
        public void dispatch(OrderCommand command) {
            ISSUED.add(command);
        }
    }

    enum TestConverter implements CloudEventConverter<OrderEvent> {
        INSTANCE;

        @Override
        public CloudEvent toCloudEvent(OrderEvent domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(domainEvent.eventId())
                    .withSource(URI.create("urn:test"))
                    .withType(domainEvent.getClass().getSimpleName())
                    .withSubject(domainEvent.orderId())
                    .build();
        }

        @Override
        public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
            return new OrderPlaced(cloudEvent.getId(), Objects.requireNonNull(cloudEvent.getSubject()));
        }

        @Override
        public String getCloudEventType(Class<? extends OrderEvent> type) {
            return type.getSimpleName();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class BackgroundPushSagaConfiguration {

        @Bean
        CloudEventConverter<OrderEvent> cloudEventConverter() {
            return TestConverter.INSTANCE;
        }

        @Bean
        SagaInstancesRegistryImpl sagaInstancesRegistry() {
            return new SagaInstancesRegistryImpl();
        }

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        SagaStateStore<OrderState> sagaStateStore() {
            return SagaStateStore.inMemory();
        }

        @Bean
        RecordingDispatcher commandDispatcher() {
            return new RecordingDispatcher();
        }

        @Bean
        CheckpointStorage checkpointStorage() {
            return mock(CheckpointStorage.class);
        }

        /**
         * Three events, each its own order, parking on the way into the second. The replay is asked whether to keep
         * going once per event before it is folded, so a stopped replay leaves the second and third unfolded while a
         * surviving one folds both.
         */
        @Bean
        PositionOrderedReader reader() {
            return new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.of(orderPlaced("e1", "order-1"), orderPlaced("e2", "order-2"), orderPlaced("e3", "order-3"))
                            .peek(event -> {
                                if (!"e2".equals(event.getId())) {
                                    return;
                                }
                                REPLAY_PARKED[0].countDown();
                                try {
                                    if (!RELEASE_REPLAY[0].await(10, TimeUnit.SECONDS)) {
                                        throw new IllegalStateException("Timed out waiting to be released");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(e);
                                }
                            });
                }

                @Override
                public long currentPosition() {
                    return 3;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
        }

        @Bean
        BackgroundPushSaga backgroundPushSaga() {
            return new BackgroundPushSaga();
        }
    }

    static class BackgroundPushSaga {
        @Saga(id = "push-background-saga", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, event) -> new OrderState(event.orderId()))
                    .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.issue(new ShipOrder(event.orderId()))))
                    .build();
        }
    }
}
