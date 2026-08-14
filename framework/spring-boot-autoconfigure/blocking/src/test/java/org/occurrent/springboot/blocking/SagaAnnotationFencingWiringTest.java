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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * {@code @Saga(source = PUSH)} with the default catch-up: {@link SagaAnnotationRegistrar} builds a
 * {@code CatchupThenPushSubscriptionModel} for the feed and has to reach it with a
 * {@link org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource} built over a lazily resolved
 * {@link CompetingConsumerStrategy} bean (ADR 116), independently of {@code occurrent.saga.competing-consumer},
 * which gates a different strategy use (the timer poller) entirely. Proven the same way
 * {@link ProjectionAnnotationFencingWiringTest} proves the projection registrar, with a finite fake reader whose one
 * event completes the catch-up synchronously, and the marker write that follows carries the condition under test.
 * Container-free, the same way {@link SagaAnnotationPushShutdownTest} is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationFencingWiringTest {

    private static final String SUBSCRIPTION_ID = "saga-fenced";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(FencedPushSagaConfiguration.class);

    @Test
    void one_strategy_bean_stamps_the_catch_up_marker_write_not_older_than_its_token() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(42L));
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.evaluatesWriteConditions()).thenReturn(true);
        // Mockito does not fall back to the interface's own default (which would delegate to the stub above), so
        // this needs its own stub for the id-specific check the fencing check also runs now.
        when(checkpointStorage.evaluatesWriteConditionsFor(any())).thenReturn(true);

        runner.withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    verify(checkpointStorage).save(eq(SUBSCRIPTION_ID), any(), eq(CheckpointWriteCondition.notOlderThan(42L)));
                });
    }

    @Test
    void several_strategy_beans_refuse_to_start_rather_than_writing_the_marker_unconditionally() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);

        runner.withBean("primaryStrategy", CompetingConsumerStrategy.class, () -> strategy)
                .withBean("rivalStrategy", CompetingConsumerStrategy.class, RivalCompetingConsumerStrategy::new)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).getFailure()
                            .isInstanceOf(AmbiguousCompetingConsumerStrategyException.class)
                            .hasMessageContaining("rivalStrategy");
                    verify(checkpointStorage, never()).save(any(), any(), any());
                });
    }

    @Test
    void no_strategy_bean_leaves_the_catch_up_marker_write_unconditional() {
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);

        runner.withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    verify(checkpointStorage).save(eq(SUBSCRIPTION_ID), any(), eq(CheckpointWriteCondition.any()));
                });
    }

    private static CloudEvent orderPlaced(String eventId, String orderId) {
        return CloudEventBuilder.v1(TestConverter.INSTANCE.toCloudEvent(new OrderPlaced(eventId, orderId)))
                .withExtension(new OccurrentCloudEventExtension(orderId, 1L))
                .build();
    }

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
            return new OrderPlaced(cloudEvent.getId(), cloudEvent.getSubject());
        }

        @Override
        public String getCloudEventType(Class<? extends OrderEvent> type) {
            return type.getSimpleName();
        }
    }

    private static final class RivalCompetingConsumerStrategy implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FencedPushSagaConfiguration {

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
        CommandDispatcher<OrderCommand> commandDispatcher() {
            return command -> {
            };
        }

        // One event, so the catch-up (which the default startupMode waits for) reaches the end of the reader and
        // writes its one-shot marker, the write this test class observes the condition of.
        @Bean
        PositionOrderedReader reader() {
            return new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.of(orderPlaced("e1", "order-1"));
                }

                @Override
                public long currentPosition() {
                    return 1;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
        }

        @Bean
        FencedPushSaga fencedPushSaga() {
            return new FencedPushSaga();
        }
    }

    static class FencedPushSaga {
        @Saga(id = SUBSCRIPTION_ID, source = Source.PUSH)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, event) -> new OrderState(event.orderId()))
                    .build();
        }
    }
}
