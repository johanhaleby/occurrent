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
import org.occurrent.annotation.Catchup;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.PushCatchupStatus;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A {@code @Saga(source = PUSH)} reports where its catch-up is on the same {@link PushCatchupStatus} bean a push
 * projection uses. Before this it recorded nothing at all, not even failures.
 * <p>
 * The failing case is the one worth having. Under {@code startupMode = BACKGROUND} nobody joins the replay, and the
 * subscription model forgets a replay that failed while keeping the registration that now refuses events. So asking
 * the model alone would answer {@code Live} for a saga that is dead, which is worse than saying nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaPushCatchupStatusTest {

    @Test
    void a_background_catch_up_that_fails_is_reported_as_failed_rather_than_live() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaSupportConfiguration.class, FailingCatchupSagaConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    PushCatchupStatus status = context.getBean(PushCatchupStatus.class);

                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (!(status.of("push-saga-failing") instanceof PushCatchupStatus.Failed) && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }

                    assertThat(status.of("push-saga-failing")).isInstanceOfSatisfying(PushCatchupStatus.Failed.class, failed ->
                            assertThat(failed.cause()).isInstanceOf(RuntimeException.class).hasMessage("replay boom"));
                    assertThat(status.isCaughtUp("push-saga-failing")).isFalse();
                });
    }

    @Test
    void a_saga_that_replays_nothing_is_live_from_the_start() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(SagaSupportConfiguration.class, NoCatchupSagaConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    PushCatchupStatus status = context.getBean(PushCatchupStatus.class);

                    assertThat(status.of("push-saga-no-catchup")).isEqualTo(new PushCatchupStatus.Live("push-saga-no-catchup"));
                    assertThat(status.isCaughtUp("push-saga-no-catchup")).isTrue();
                });
    }

    // --- Fixtures, the same shape as SagaAnnotationPushWithoutCatchupTest ---

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

    private static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> shipsOnEveryPlaced() {
        return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, event) -> new OrderState(event.orderId()))
                .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.issue(new ShipOrder(event.orderId()))))
                .build();
    }

    static final class RecordingDispatcher implements CommandDispatcher<OrderCommand> {
        final CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();

        @Override
        public void dispatch(OrderCommand command) {
            issued.add(command);
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
    static class SagaSupportConfiguration {

        @Bean
        PushCatchupStatusImpl pushCatchupStatus() {
            return new PushCatchupStatusImpl();
        }

        @Bean
        CloudEventConverter<OrderEvent> cloudEventConverter() {
            return TestConverter.INSTANCE;
        }

        @Bean
        OccurrentProperties occurrentProperties() {
            return new OccurrentProperties();
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
    }

    @Configuration(proxyBeanMethods = false)
    static class FailingCatchupSagaConfiguration {

        @Bean
        CheckpointStorage checkpointStorage() {
            return mock(CheckpointStorage.class);
        }

        // Fails the replay outright. This test is about where the failure ends up, not about timing.
        @Bean
        PositionOrderedReader reader() {
            return new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    throw new RuntimeException("replay boom");
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
        FailingCatchupSaga failingCatchupSaga() {
            return new FailingCatchupSaga();
        }
    }

    static class FailingCatchupSaga {
        @Saga(id = "push-saga-failing", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return shipsOnEveryPlaced();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NoCatchupSagaConfiguration {
        @Bean
        NoCatchupSaga noCatchupSaga() {
            return new NoCatchupSaga();
        }
    }

    static class NoCatchupSaga {
        @Saga(id = "push-saga-no-catchup", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return shipsOnEveryPlaced();
        }
    }
}
