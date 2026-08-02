/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code @Saga(source = PUSH, catchup = NONE)}: the saga takes live events only, so it needs no event store to replay
 * from. That is the only thing that works when the events are written by another application and forwarded over a
 * broker, and it is why this test can run the whole path without Docker, unlike the catch-up variant.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationPushWithoutCatchupTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(PushOnlySagaConfiguration.class);

    @Test
    void the_context_starts_with_no_event_store_beans_at_all() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            // Not incidental: catch-up resolves both of these by type, so a saga fed by a foreign broker could not
            // start if the registrar asked for them regardless of catchup.
            assertThat(context.getBeanNamesForType(PositionOrderedReader.class)).isEmpty();
            assertThat(context.getBeanNamesForType(CheckpointStorage.class)).isEmpty();
        });
    }

    @Test
    void an_event_pushed_to_the_feed_reaches_the_command_dispatcher() {
        runner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            RecordingDispatcher dispatcher = context.getBean(RecordingDispatcher.class);

            feed.accept(orderPlaced("e1", "order-1", 1L));

            assertThat(dispatcher.issued).containsExactly(new ShipOrder("order-1"));
        });
    }

    @Test
    void a_redelivered_event_is_still_recognised_without_a_catch_up_in_front_of_the_feed() {
        runner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            RecordingDispatcher dispatcher = context.getBean(RecordingDispatcher.class);
            CloudEvent placed = orderPlaced("e1", "order-2", 1L);

            // At-least-once delivery, which is what a broker gives you
            feed.accept(placed);
            feed.accept(placed);

            assertThat(dispatcher.issued).containsExactly(new ShipOrder("order-2"));
        });
    }

    @Test
    void the_saga_is_published_under_its_own_bean_name() {
        runner.run(context ->
                assertThat(context).hasBean(SagaAnnotationRegistrar.sagaInstancesBeanName("push-only-saga")));
    }

    @Test
    void an_event_store_saga_that_sets_catchup_is_pointed_at_start_at_instead() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(PushOnlySagaConfiguration.class, EventStoreCatchupConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .hasMessageContaining("sets catchup, which only applies to source=PUSH")
                            .hasMessageContaining("startAt = NOW to skip it");
                });
    }

    @Test
    void a_push_saga_with_catchup_none_still_cannot_set_a_start_position() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(PushOnlySagaConfiguration.class, PushCatchupNoneStartAtConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .hasMessageContaining("cannot set startAt, startAtGlobalPosition or resumeBehavior")
                            .hasMessageContaining("With catchup=NONE it takes live events only");
                });
    }

    @Test
    void a_push_saga_with_catchup_none_cannot_set_a_startup_mode_either_since_it_replays_nothing() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(PushOnlySagaConfiguration.class, PushCatchupNoneStartupModeConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                            .hasMessageContaining("replays nothing and there is no startup work for startupMode to decide about")
                            .hasMessageContaining("drop catchup=NONE if you meant the saga to catch up first");
                });
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

    private static CloudEvent orderPlaced(String eventId, String orderId, long streamVersion) {
        return CloudEventBuilder.v1(TestConverter.INSTANCE.toCloudEvent(new OrderPlaced(eventId, orderId)))
                .withExtension(new OccurrentCloudEventExtension(orderId, streamVersion))
                .build();
    }

    /**
     * Reacts on every {@code OrderPlaced} and never becomes terminal, so a second delivery of the same event is
     * governed by the redelivery dedup alone rather than by the terminal-instance skip, which would hide it.
     */
    private static org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> shipsOnEveryPlaced() {
        return org.occurrent.dsl.saga.Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, event) -> new OrderState(event.orderId()))
                .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.issue(new ShipOrder(event.orderId()))))
                .build();
    }

    // A named type rather than a lambda, so the test can pull the recorded commands back out of the context.
    static final class RecordingDispatcher implements CommandDispatcher<OrderCommand> {
        final CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();

        @Override
        public void dispatch(OrderCommand command) {
            issued.add(command);
        }
    }

    // The order id travels in the subject, which is all the saga needs to correlate.
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
    static class PushOnlySagaConfiguration {
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

        @Bean
        PushOnlySaga pushOnlySaga() {
            return new PushOnlySaga();
        }
    }

    static class PushOnlySaga {
        @Saga(id = "push-only-saga", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return shipsOnEveryPlaced();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EventStoreCatchupConfiguration {
        @Bean
        EventStoreCatchupSaga eventStoreCatchupSaga() {
            return new EventStoreCatchupSaga();
        }
    }

    static class EventStoreCatchupSaga {
        @Saga(id = "event-store-catchup", catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return shipsOnEveryPlaced();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushCatchupNoneStartAtConfiguration {
        @Bean
        PushCatchupNoneStartAtSaga pushCatchupNoneStartAtSaga() {
            return new PushCatchupNoneStartAtSaga();
        }
    }

    static class PushCatchupNoneStartAtSaga {
        @Saga(id = "push-none-start-at", source = Source.PUSH, catchup = Catchup.NONE, startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return shipsOnEveryPlaced();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushCatchupNoneStartupModeConfiguration {
        @Bean
        PushCatchupNoneStartupModeSaga pushCatchupNoneStartupModeSaga() {
            return new PushCatchupNoneStartupModeSaga();
        }
    }

    static class PushCatchupNoneStartupModeSaga {
        @Saga(id = "push-none-startup-mode", source = Source.PUSH, catchup = Catchup.NONE, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return shipsOnEveryPlaced();
        }
    }
}
