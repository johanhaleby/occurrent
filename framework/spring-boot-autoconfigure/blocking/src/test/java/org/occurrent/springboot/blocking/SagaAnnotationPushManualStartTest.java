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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.SubscriptionMode;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * {@code occurrent.subscription.mode=manual} against a {@code @Saga(source = PUSH)}. A push feed is a bean the
 * application supplies, so the withholding that mode applies to Occurrent's own {@code SubscriptionModel} never reaches
 * it. Without the deferral this test pins, such a saga would start issuing commands at boot after being told to wait,
 * which is the failure manual mode exists to prevent (a saga behind a leader election issuing commands on every node).
 * <p>
 * {@code catchup = NONE} throughout, so the whole path runs with no event store beans and no Docker.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationPushManualStartTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withBean(ManualStartPushSources.class, ManualStartPushSources::new)
            .withUserConfiguration(ManualPushSagaConfiguration.class);

    @Test
    void a_withheld_push_saga_issues_no_commands_for_an_event_pushed_before_it_is_started() {
        runner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            RecordingDispatcher dispatcher = context.getBean(RecordingDispatcher.class);

            feed.accept(orderPlaced("e1", "order-1", 1L));

            assertThat(dispatcher.issued).isEmpty();
        });
    }

    @Test
    void a_withheld_push_saga_is_reported_as_pending_under_its_own_id() {
        runner.run(context ->
                assertThat(context.getBean(ManualStartPushSources.class).pendingIds()).containsExactly("manual-push-saga"));
    }

    @Test
    void starting_it_puts_the_saga_on_the_feed_so_a_later_event_reaches_the_dispatcher() {
        runner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            RecordingDispatcher dispatcher = context.getBean(RecordingDispatcher.class);

            context.getBean(ManualStartPushSources.class).start("manual-push-saga");
            feed.accept(orderPlaced("e1", "order-1", 1L));

            assertThat(dispatcher.issued).containsExactly(new ShipOrder("order-1"));
        });
    }

    @Test
    void starting_it_takes_it_off_the_pending_list() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            pushSources.start("manual-push-saga");

            assertThat(pushSources.pendingIds()).isEmpty();
        });
    }

    @Test
    void start_all_starts_a_withheld_saga_the_same_way_a_withheld_projection_is_started() {
        runner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            RecordingDispatcher dispatcher = context.getBean(RecordingDispatcher.class);

            assertThat(context.getBean(ManualStartPushSources.class).startAll()).containsExactly("manual-push-saga");
            feed.accept(orderPlaced("e1", "order-1", 1L));

            assertThat(dispatcher.issued).containsExactly(new ShipOrder("order-1"));
        });
    }

    /**
     * The observation view is not withheld with the saga. An application deciding whether to start this saga is exactly
     * the caller that wants to look at the instances it already has, and that read needs the state store rather than a
     * running subscription.
     */
    @Test
    void the_instances_of_a_withheld_saga_can_be_observed_before_it_is_started() {
        runner.run(context -> {
            assertThat(context).hasBean(SagaAnnotationRegistrar.sagaInstancesBeanName("manual-push-saga"));
            SagaInstances instances = context.getBean(SagaAnnotationRegistrar.sagaInstancesBeanName("manual-push-saga"), SagaInstances.class);

            assertThat(instances.find("order-1")).isEmpty();
        });
    }

    @Test
    void the_running_subscription_is_published_only_once_the_withheld_saga_is_started() {
        runner.run(context -> {
            String beanName = SagaAnnotationRegistrar.sagaSubscriptionBeanName("manual-push-saga");
            // Nothing is running yet, so there is no subscription to hand anyone.
            assertThat(context).doesNotHaveBean(beanName);

            context.getBean(ManualStartPushSources.class).start("manual-push-saga");

            SagaSubscription subscription = context.getBean(beanName, SagaSubscription.class);
            SagaInstances published = context.getBean(SagaAnnotationRegistrar.sagaInstancesBeanName("manual-push-saga"), SagaInstances.class);

            // Not the same object as the published one. A withheld saga gets its observation view at refresh, before
            // there is a subscription to publish, so the two read the same store through different handles.
            assertAll(
                    () -> assertThat(subscription.id()).isEqualTo("manual-push-saga"),
                    () -> assertThat(subscription.instances().find("order-1")).isEqualTo(published.find("order-1"))
            );
        });
    }

    @Test
    void a_push_saga_under_auto_mode_is_not_withheld_at_all() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withBean(ManualStartPushSources.class, ManualStartPushSources::new)
                .withUserConfiguration(AutoModePushSagaConfiguration.class)
                .run(context -> {
                    PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
                    RecordingDispatcher dispatcher = context.getBean(RecordingDispatcher.class);

                    feed.accept(orderPlaced("e1", "order-1", 1L));

                    assertThat(dispatcher.issued).containsExactly(new ShipOrder("order-1"));
                    assertThat(context.getBean(ManualStartPushSources.class).pendingIds()).isEmpty();
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
    static class ManualPushSagaConfiguration extends PushSagaFixture {
        @Bean
        OccurrentProperties occurrentProperties() {
            OccurrentProperties properties = new OccurrentProperties();
            properties.getSubscription().setMode(SubscriptionMode.MANUAL);
            return properties;
        }

        @Bean
        ManualPushSaga manualPushSaga() {
            return new ManualPushSaga();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AutoModePushSagaConfiguration extends PushSagaFixture {
        @Bean
        OccurrentProperties occurrentProperties() {
            OccurrentProperties properties = new OccurrentProperties();
            properties.getSubscription().setMode(SubscriptionMode.AUTO);
            return properties;
        }

        @Bean
        ManualPushSaga manualPushSaga() {
            return new ManualPushSaga();
        }
    }

    // The beans a push saga needs whatever the mode is, so the two configurations differ only in the mode.
    static class PushSagaFixture {
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
    }

    static class ManualPushSaga {
        @Saga(id = "manual-push-saga", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.saga.Saga<OrderEvent, OrderState, OrderCommand> saga() {
            return shipsOnEveryPlaced();
        }
    }
}
