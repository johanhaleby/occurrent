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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.SubscriptionMode;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pair invariant behind <a href="https://github.com/johanhaleby/occurrent/issues/988">issue 988</a>: every poller
 * or model a registration creates is either stopped by the {@code close()} that follows it, or stopped by the
 * registration itself because that {@code close()} has already gone past. Never neither.
 * <p>
 * This aims at the second arm, which is the one that can be driven deterministically. {@code close()} runs first and
 * the registration second, with nothing interleaved, so what it exercises is the recheck of the closing flag rather
 * than a race. That is deliberate. A test that tried to stage the interleaving would either pass without the fix or
 * park a thread inside a bean factory and serialise the two threads the hazard needs, which is what
 * {@link LateRegistrationConcurrencyContractTest} says at greater length.
 * <p>
 * The manual-start path is used because it reaches a registration from application code on an application thread, so
 * a close that has already happened is expressible without reflection or a stubbed registrar. That path predates the
 * late-bean registration 981 added, and it is the one that already leaked on a released version.
 * <p>
 * {@code catchup = NONE} throughout, so the whole path runs with no event store beans and no Docker.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RegistrationRacingCloseTest {

    private static final String SAGA_ID = "closing-push-saga";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withBean(ManualStartPushSources.class, ManualStartPushSources::new)
            .withUserConfiguration(ManualPushSagaConfiguration.class);

    // The symptom the issue names: a timer poller that outlives the context that owns it. The poller runs on a thread
    // named after its subscription, so a surviving one is visible by name rather than through the registrar's state.
    @Test
    void a_push_saga_started_after_the_context_closed_leaves_no_timer_poller_running() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();
            pushSources.startAll();

            assertThat(liveTimerPollerThreads()).describedAs("timer poller threads outliving the context").isEmpty();
        });
    }

    // The other half of refusing: a registration that discovers close() has passed stops what it built and abandons
    // the rest, rather than half-completing. Publishing the handle would tell the application it has a running saga
    // when the poller behind it has just been stopped.
    @Test
    void a_push_saga_started_after_the_context_closed_publishes_no_handle_to_itself() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            ConfigurableApplicationContext closed = (ConfigurableApplicationContext) context;

            closed.close();
            pushSources.startAll();

            assertThat(closed.getBeanFactory().containsSingleton(SagaAnnotationRegistrar.sagaSubscriptionBeanName(SAGA_ID)))
                    .describedAs("a handle published for a saga that was refused")
                    .isFalse();
        });
    }

    // The ordinary path, so the two tests above cannot pass by the saga never starting at all. Without this a fix
    // that refused every registration would look correct.
    @Test
    void a_push_saga_started_while_the_context_is_open_does_run() {
        runner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            RecordingDispatcher dispatcher = context.getBean(RecordingDispatcher.class);

            context.getBean(ManualStartPushSources.class).startAll();
            feed.accept(orderPlaced("e1", "order-1", 1L));

            assertThat(dispatcher.issued).containsExactly(new ShipOrder("order-1"));
        });
    }

    private static List<String> liveTimerPollerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith("occurrent-saga-timer-" + SAGA_ID))
                .toList();
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
    static class ManualPushSagaConfiguration {
        @Bean
        OccurrentProperties occurrentProperties() {
            OccurrentProperties properties = new OccurrentProperties();
            properties.getSubscription().setMode(SubscriptionMode.MANUAL);
            return properties;
        }

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
        ClosingPushSaga closingPushSaga() {
            return new ClosingPushSaga();
        }
    }

    static class ClosingPushSaga {
        @Saga(id = SAGA_ID, source = Source.PUSH, catchup = Catchup.NONE)
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
