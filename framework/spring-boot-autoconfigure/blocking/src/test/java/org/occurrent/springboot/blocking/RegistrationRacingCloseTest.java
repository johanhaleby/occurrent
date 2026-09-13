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
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.PushCatchupStatus;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.occurrent.springboot.common.SubscriptionMode;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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
    private static final String PROJECTION_ID = "closing-domain-push-projection";
    private static final String NO_CATCHUP_PROJECTION_ID = "closing-domain-push-projection-without-catchup";
    private static final String MODEL_PROJECTION_ID = "closing-model-push-projection";

    private final ApplicationContextRunner runner = runnerWith(ManualPushSagaConfiguration.class);
    private final ApplicationContextRunner catchingUpRunner = domainFeedRunnerWith(CatchingUpProjectionConfiguration.class);
    private final ApplicationContextRunner noCatchupRunner = domainFeedRunnerWith(NoCatchupProjectionConfiguration.class);
    private final ApplicationContextRunner pushModelRunner = runnerWith(ManualPushModelProjectionConfiguration.class);

    private static ApplicationContextRunner runnerWith(Class<?>... configurations) {
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withBean(ManualStartPushSources.class, ManualStartPushSources::new)
                .withUserConfiguration(configurations);
    }

    private static ApplicationContextRunner domainFeedRunnerWith(Class<?> projectionConfiguration) {
        return runnerWith(ManualDomainFeedConfiguration.class, projectionConfiguration);
    }

    // The symptom the issue names is a timer poller that outlives the context that owns it. The poller runs on a thread
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

    // The other half of refusing. A registration that discovers close() has passed stops what it built and abandons
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

            List<String> started = context.getBean(ManualStartPushSources.class).startAll();
            feed.accept(orderPlaced("e1", "order-1", 1L));

            assertThat(dispatcher.issued).containsExactly(new ShipOrder("order-1"));
            assertThat(started).describedAs("the ids startAll reported as started").containsExactly(SAGA_ID);
        });
    }

    // The second arm again, on the DomainEventFeed path rather than the saga one. Reading history is what a replay
    // does first, so a reader nobody asked is a replay that never started.
    @Test
    void a_push_projection_started_after_the_context_closed_starts_no_replay() {
        catchingUpRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            AtomicInteger historyReads = context.getBean(HistoryReads.class).count;

            ((ConfigurableApplicationContext) context).close();
            pushSources.startAll();

            assertThat(historyReads).describedAs("history reads after the context closed").hasValue(0);
        });
    }

    // Starting no replay was never the whole invariant. register(...) on its own puts the feed into buffering mode,
    // and a feed has no unregister, so a registration that survives a refused start leaves the feed buffering into a
    // bounded buffer that nothing will ever drain, until it overflows into the application's own publish path.
    @Test
    void a_push_projection_started_after_the_context_closed_leaves_the_feed_unregistered() {
        catchingUpRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            DomainEventFeed<?> feed = context.getBean(DomainEventFeed.class);

            ((ConfigurableApplicationContext) context).close();
            pushSources.startAll();

            assertThat(feed.hasProjection()).describedAs("a projection registered on the feed after the context closed").isFalse();
        });
    }

    // catchup = NONE goes live instead of replaying, so a reader that was never asked for history says nothing about
    // it. Three assertions because the branch leaves three traces, the registration, the handover goLive drives, and
    // the status a readiness probe reads. All three fail at the check the deferred block opens with, one level up
    // from the branch they name, which checks nothing itself.
    @Test
    void a_push_projection_that_does_not_catch_up_and_is_started_after_the_context_closed_neither_registers_nor_goes_live() {
        noCatchupRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            DomainEventFeed<?> feed = context.getBean(DomainEventFeed.class);
            PushCatchupStatus status = context.getBean(PushCatchupStatusImpl.class);

            ((ConfigurableApplicationContext) context).close();
            pushSources.startAll();

            assertThat(feed.hasProjection()).describedAs("a projection registered on the feed after the context closed").isFalse();
            assertThat(feed.isReadyForLiveDelivery()).describedAs("a feed taking live events after the context closed").isFalse();
            assertThat(status.of(NO_CATCHUP_PROJECTION_ID))
                    .describedAs("the reported status of a projection that was refused")
                    .isEqualTo(new PushCatchupStatus.Unknown(NO_CATCHUP_PROJECTION_ID));
        });
    }

    // The ordinary paths, so neither refusal above can pass by the projection never starting at all.
    @Test
    void a_push_projection_started_while_the_context_is_open_does_replay() {
        catchingUpRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            AtomicInteger historyReads = context.getBean(HistoryReads.class).count;
            DomainEventFeed<?> feed = context.getBean(DomainEventFeed.class);

            List<String> started = pushSources.startAll();

            assertThat(historyReads).describedAs("history reads while the context is open").hasValue(1);
            assertThat(feed.hasProjection()).describedAs("a projection registered on the feed").isTrue();
            assertThat(started).describedAs("the ids startAll reported as started").containsExactly(PROJECTION_ID);
        });
    }

    @Test
    void a_push_projection_that_does_not_catch_up_and_is_started_while_the_context_is_open_registers_and_goes_live() {
        noCatchupRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            DomainEventFeed<?> feed = context.getBean(DomainEventFeed.class);
            PushCatchupStatus status = context.getBean(PushCatchupStatusImpl.class);

            List<String> started = pushSources.startAll();

            assertThat(feed.hasProjection()).describedAs("a projection registered on the feed").isTrue();
            assertThat(feed.isReadyForLiveDelivery()).describedAs("a feed taking live events").isTrue();
            assertThat(status.of(NO_CATCHUP_PROJECTION_ID))
                    .describedAs("the reported status of a projection that started")
                    .isEqualTo(new PushCatchupStatus.Live(NO_CATCHUP_PROJECTION_ID));
            assertThat(started).describedAs("the ids startAll reported as started").containsExactly(NO_CATCHUP_PROJECTION_ID);
        });
    }

    // What startAll() answers, rather than what the registration did. The two are separate invariants: everything
    // above checks that a refused registration built and left nothing, and these check that the list does not claim
    // it started anyway. Reporting on having found an entry to remove made a closing context tell a readiness probe
    // that every push source came up.
    @Test
    void a_push_saga_started_after_the_context_closed_is_not_reported_as_started() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();

            assertThat(pushSources.startAll()).describedAs("the ids startAll reported as started").isEmpty();
        });
    }

    @Test
    void a_push_projection_started_after_the_context_closed_is_not_reported_as_started() {
        catchingUpRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();

            assertThat(pushSources.startAll()).describedAs("the ids startAll reported as started").isEmpty();
        });
    }

    @Test
    void a_push_projection_that_does_not_catch_up_and_is_started_after_the_context_closed_is_not_reported_as_started() {
        noCatchupRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();

            assertThat(pushSources.startAll()).describedAs("the ids startAll reported as started").isEmpty();
        });
    }

    // The subscription-model path rather than the DomainEventFeed one, and the ordering is why it earns its own
    // fixture. project() subscribes before the closing check runs, so this is the refusal taken with real work
    // already done, and the one a list built from what was claimed could least afford to get wrong.
    @Test
    void a_push_projection_on_a_subscription_model_started_after_the_context_closed_is_not_reported_as_started() {
        pushModelRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();

            assertThat(pushSources.startAll()).describedAs("the ids startAll reported as started").isEmpty();
        });
    }

    // The ordinary path for that fixture, so the test above cannot pass by the projection never starting at all.
    @Test
    void a_push_projection_on_a_subscription_model_started_while_the_context_is_open_is_reported_as_started() {
        pushModelRunner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);

            List<String> started = context.getBean(ManualStartPushSources.class).startAll();
            feed.accept(orderPlaced("e1", "order-1", 1L));

            assertThat(started).describedAs("the ids startAll reported as started").containsExactly(MODEL_PROJECTION_ID);
            assertThat(feed.isRunning(MODEL_PROJECTION_ID)).describedAs("a projection subscribed to the push model").isTrue();
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

    static final class HistoryReads {
        final AtomicInteger count = new AtomicInteger();
    }

    // The DomainEventFeed half of the fixtures. Separate from the saga configuration above because the two paths share
    // nothing but the manual mode that withholds them, and one feed feeds one projection, so a second projection here
    // would need a second feed bean the registrar could not choose between.
    @Configuration(proxyBeanMethods = false)
    static class ManualDomainFeedConfiguration {

        @Bean
        OccurrentProperties occurrentProperties() {
            OccurrentProperties properties = new OccurrentProperties();
            properties.getSubscription().setMode(SubscriptionMode.MANUAL);
            return properties;
        }

        @Bean
        HistoryReads historyReads() {
            return new HistoryReads();
        }

        // Declared so a refused registration can be asked what it reported. The registrar resolves the status bean
        // through getIfAvailable, so without it every recordLive is a call into nothing and the assertion cannot
        // tell the two outcomes apart.
        @Bean
        PushCatchupStatusImpl pushCatchupStatus() {
            return new PushCatchupStatusImpl();
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }

        @Bean
        CloudEventConverter<OrderEvent> domainFeedConverter() {
            return TestConverter.INSTANCE;
        }

        @Bean
        DomainEventFeed<OrderEvent> domainEventFeed(CloudEventConverter<OrderEvent> converter, HistoryReads reads) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    reads.count.incrementAndGet();
                    return Stream.of(orderPlaced("history", "order-1", 1L));
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
            return new DomainEventFeed<>(reader, converter, OrderEvent::eventId);
        }
    }

    // The subscription-model half of the fixtures. Kept apart from the DomainEventFeed configuration because a
    // DomainEventFeed bean in the same context sends the registration down the other path entirely.
    @Configuration(proxyBeanMethods = false)
    static class ManualPushModelProjectionConfiguration {

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
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        PushCatchupStatusImpl pushCatchupStatus() {
            return new PushCatchupStatusImpl();
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }

        @Bean
        ModelBackedPushProjection modelBackedPushProjection() {
            return new ModelBackedPushProjection();
        }
    }

    static class ModelBackedPushProjection {
        @Projection(id = MODEL_PROJECTION_ID, source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CatchingUpProjectionConfiguration {
        @Bean
        ClosingPushProjection closingPushProjection() {
            return new ClosingPushProjection();
        }
    }

    static class ClosingPushProjection {
        @Projection(id = PROJECTION_ID, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NoCatchupProjectionConfiguration {
        @Bean
        ClosingPushProjectionWithoutCatchup closingPushProjectionWithoutCatchup() {
            return new ClosingPushProjectionWithoutCatchup();
        }
    }

    static class ClosingPushProjectionWithoutCatchup {
        @Projection(id = NO_CATCHUP_PROJECTION_ID, source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> projection() {
            return countProjection();
        }
    }

    private static org.occurrent.dsl.projection.Projection<Integer, OrderEvent, String> countProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, OrderEvent, String>builder(0)
                .id(event -> "k")
                .on(OrderPlaced.class, (state, event) -> state + 1)
                .build();
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
