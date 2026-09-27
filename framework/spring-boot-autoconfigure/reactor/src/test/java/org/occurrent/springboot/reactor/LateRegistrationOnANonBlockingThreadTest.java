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

package org.occurrent.springboot.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Capability;
import org.occurrent.annotation.Catchup;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.FluxSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

/**
 * A bean built after startup registers its annotations on whichever thread asks for it. Asked for from a Reactor
 * parallel thread, a WebFlux handler or a {@code Schedulers.parallel()} task for example, the registration must not
 * call {@code block()}, which throws there, so the bean resolves and its projection, snapshot or subscription receives
 * events. That includes the {@code block()} that {@code ReactorDurableSubscriptionModel} calls inside {@code subscribe}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class LateRegistrationOnANonBlockingThreadTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(BaseConfiguration.class);

    @Test
    void a_lazy_event_store_projection_resolved_on_a_parallel_thread_folds_the_events_it_is_delivered() {
        runner.withUserConfiguration(RecordingSubscribableConfiguration.class, LazyEventStoreProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "eventStoreProjectionHolder")).isInstanceOf(EventStoreProjectionHolder.class);
            RecordingSubscribable subscribable = context.getBean(RecordingSubscribable.class);
            awaitUntil(() -> subscribable.isSubscribed("late-event-store-projection"));
            subscribable.deliver("late-event-store-projection", cloudEvent("1", "stream", 1));

            assertThat(readModel(context).get("k")).isEqualTo(1);
        });
    }

    // Two catch-ups on one bean, because the first catch-up failing used to leave the second queued and its feed
    // buffering with nothing left to drain it.
    @Test
    void a_lazy_beans_domain_feed_catch_ups_resolved_on_a_parallel_thread_both_fold_their_history_and_then_go_live() {
        runner.withUserConfiguration(RecordingSubscribableConfiguration.class, TwoDomainFeedsConfiguration.class, LazyDomainFeedCatchUpConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "domainFeedCatchUpHolder")).isInstanceOf(DomainFeedCatchUpHolder.class);

            Map<String, Integer> readModel = readModel(context);
            awaitUntil(() -> readModel.get("a") != null && readModel.get("b") != null);
            assertThat(readModel).containsEntry("a", 1).containsEntry("b", 1);

            domainFeed(context, "feedA").accept(new TestEvent("live")).block(Duration.ofSeconds(5));
            assertThat(readModel).containsEntry("a", 2);
        });
    }

    @Test
    void a_lazy_domain_feed_projection_without_catch_up_resolved_on_a_parallel_thread_folds_a_live_event() {
        runner.withUserConfiguration(RecordingSubscribableConfiguration.class, TwoDomainFeedsConfiguration.class, LazyDomainFeedWithoutCatchUpConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "domainFeedWithoutCatchUpHolder")).isInstanceOf(DomainFeedWithoutCatchUpHolder.class);
            domainFeed(context, "feedA").accept(new TestEvent("live")).block(Duration.ofSeconds(5));

            Map<String, Integer> readModel = readModel(context);
            awaitUntil(() -> readModel.get("a") != null);
            assertThat(readModel).containsEntry("a", 1);
        });
    }

    @Test
    void a_lazy_push_model_projection_resolved_on_a_parallel_thread_folds_its_history_and_then_a_pushed_event() {
        runner.withUserConfiguration(PushModelConfiguration.class, LazyPushModelProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "pushModelProjectionHolder")).isInstanceOf(PushModelProjectionHolder.class);

            Map<String, Integer> readModel = readModel(context);
            awaitUntil(() -> readModel.get("k") != null);
            assertThat(readModel).containsEntry("k", 1);

            context.getBean(PushSubscriptionModel.class).accept(cloudEvent("live", "stream", 2)).block(Duration.ofSeconds(5));
            awaitUntil(() -> Integer.valueOf(2).equals(readModel.get("k")));
            assertThat(readModel).containsEntry("k", 2);
        });
    }

    @Test
    void a_lazy_snapshot_resolved_on_a_parallel_thread_saves_a_snapshot_for_the_event_it_is_delivered() {
        runner.withUserConfiguration(RecordingSubscribableConfiguration.class, SnapshotConfiguration.class, LazySnapshotConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "snapshotHolder")).isInstanceOf(SnapshotHolder.class);
            RecordingSubscribable subscribable = context.getBean(RecordingSubscribable.class);
            awaitUntil(() -> subscribable.isSubscribed("late-snapshot"));
            subscribable.deliver("late-snapshot", cloudEvent("1", "stream", 1));

            @SuppressWarnings("unchecked")
            ReactiveSnapshotStore<Integer> store = context.getBean(ReactiveSnapshotStore.class);
            assertThat(store.findLatest("stream").map(org.occurrent.dsl.snapshot.Snapshot::state).block(Duration.ofSeconds(5))).isEqualTo(1);
        });
    }

    @Test
    void a_lazy_dcb_projection_resolved_on_a_parallel_thread_folds_the_events_it_is_delivered() {
        runner.withUserConfiguration(RecordingSubscribableConfiguration.class, LazyDcbProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "dcbProjectionHolder")).isInstanceOf(DcbProjectionHolder.class);
            RecordingSubscribable subscribable = context.getBean(RecordingSubscribable.class);
            awaitUntil(() -> subscribable.isSubscribed("late-dcb-projection"));
            subscribable.deliver("late-dcb-projection", dcbCloudEvent("1"));

            assertThat(readModel(context).get("k")).isEqualTo(1);
        });
    }

    @Test
    void a_lazy_stream_snapshot_resolved_on_a_parallel_thread_saves_a_snapshot_for_the_event_it_is_delivered() {
        runner.withUserConfiguration(RecordingSubscribableConfiguration.class, SnapshotConfiguration.class, LazyStreamSnapshotConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "streamSnapshotHolder")).isInstanceOf(StreamSnapshotHolder.class);
            RecordingSubscribable subscribable = context.getBean(RecordingSubscribable.class);
            awaitUntil(() -> subscribable.isSubscribed("late-stream-snapshot"));
            subscribable.deliver("late-stream-snapshot", cloudEvent("1", "stream", 1));

            @SuppressWarnings("unchecked")
            ReactiveSnapshotStore<Integer> store = context.getBean(ReactiveSnapshotStore.class);
            assertThat(store.findLatest("stream").map(org.occurrent.dsl.snapshot.Snapshot::state).block(Duration.ofSeconds(5))).isEqualTo(1);
        });
    }

    // Registration only. Saving a DCB snapshot reads the boundary back from the DCB event store, which is a mock here.
    @Test
    void a_lazy_dcb_snapshot_resolved_on_a_parallel_thread_subscribes() {
        runner.withUserConfiguration(RecordingSubscribableConfiguration.class, SnapshotConfiguration.class, LazyDcbSnapshotConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "dcbSnapshotHolder")).isInstanceOf(DcbSnapshotHolder.class);

            RecordingSubscribable subscribable = context.getBean(RecordingSubscribable.class);
            awaitUntil(() -> subscribable.isSubscribed("late-dcb-snapshot"));
            assertThat(subscribable.isSubscribed("late-dcb-snapshot")).isTrue();
        });
    }

    // ReactorDurableSubscriptionModel, the model the reactive MongoDB starter registers, reads the stored position with
    // block() inside subscribe for a DEFAULT start, so the subscribe itself cannot run on the thread that built the bean.
    @Test
    void a_lazy_event_store_projection_on_the_durable_model_resolved_on_a_parallel_thread_folds_the_events_it_is_delivered() {
        runner.withUserConfiguration(DurableModelConfiguration.class, LazyEventStoreProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "eventStoreProjectionHolder")).isInstanceOf(EventStoreProjectionHolder.class);
            RecordingDelegate delegate = delegate(context);
            awaitUntil(() -> delegate.isSubscribed("late-event-store-projection"));
            delegate.deliver("late-event-store-projection", cloudEvent("1", "stream", 1));

            assertThat(readModel(context).get("k")).isEqualTo(1);
        });
    }

    @Test
    void a_lazy_subscription_on_the_durable_model_resolved_on_a_parallel_thread_handles_the_events_it_is_delivered() {
        runner.withUserConfiguration(DurableModelConfiguration.class, LazySubscriptionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "subscriptionHolder")).isInstanceOf(SubscriptionHolder.class);
            RecordingDelegate delegate = delegate(context);
            awaitUntil(() -> delegate.isSubscribed("late-subscription"));
            delegate.deliver("late-subscription", cloudEvent("1", "stream", 1));

            assertThat(handled(context)).containsExactly("1");
        });
    }

    @Test
    void a_lazy_snapshot_on_the_durable_model_resolved_on_a_parallel_thread_saves_a_snapshot_for_the_event_it_is_delivered() {
        runner.withUserConfiguration(DurableModelConfiguration.class, SnapshotStoreConfiguration.class, LazyDefaultStartSnapshotConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "defaultStartSnapshotHolder")).isInstanceOf(DefaultStartSnapshotHolder.class);
            RecordingDelegate delegate = delegate(context);
            awaitUntil(() -> delegate.isSubscribed("late-default-start-snapshot"));
            delegate.deliver("late-default-start-snapshot", cloudEvent("1", "stream", 1));

            @SuppressWarnings("unchecked")
            ReactiveSnapshotStore<Integer> store = context.getBean(ReactiveSnapshotStore.class);
            assertThat(store.findLatest("stream").map(org.occurrent.dsl.snapshot.Snapshot::state).block(Duration.ofSeconds(5))).isEqualTo(1);
        });
    }

    // The subscribe is parked reading the stored position when the context starts closing. The model shuts down
    // while beans are destroyed, so a close that went ahead without waiting would have the subscribe register on a
    // model that had already shut down, and nothing would ever stop that subscription.
    @Test
    void closing_the_context_waits_for_a_late_subscribe_so_the_subscription_model_stops_it() {
        runner.withUserConfiguration(DurableModelConfiguration.class, LazyEventStoreProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            ParkingCheckpointStorage storage = context.getBean(ParkingCheckpointStorage.class);
            CountDownLatch release = new CountDownLatch(1);
            storage.parkReadsUntil(release);
            RecordingDelegate delegate = delegate(context);

            resolvedOnAParallelThread(context, "eventStoreProjectionHolder");
            assertThat(storage.parked.await(5, TimeUnit.SECONDS)).describedAs("the subscribe reached the position read").isTrue();

            Thread closing = Thread.ofVirtual().start(context::close);
            assertThat(delegate.shutDown.await(500, TimeUnit.MILLISECONDS)).describedAs("the model shut down while the subscribe was still running").isFalse();

            release.countDown();
            assertThat(closing.join(Duration.ofSeconds(10))).describedAs("the context finished closing").isTrue();
            assertThat(delegate.shutDown.getCount()).describedAs("the model shut down once the subscribe finished").isZero();
            assertThat(delegate.actions).describedAs("subscriptions still registered after the context closed").isEmpty();
        });
    }

    // A subscribe that fails after the bean was returned gives back the id and the handler, the same as one that fails
    // while the bean is being built, so the next instance of a prototype registers.
    @Test
    void a_prototype_whose_late_subscribe_failed_registers_when_it_is_built_again() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PrototypeEventStoreProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);
            delegate.refuseNextSubscribes(1);

            resolvedOnAParallelThread(context, "prototypeProjectionHolder");
            awaitUntil(() -> delegate.refusals.get() == 1);
            assertThat(delegate.refusals).hasValue(1);

            // The release runs just after the refusal, on the thread that ran the subscribe, so a build in between
            // still finds the handler taken and registers nothing. Building again until one registers covers that.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!delegate.isSubscribed("late-prototype-projection") && System.nanoTime() < deadline) {
                resolvedOnAParallelThread(context, "prototypeProjectionHolder");
                Thread.sleep(50);
            }
            assertThat(delegate.isSubscribed("late-prototype-projection")).isTrue();
        });
    }

    // Nothing waits for a subscribe moved off a non-blocking thread, so one the scheduler has not run yet when the
    // context closes must not run at all.
    @Test
    void a_late_subscribe_that_has_not_run_when_the_context_closes_never_runs() {
        List<Runnable> queued = new CopyOnWriteArrayList<>();
        LateSubscriber subscriber = new LateSubscriber(Schedulers.fromExecutor(queued::add));
        AtomicBoolean subscribed = new AtomicBoolean(false);
        Mono.fromRunnable(() -> subscriber.subscribe("a test registration", () -> subscribed.set(true), () -> {
                }))
                .subscribeOn(Schedulers.parallel())
                .block(Duration.ofSeconds(5));
        assertThat(queued).hasSize(1);

        subscriber.close();
        queued.forEach(Runnable::run);

        assertThat(subscribed).isFalse();
    }

    // Asserts the bean was not built at startup and the thread really is one Reactor refuses to block on, so a pass
    // cannot come from the bean being registered somewhere block() is allowed.
    private static Object resolvedOnAParallelThread(ConfigurableApplicationContext context, String beanName) {
        assertThat(context.getBeanFactory().containsSingleton(beanName)).describedAs("%s is still unbuilt after startup", beanName).isFalse();
        AtomicBoolean nonBlocking = new AtomicBoolean(false);
        AtomicReference<Object> bean = new AtomicReference<>();
        assertThatCode(() -> bean.set(Mono.fromCallable(() -> {
                    nonBlocking.set(Schedulers.isInNonBlockingThread());
                    return context.getBean(beanName);
                })
                .subscribeOn(Schedulers.parallel())
                .block(Duration.ofSeconds(10))))
                .describedAs("resolving %s on a Reactor parallel thread", beanName)
                .doesNotThrowAnyException();
        assertThat(nonBlocking).describedAs("the bean was built on a non-blocking thread").isTrue();
        return bean.get();
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Integer> readModel(ApplicationContext context) {
        return context.getBean("readModel", Map.class);
    }

    @SuppressWarnings("unchecked")
    private static DomainEventFeed<TestEvent> domainFeed(ApplicationContext context, String name) {
        return context.getBean(name, DomainEventFeed.class);
    }

    private static CloudEvent cloudEvent(String id, String streamId, long streamVersion) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("TestEvent")
                .withExtension(new OccurrentCloudEventExtension(streamId, streamVersion))
                .build();
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> countProjection(String key) {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> key)
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    record TestEvent(String id) {
    }

    // What every @Bean below declares to return. The startup scan reads a lazy bean through that declared type, finds
    // no annotation on it and does not build the bean, so its annotation registers only when a test asks for it.
    interface Marker {
    }

    // A subscription model that keeps each registered action, so a test can deliver to it by id. Its
    // waitUntilStarted() completes as soon as it is subscribed. It is deferred because Mono.empty() and
    // Mono.fromRunnable() override block() and return without checking the thread.
    static class RecordingSubscribable implements Subscribable, FluxSubscriptionModel {
        private final Map<String, Function<CloudEvent, Mono<Void>>> actions = new ConcurrentHashMap<>();

        @Override
        public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            actions.put(subscriptionId, action);
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.defer(Mono::empty);
                }
            };
        }

        // Only here so the DCB runners, which take a FluxSubscriptionModel, accept this model. Nothing reads it.
        @Override
        public Flux<CloudEvent> subscribe(SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }

        boolean isSubscribed(String subscriptionId) {
            return actions.containsKey(subscriptionId);
        }

        void deliver(String subscriptionId, CloudEvent cloudEvent) {
            assertThat(actions).describedAs("registered subscriptions").containsKey(subscriptionId);
            actions.get(subscriptionId).apply(cloudEvent).block(Duration.ofSeconds(5));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class BaseConfiguration {
        @Bean
        Map<String, Integer> readModel() {
            return new ConcurrentHashMap<>();
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository(Map<String, Integer> readModel) {
            return ViewStateRepository.create(readModel::get, readModel::put);
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return cloudEvent(domainEvent.id(), "stream", 1);
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new TestEvent(cloudEvent.getId());
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return "TestEvent";
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RecordingSubscribableConfiguration {
        @Bean
        RecordingSubscribable subscribable() {
            return new RecordingSubscribable();
        }
    }

    // Replays one history event the moment it is asked to catch up.
    private static PositionOrderedReader historyOfOneEvent() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.just(cloudEvent("history", "stream", 1));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(1L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    // One projection subscribes to each feed.
    @Configuration(proxyBeanMethods = false)
    static class TwoDomainFeedsConfiguration {
        @Bean
        DomainEventFeed<TestEvent> feedA(CloudEventConverter<TestEvent> converter) {
            return new DomainEventFeed<>(historyOfOneEvent(), converter, TestEvent::id);
        }

        @Bean
        DomainEventFeed<TestEvent> feedB(CloudEventConverter<TestEvent> converter) {
            return new DomainEventFeed<>(historyOfOneEvent(), converter, TestEvent::id);
        }
    }

    // What a push projection catches up from before it takes pushed events.
    @Configuration(proxyBeanMethods = false)
    static class PushModelConfiguration {
        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        PositionOrderedReader reader() {
            return historyOfOneEvent();
        }

        @Bean
        CheckpointStorage checkpointStorage() {
            return new CheckpointStorage() {
                @Override
                public Mono<Checkpoint> read(String subscriptionId) {
                    return Mono.empty();
                }

                @Override
                public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                    return Mono.just(checkpoint);
                }

                @Override
                public Mono<Long> writeVersion(String subscriptionId) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> delete(String subscriptionId) {
                    return Mono.empty();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SnapshotConfiguration {
        @Bean
        Subscriptions<TestEvent> subscriptions(RecordingSubscribable subscribable, CloudEventConverter<TestEvent> converter) {
            return new Subscriptions<>(subscribable, converter);
        }

        @Bean
        StreamSubscriptions<TestEvent> streamSubscriptions(RecordingSubscribable subscribable, CloudEventConverter<TestEvent> converter) {
            return new StreamSubscriptions<>(subscribable, converter);
        }

        @Bean
        DcbSubscriptions<TestEvent> dcbSubscriptions(RecordingSubscribable subscribable, CloudEventConverter<TestEvent> converter) {
            return new DcbSubscriptions<>(subscribable, converter);
        }

        @Bean
        DcbEventStore dcbEventStore() {
            return mock(DcbEventStore.class);
        }

        @Bean
        ReactiveSnapshotStore<Integer> reactiveSnapshotStore() {
            return ReactiveSnapshotStore.inMemory();
        }

        // Only read for a redelivery or a gap, and the single event delivered here is neither.
        @Bean
        EventStore eventStore() {
            return mock(EventStore.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyEventStoreProjectionConfiguration {
        @Lazy
        @Bean
        Marker eventStoreProjectionHolder() {
            return new EventStoreProjectionHolder();
        }
    }

    static class EventStoreProjectionHolder implements Marker {
        // The default start position replays nothing, so the default startupMode waits for it to start.
        @Projection(id = "late-event-store-projection")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection("k");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyDomainFeedCatchUpConfiguration {
        @Lazy
        @Bean
        Marker domainFeedCatchUpHolder() {
            return new DomainFeedCatchUpHolder();
        }
    }

    static class DomainFeedCatchUpHolder implements Marker {
        @Projection(id = "late-domain-feed-a", source = Source.PUSH, subscriptionModelName = "feedA")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projectionA() {
            return countProjection("a");
        }

        @Projection(id = "late-domain-feed-b", source = Source.PUSH, subscriptionModelName = "feedB")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projectionB() {
            return countProjection("b");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyDomainFeedWithoutCatchUpConfiguration {
        @Lazy
        @Bean
        Marker domainFeedWithoutCatchUpHolder() {
            return new DomainFeedWithoutCatchUpHolder();
        }
    }

    static class DomainFeedWithoutCatchUpHolder implements Marker {
        @Projection(id = "late-domain-feed-without-catch-up", source = Source.PUSH, subscriptionModelName = "feedA", catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection("a");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyPushModelProjectionConfiguration {
        @Lazy
        @Bean
        Marker pushModelProjectionHolder() {
            return new PushModelProjectionHolder();
        }
    }

    static class PushModelProjectionHolder implements Marker {
        @Projection(id = "late-push-model-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection("k");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazySnapshotConfiguration {
        @Lazy
        @Bean
        Marker snapshotHolder() {
            return new SnapshotHolder();
        }
    }

    static class SnapshotHolder implements Marker {
        // startAt = NOW because this reader-less context cannot replay, and WAIT_UNTIL_STARTED so a startup
        // registration would wait for it.
        @Snapshot(id = "late-snapshot", startAt = StartPosition.NOW, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        SnapshotView<Integer, TestEvent> snapshot() {
            return SnapshotView.<Integer, TestEvent>builder(0)
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyDcbProjectionConfiguration {
        @Lazy
        @Bean
        Marker dcbProjectionHolder() {
            return new DcbProjectionHolder();
        }
    }

    static class DcbProjectionHolder implements Marker {
        @Projection(id = "late-dcb-projection")
        DcbProjection<Integer, TestEvent, String> projection() {
            return new DcbProjection<>(countProjection("k"), DcbCriteria.all());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyStreamSnapshotConfiguration {
        @Lazy
        @Bean
        Marker streamSnapshotHolder() {
            return new StreamSnapshotHolder();
        }
    }

    static class StreamSnapshotHolder implements Marker {
        @Snapshot(id = "late-stream-snapshot", capability = Capability.STREAM, startAt = StartPosition.NOW, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        SnapshotView<Integer, TestEvent> snapshot() {
            return countSnapshot();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyDcbSnapshotConfiguration {
        @Lazy
        @Bean
        Marker dcbSnapshotHolder() {
            return new DcbSnapshotHolder();
        }
    }

    static class DcbSnapshotHolder implements Marker {
        @Snapshot(id = "late-dcb-snapshot", startAt = StartPosition.NOW, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        DcbSnapshotView<Integer, TestEvent> snapshot() {
            return new DcbSnapshotView<>(countSnapshot(), DcbCriteria.all());
        }
    }

    // The composition the reactive MongoDB starter builds, with a recording model in place of the MongoDB one. The
    // model is not a bean of its own, since a second Subscribable would make the one a projection resolves ambiguous.
    @Configuration(proxyBeanMethods = false)
    static class DurableModelConfiguration {
        @Bean
        RecordingDelegate.Holder recordingDelegate() {
            return new RecordingDelegate.Holder(new RecordingDelegate());
        }

        @Bean
        ParkingCheckpointStorage checkpointStorage() {
            return new ParkingCheckpointStorage();
        }

        @Bean(destroyMethod = "shutdown")
        ReactorDurableSubscriptionModel durableSubscriptionModel(RecordingDelegate.Holder delegate, ParkingCheckpointStorage storage) {
            return new ReactorDurableSubscriptionModel(delegate.delegate(), storage, new ReactorDurableSubscriptionModelConfig(event -> false));
        }

        @Bean
        Subscriptions<TestEvent> subscriptions(ReactorDurableSubscriptionModel model, CloudEventConverter<TestEvent> converter) {
            return new Subscriptions<>(model, converter);
        }

        @Bean
        List<String> handled() {
            return new CopyOnWriteArrayList<>();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SnapshotStoreConfiguration {
        @Bean
        ReactiveSnapshotStore<Integer> reactiveSnapshotStore() {
            return ReactiveSnapshotStore.inMemory();
        }

        // Only read for a redelivery or a gap, and the single event delivered here is neither.
        @Bean
        EventStore eventStore() {
            return mock(EventStore.class);
        }
    }

    // Stands in for the model ReactorDurableSubscriptionModel wraps. Its position read is deferred rather than a plain
    // Mono.just, so the durable model's block() on it checks the thread it runs on.
    static class RecordingDelegate implements SubscriptionModel, CheckpointAwareSubscriptionModel {
        final Map<String, Function<CloudEvent, Mono<Void>>> actions = new ConcurrentHashMap<>();
        final CountDownLatch shutDown = new CountDownLatch(1);
        final AtomicInteger refusals = new AtomicInteger();
        private final AtomicInteger subscribesToRefuse = new AtomicInteger();

        record Holder(RecordingDelegate delegate) {
        }

        void refuseNextSubscribes(int count) {
            subscribesToRefuse.set(count);
        }

        @Override
        public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            if (subscribesToRefuse.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                refusals.incrementAndGet();
                throw new IllegalStateException("Refusing " + subscriptionId);
            }
            actions.put(subscriptionId, action);
            return subscription(subscriptionId);
        }

        @Override
        public Flux<CloudEvent> subscribe(SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.defer(() -> Mono.just(GlobalCheckpoint.of(1)));
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return actions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return subscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            actions.remove(subscriptionId);
        }

        @Override
        public void shutdown() {
            actions.clear();
            shutDown.countDown();
        }

        boolean isSubscribed(String subscriptionId) {
            return actions.containsKey(subscriptionId);
        }

        void deliver(String subscriptionId, CloudEvent cloudEvent) {
            assertThat(actions).describedAs("registered subscriptions").containsKey(subscriptionId);
            actions.get(subscriptionId).apply(cloudEvent).block(Duration.ofSeconds(5));
        }

        private static Subscription subscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.defer(Mono::empty);
                }
            };
        }
    }

    // Stores nothing, so every subscription starts from the position the model reports. A test can make the read
    // wait, to hold a subscribe where it is.
    static class ParkingCheckpointStorage implements CheckpointStorage {
        final CountDownLatch parked = new CountDownLatch(1);
        private volatile CountDownLatch parkUntil;

        void parkReadsUntil(CountDownLatch release) {
            parkUntil = release;
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                CountDownLatch release = parkUntil;
                if (release != null) {
                    parked.countDown();
                    try {
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Mono.empty();
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.just(checkpoint);
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazySubscriptionConfiguration {
        @Lazy
        @Bean
        Marker subscriptionHolder(List<String> handled) {
            return new SubscriptionHolder(handled);
        }
    }

    static class SubscriptionHolder implements Marker {
        private final List<String> handled;

        SubscriptionHolder(List<String> handled) {
            this.handled = handled;
        }

        @org.occurrent.annotation.Subscription(id = "late-subscription")
        Mono<Void> on(TestEvent event) {
            handled.add(event.id());
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyDefaultStartSnapshotConfiguration {
        @Lazy
        @Bean
        Marker defaultStartSnapshotHolder() {
            return new DefaultStartSnapshotHolder();
        }
    }

    static class DefaultStartSnapshotHolder implements Marker {
        // startAt = DEFAULT is the start the durable model reads a stored position for.
        @Snapshot(id = "late-default-start-snapshot", startAt = StartPosition.DEFAULT)
        SnapshotView<Integer, TestEvent> snapshot() {
            return countSnapshot();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrototypeEventStoreProjectionConfiguration {
        @Scope("prototype")
        @Bean
        Marker prototypeProjectionHolder() {
            return new PrototypeProjectionHolder();
        }
    }

    static class PrototypeProjectionHolder implements Marker {
        @Projection(id = "late-prototype-projection")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection("k");
        }
    }

    private static SnapshotView<Integer, TestEvent> countSnapshot() {
        return SnapshotView.<Integer, TestEvent>builder(0)
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEvent dcbCloudEvent(String id) {
        return DcbCloudEvents.withTags(cloudEvent(id, "stream", 1), Set.of(Tag.of("k", "1")));
    }

    private static RecordingDelegate delegate(ApplicationContext context) {
        return context.getBean(RecordingDelegate.Holder.class).delegate();
    }

    @SuppressWarnings("unchecked")
    private static List<String> handled(ApplicationContext context) {
        return context.getBean("handled", List.class);
    }
}
