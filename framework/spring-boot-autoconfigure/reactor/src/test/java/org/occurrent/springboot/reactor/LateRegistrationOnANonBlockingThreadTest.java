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
import org.occurrent.annotation.Catchup;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

/**
 * A bean built after startup registers its annotations on whichever thread asks for it. Asked for from a Reactor
 * parallel thread, a WebFlux handler or a {@code Schedulers.parallel()} task for example, the registration must not
 * call {@code block()}, which throws there, so the bean resolves and its projection or snapshot receives events.
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
            context.getBean(RecordingSubscribable.class).deliver("late-event-store-projection", cloudEvent("1", "stream", 1));

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
            context.getBean(RecordingSubscribable.class).deliver("late-snapshot", cloudEvent("1", "stream", 1));

            @SuppressWarnings("unchecked")
            ReactiveSnapshotStore<Integer> store = context.getBean(ReactiveSnapshotStore.class);
            assertThat(store.findLatest("stream").map(org.occurrent.dsl.snapshot.Snapshot::state).block(Duration.ofSeconds(5))).isEqualTo(1);
        });
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
    static class RecordingSubscribable implements Subscribable {
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
}
