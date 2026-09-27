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
import org.occurrent.annotation.DcbSubscription;
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
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.FluxSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelConfig;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A bean built after startup registers its annotations on whichever thread asks for it. Asked for from a Reactor
 * parallel thread, a WebFlux handler or a {@code Schedulers.parallel()} task for example, the registration must not
 * call {@code block()}, which throws there, so the bean resolves and its projection, snapshot or subscription receives
 * events. {@code ReactorDurableSubscriptionModel} calls {@code block()} inside {@code subscribe} itself, so there a
 * registration that starts at the beginning subscribes on another thread, and one with a {@code DEFAULT} start fails
 * the bean with a message saying how to register it.
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

    // ReactorDurableSubscriptionModel, the model the reactive MongoDB starter registers, calls block() inside subscribe.
    // A start that does not depend on when the subscribe runs lets the subscribe run on another thread after the bean
    // is returned.
    @Test
    void a_lazy_projection_starting_at_the_beginning_on_the_durable_model_resolved_on_a_parallel_thread_folds_the_events_it_is_delivered() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyBeginningProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "beginningProjectionHolder")).isInstanceOf(BeginningProjectionHolder.class);
            RecordingDelegate delegate = delegate(context);
            awaitUntil(() -> delegate.isSubscribed("late-beginning-projection"));
            delegate.deliver("late-beginning-projection", cloudEvent("1", "stream", 1));

            assertThat(readModel(context).get("k")).isEqualTo(1);
        });
    }

    @Test
    void a_lazy_subscription_starting_at_the_beginning_on_the_durable_model_resolved_on_a_parallel_thread_handles_the_events_it_is_delivered() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazySubscriptionConfiguration.class).run(context -> {
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
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, SnapshotStoreConfiguration.class, LazyBeginningSnapshotConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(resolvedOnAParallelThread(context, "beginningSnapshotHolder")).isInstanceOf(BeginningSnapshotHolder.class);
            RecordingDelegate delegate = delegate(context);
            awaitUntil(() -> delegate.isSubscribed("late-beginning-snapshot"));
            delegate.deliver("late-beginning-snapshot", cloudEvent("1", "stream", 1));

            @SuppressWarnings("unchecked")
            ReactiveSnapshotStore<Integer> store = context.getBean(ReactiveSnapshotStore.class);
            assertThat(store.findLatest("stream").map(org.occurrent.dsl.snapshot.Snapshot::state).block(Duration.ofSeconds(5))).isEqualTo(1);
        });
    }

    // A DEFAULT start is wherever the feed has reached when the subscribe runs. Subscribing on another thread after the
    // bean is returned would skip what the caller writes in between, so the bean fails, and built where blocking is
    // allowed it registers.
    @Test
    void a_lazy_projection_with_a_default_start_on_the_durable_model_is_refused_on_a_parallel_thread_and_registers_when_built_off_it() {
        runner.withUserConfiguration(DurableModelConfiguration.class, LazyEventStoreProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);

            assertThat(failureResolvingOnAParallelThread(context, "eventStoreProjectionHolder"))
                    .hasStackTraceContaining("@Projection 'late-event-store-projection' may start from wherever the event feed has reached when it subscribes")
                    .hasStackTraceContaining("Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())");
            assertThat(delegate.isSubscribed("late-event-store-projection")).isFalse();

            context.getBean("eventStoreProjectionHolder");

            assertThat(delegate.isSubscribed("late-event-store-projection")).isTrue();
        });
    }

    // Also shows the handler and id the refused bean claimed are given back, since building it again registers.
    @Test
    void a_lazy_subscription_with_a_default_start_on_the_durable_model_is_refused_on_a_parallel_thread_and_registers_when_built_off_it() {
        runner.withUserConfiguration(DurableModelConfiguration.class, LazyDefaultStartSubscriptionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);

            assertThat(failureResolvingOnAParallelThread(context, "defaultStartSubscriptionHolder"))
                    .hasStackTraceContaining("the handler 'late-default-start-subscription' on " + DefaultStartSubscriptionHolder.class.getName() + "#on may start from wherever the event feed has reached");
            assertThat(delegate.isSubscribed("late-default-start-subscription")).isFalse();

            context.getBean("defaultStartSubscriptionHolder");

            assertThat(delegate.isSubscribed("late-default-start-subscription")).isTrue();
        });
    }

    // Where each handler starts is decided per handler, so a handler starting at the beginning is not refused for
    // sharing its bean with one that starts now.
    @Test
    void a_lazy_bean_with_a_beginning_and_a_now_subscription_on_the_durable_model_resolved_on_a_parallel_thread_subscribes_both() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyStartPositionsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);

            assertThat(resolvedOnAParallelThread(context, "beginningAndNowHolder")).isInstanceOf(BeginningAndNowHolder.class);

            assertThat(delegate.isSubscribed("late-mixed-now")).describedAs("the NOW handler subscribed before the bean was returned").isTrue();
            awaitUntil(() -> delegate.isSubscribed("late-mixed-beginning"));
        });
    }

    // The DEFAULT handler subscribes on the calling thread before the BEGINNING handler is handed to the scheduler,
    // so when it is refused, the BEGINNING handler has not subscribed and what both claimed is given back.
    @Test
    void a_lazy_bean_with_a_beginning_and_a_default_subscription_on_the_durable_model_is_refused_for_its_default_handler_and_registers_both_when_built_off_it() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyStartPositionsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);

            assertThat(failureResolvingOnAParallelThread(context, "beginningAndDefaultHolder"))
                    .hasStackTraceContaining("the handler 'late-mixed-default' on " + BeginningAndDefaultHolder.class.getName() + "#fromWhereItStopped may start from wherever the event feed has reached");
            assertThat(delegate.isSubscribed("late-mixed-default")).isFalse();
            assertThat(delegate.isSubscribed("late-mixed-default-beginning")).isFalse();

            context.getBean("beginningAndDefaultHolder");

            assertThat(delegate.isSubscribed("late-mixed-default")).isTrue();
            assertThat(delegate.isSubscribed("late-mixed-default-beginning")).isTrue();
        });
    }

    // BEGINNING replays only the first time. A position stored by an earlier run is further on, and the subscription
    // resumes from there.
    @Test
    void a_lazy_subscription_starting_at_the_beginning_with_a_stored_position_on_the_durable_model_resumes_from_the_stored_position() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazySubscriptionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            context.getBean(ParkingCheckpointStorage.class).stored.put("late-subscription", GlobalCheckpoint.of(5));
            RecordingDelegate delegate = delegate(context);

            resolvedOnAParallelThread(context, "subscriptionHolder");
            awaitUntil(() -> delegate.isSubscribed("late-subscription"));

            assertThat(delegate.startedAt.get("late-subscription")).hasToString(GlobalCheckpoint.of(5).asString());
        });
    }

    @Test
    void a_lazy_subscription_starting_at_a_global_position_on_the_durable_model_resolved_on_a_parallel_thread_starts_there() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyStartPositionsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);

            resolvedOnAParallelThread(context, "globalPositionHolder");
            awaitUntil(() -> delegate.isSubscribed("late-global-position"));

            assertThat(delegate.startedAt.get("late-global-position")).hasToString(GlobalCheckpoint.of(3).asString());
        });
    }

    @Test
    void a_lazy_dcb_subscription_starting_at_the_beginning_on_the_durable_model_resolved_on_a_parallel_thread_handles_the_events_it_is_delivered() {
        runner.withUserConfiguration(DurableModelConfiguration.class, DurableDcbSubscriptionsConfiguration.class, LazyStartPositionsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);

            resolvedOnAParallelThread(context, "dcbSubscriptionHolder");
            awaitUntil(() -> delegate.isSubscribed("late-dcb-subscription"));
            delegate.deliver("late-dcb-subscription", dcbCloudEvent("1"));

            assertThat(handled(context)).containsExactly("1");
        });
    }

    // NOW needs no stored position, so the durable model does not block and the subscribe runs on the calling thread.
    @Test
    void a_lazy_subscription_starting_now_on_the_durable_model_resolved_on_a_parallel_thread_subscribes_before_the_bean_is_returned() {
        runner.withUserConfiguration(DurableModelConfiguration.class, LazyStartPositionsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);

            resolvedOnAParallelThread(context, "nowHolder");

            assertThat(delegate.isSubscribed("late-now")).isTrue();
            assertThat(delegate.startedAt.get("late-now").isNow()).isTrue();
        });
    }

    // A refusal that says the subscribe itself is wrong fails the same way however often it is tried.
    @Test
    void a_late_subscribe_the_subscription_model_refuses_as_a_duplicate_is_not_tried_again() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyBeginningProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);
            delegate.refuseNextSubscribes(5, DuplicateSubscriptionIdException::new);

            resolvedOnAParallelThread(context, "beginningProjectionHolder");
            awaitUntil(() -> delegate.refusals.get() >= 1);
            // The first retry would come after 100 ms
            Thread.sleep(500);

            assertThat(delegate.refusals).hasValue(1);
            assertThat(delegate.isSubscribed("late-beginning-projection")).isFalse();
        });
    }

    // DcbSubscriptions is looked up while the bean is built, so a context without it fails the caller rather than a
    // subscribe on another thread that has no caller to fail.
    @Test
    void a_lazy_dcb_subscription_in_a_context_without_dcb_subscriptions_fails_its_bean_on_a_parallel_thread() {
        runner.withUserConfiguration(DurableModelConfiguration.class, LazyStartPositionsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(failureResolvingOnAParallelThread(context, "dcbSubscriptionHolder"))
                    .hasRootCauseInstanceOf(NoSuchBeanDefinitionException.class)
                    .rootCause().hasMessageContaining(DcbSubscriptions.class.getName());
        });
    }

    // A model the application shut down itself, with the context still open, cannot be started again
    @Test
    void a_late_subscribe_on_a_subscription_model_that_is_shut_down_is_not_tried_again() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyBeginningProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);
            delegate.refuseNextSubscribes(5, id -> new SubscriptionModelShutdownException());

            resolvedOnAParallelThread(context, "beginningProjectionHolder");
            awaitUntil(() -> delegate.refusals.get() >= 1);
            // The first retry would come after 100 ms
            Thread.sleep(500);

            assertThat(delegate.refusals).hasValue(1);
            assertThat(delegate.isSubscribed("late-beginning-projection")).isFalse();
        });
    }

    // Two handlers with a fixed start subscribe one after the other, so the second waits for the first, and a give-up
    // on the first gives back both
    @Test
    void a_late_subscribe_that_gives_up_names_the_handlers_after_the_failing_one_that_it_gives_back() {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(LateSubscriber.class);
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender = new ch.qos.logback.core.read.ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyTwoBeginningSubscriptionsConfiguration.class).run(context -> {
                assertThat(context).hasNotFailed();
                RecordingDelegate delegate = delegate(context);
                delegate.refuseNextSubscribes(5, id -> new SubscriptionModelShutdownException());

                resolvedOnAParallelThread(context, "twoBeginningSubscriptionsHolder");
                awaitUntil(() -> appender.list.stream().anyMatch(event -> event.getFormattedMessage().startsWith("Gave up subscribing")));

                assertThat(appender.list).filteredOn(event -> event.getFormattedMessage().startsWith("Gave up subscribing")).singleElement()
                        .satisfies(event -> assertThat(event.getFormattedMessage())
                                .containsPattern("the handler 'late-(first|second)-beginning' on .*, and after it the handler 'late-(first|second)-beginning' on the same bean")
                                .contains("late-first-beginning", "late-second-beginning", "restart the application"));
                assertThat(delegate.refusals).hasValue(1);
            });
        } finally {
            logger.detachAppender(appender);
        }
    }

    // A refresh that fails after the startup scan destroys the post processor without a ContextClosedEvent
    @Test
    void destroying_the_post_processor_refuses_a_late_subscribe_as_closing_the_context_does() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyBeginningProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            context.getBean(OccurrentReactiveAnnotationBeanPostProcessor.class).destroy();

            assertThat(failureResolvingOnAParallelThread(context, "beginningProjectionHolder"))
                    .hasStackTraceContaining("Cannot subscribe @Projection 'late-beginning-projection', since the application context is closing.");
        });
    }

    // A subscribe on another thread has no caller to fail, so one the model refuses is tried again until it is
    // accepted. Its start does not depend on when it subscribes, so the retries skip nothing.
    @Test
    void a_late_subscribe_the_subscription_model_refuses_is_tried_again_until_it_is_accepted() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyBeginningProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            RecordingDelegate delegate = delegate(context);
            delegate.refuseNextSubscribes(2);

            resolvedOnAParallelThread(context, "beginningProjectionHolder");
            awaitUntil(() -> delegate.isSubscribed("late-beginning-projection"));

            assertThat(delegate.refusals).hasValue(2);
            assertThat(delegate.isSubscribed("late-beginning-projection")).describedAs("subscribed after two refusals").isTrue();
        });
    }

    // The subscribe is parked reading the stored position when the context starts closing. The model shuts down
    // while beans are destroyed, so a close that went ahead without waiting would have the subscribe register on a
    // model that had already shut down, and nothing would ever stop that subscription.
    @Test
    void closing_the_context_waits_for_a_late_subscribe_so_the_subscription_model_stops_it() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyBeginningProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            ParkingCheckpointStorage storage = context.getBean(ParkingCheckpointStorage.class);
            CountDownLatch release = new CountDownLatch(1);
            storage.parkReadsUntil(release);
            RecordingDelegate delegate = delegate(context);

            resolvedOnAParallelThread(context, "beginningProjectionHolder");
            assertThat(storage.parked.await(5, TimeUnit.SECONDS)).describedAs("the subscribe reached the position read").isTrue();

            Thread closing = Thread.ofVirtual().start(context::close);
            assertThat(delegate.shutDown.await(500, TimeUnit.MILLISECONDS)).describedAs("the model shut down while the subscribe was still running").isFalse();

            release.countDown();
            assertThat(closing.join(Duration.ofSeconds(10))).describedAs("the context finished closing").isTrue();
            assertThat(delegate.shutDown.getCount()).describedAs("the model shut down once the subscribe finished").isZero();
            assertThat(delegate.actions).describedAs("subscriptions still registered after the context closed").isEmpty();
        });
    }

    // A child context's close reaches the parent's listeners too, and must not stop the parent's late subscribes.
    @Test
    void a_child_context_closing_leaves_late_subscribes_in_its_parent_working() {
        runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, LazyBeginningProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            AnnotationConfigApplicationContext child = new AnnotationConfigApplicationContext();
            child.setParent(context);
            child.refresh();
            child.close();

            assertThat(resolvedOnAParallelThread(context, "beginningProjectionHolder")).isInstanceOf(BeginningProjectionHolder.class);
            RecordingDelegate delegate = delegate(context);
            awaitUntil(() -> delegate.isSubscribed("late-beginning-projection"));
            assertThat(delegate.isSubscribed("late-beginning-projection")).isTrue();
        });
    }

    // Registration at startup subscribes in place whatever thread refreshes the context, so a subscribe that cannot
    // run there fails the refresh rather than only being logged.
    @Test
    void a_context_refreshed_on_a_parallel_thread_fails_when_a_startup_subscribe_cannot_run_there() {
        AtomicReference<Throwable> startupFailure = new AtomicReference<>();
        Mono.fromRunnable(() -> runner.withUserConfiguration(DurableModelConfiguration.class, PositionWritingEventStoreConfiguration.class, EagerBeginningProjectionConfiguration.class)
                        .run(context -> startupFailure.set(context.getStartupFailure())))
                .subscribeOn(Schedulers.parallel())
                .block(Duration.ofSeconds(10));

        assertThat(startupFailure.get()).describedAs("the startup failure").isNotNull()
                .hasStackTraceContaining("blocking, which is not supported in thread parallel-");
    }

    // Nothing waits for a subscribe moved off a non-blocking thread, so one the scheduler has not run yet when the
    // context closes must not run at all.
    @Test
    void a_late_subscribe_that_has_not_run_when_the_context_closes_never_runs() {
        List<Runnable> queued = new CopyOnWriteArrayList<>();
        LateSubscriber subscriber = new LateSubscriber(Schedulers.fromExecutor(queued::add), Duration.ofSeconds(5), Duration.ofMillis(100));
        AtomicBoolean subscribed = new AtomicBoolean(false);
        onAParallelThread(() -> subscriber.call(() -> {
        }).subscribe(() -> "a test registration", true, () -> subscribed.set(true)));
        assertThat(queued).hasSize(1);

        subscriber.close();
        queued.forEach(Runnable::run);

        assertThat(subscribed).isFalse();
    }

    // A subscribe still being tried again keeps what it claimed. Once the context closes it stops and gives it back.
    @Test
    void a_late_subscribe_still_failing_when_the_context_closes_gives_back_what_it_claimed() throws InterruptedException {
        LateSubscriber subscriber = new LateSubscriber(Schedulers.boundedElastic(), Duration.ofSeconds(5), Duration.ofMillis(10));
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch released = new CountDownLatch(1);
        onAParallelThread(() -> subscriber.call(released::countDown).subscribe(() -> "a test registration", true, () -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("refused");
        }));
        awaitUntil(() -> attempts.get() >= 2);
        assertThat(released.getCount()).describedAs("released while it was still being tried").isOne();

        subscriber.close();

        assertThat(released.await(5, TimeUnit.SECONDS)).describedAs("released once the context closed").isTrue();
    }

    // A subscribe stuck reading its position must not hold the shutdown open for longer than the close timeout.
    @Test
    void closing_stops_waiting_for_a_late_subscribe_that_outlasts_the_close_timeout() throws InterruptedException {
        LateSubscriber subscriber = new LateSubscriber(Schedulers.boundedElastic(), Duration.ofMillis(200), Duration.ofMillis(100));
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        onAParallelThread(() -> subscriber.call(() -> {
        }).subscribe(() -> "a test registration", true, () -> {
            running.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        assertThat(running.await(5, TimeUnit.SECONDS)).describedAs("the subscribe is running").isTrue();
        try {
            Thread closing = Thread.ofVirtual().start(subscriber::close);

            assertThat(closing.join(Duration.ofSeconds(2))).describedAs("close() returned while the subscribe was still running").isTrue();
        } finally {
            release.countDown();
        }
    }

    // A bean first built once the context has started closing, during a graceful shutdown for example, fails rather
    // than being returned with a subscription that never starts.
    @Test
    void a_late_subscribe_asked_for_after_the_context_started_closing_is_refused() {
        LateSubscriber subscriber = new LateSubscriber(Schedulers.boundedElastic(), Duration.ofSeconds(5), Duration.ofMillis(100));
        subscriber.close();

        assertThatThrownBy(() -> onAParallelThread(() -> subscriber.call(() -> {
        }).subscribe(() -> "a test registration", true, () -> {
        })))
                .hasMessage("Cannot subscribe a test registration, since the application context is closing.");
    }

    @Test
    void a_late_subscribe_that_fails_with_an_error_is_not_tried_again_and_gives_back_what_it_claimed() throws InterruptedException {
        LateSubscriber subscriber = new LateSubscriber(Schedulers.boundedElastic(), Duration.ofSeconds(5), Duration.ofMillis(10));
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch released = new CountDownLatch(1);
        onAParallelThread(() -> subscriber.call(released::countDown).subscribe(() -> "a test registration", true, () -> {
            attempts.incrementAndGet();
            throw new AssertionError("broken");
        }));

        assertThat(released.await(5, TimeUnit.SECONDS)).describedAs("released after the error").isTrue();
        Thread.sleep(200);
        assertThat(attempts).hasValue(1);
    }

    // A retry waiting out its delay is cancelled by close() and gives back what it claimed then, rather than holding
    // it until the delay runs out. close() waits until the retry is scheduled, since one closing while the first
    // attempt still runs is given up by that attempt instead, and would pass without close() cancelling anything.
    @Test
    void closing_cancels_a_late_subscribe_waiting_to_be_tried_again() throws InterruptedException {
        Scheduler elastic = Schedulers.boundedElastic();
        CountDownLatch retryScheduled = new CountDownLatch(1);
        Scheduler recordingRetries = new Scheduler() {
            @Override
            public Disposable schedule(Runnable task) {
                return elastic.schedule(task);
            }

            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                Disposable scheduled = elastic.schedule(task, delay, unit);
                retryScheduled.countDown();
                return scheduled;
            }

            @Override
            public Worker createWorker() {
                return elastic.createWorker();
            }
        };
        LateSubscriber subscriber = new LateSubscriber(recordingRetries, Duration.ofSeconds(5), Duration.ofSeconds(20));
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch released = new CountDownLatch(1);
        onAParallelThread(() -> subscriber.call(released::countDown).subscribe(() -> "a test registration", true, () -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("refused");
        }));
        assertThat(retryScheduled.await(5, TimeUnit.SECONDS)).describedAs("the retry is scheduled").isTrue();

        subscriber.close();

        assertThat(released.getCount()).describedAs("released by the time close() returned").isZero();
        assertThat(attempts).hasValue(1);
    }

    // What the classification rests on. SubscriptionRefusedException documents an IllegalArgumentException as a call
    // that different arguments would have made work, and an IllegalStateException as a failure at the time.
    @Test
    void only_a_failure_that_can_go_away_is_tried_again() {
        assertThat(LateSubscriber.retriable(new IllegalStateException("storage is unreachable"))).isTrue();
        assertThat(LateSubscriber.retriable(new RuntimeException("a driver's own exception"))).isTrue();
        assertThat(LateSubscriber.retriable(new DuplicateSubscriptionIdException("id"))).isFalse();
        assertThat(LateSubscriber.retriable(new IllegalArgumentException("refused"))).isFalse();
        assertThat(LateSubscriber.retriable(new UnsupportedOperationException("cannot serve it"))).isFalse();
        assertThat(LateSubscriber.retriable(new NullPointerException("id cannot be null"))).isFalse();
        assertThat(LateSubscriber.retriable(new NoSuchBeanDefinitionException(CheckpointStorage.class))).describedAs("a bean the context does not have").isFalse();
        assertThat(LateSubscriber.retriable(new BeanNotOfRequiredTypeException("checkpointStorage", CheckpointStorage.class, String.class)))
                .describedAs("a bean of another type").isFalse();
        assertThat(LateSubscriber.retriable(new BeanCreationException("checkpointStorage", "cannot be built", new NoSuchBeanDefinitionException(String.class))))
                .describedAs("a bean that cannot be built because one it depends on is missing").isFalse();
        assertThat(LateSubscriber.retriable(new BeanCreationException("checkpointStorage", "cannot be built", new IllegalStateException("storage is unreachable"))))
                .describedAs("a bean whose factory failed, which the next attempt builds again").isTrue();
        assertThat(LateSubscriber.retriable(new SubscriptionModelShutdownException())).describedAs("a model that was shut down").isFalse();
        assertThat(LateSubscriber.retriable(new AssertionError("broken"))).isFalse();
        assertThat(LateSubscriber.retriable(reactor.core.Exceptions.propagate(new AssertionError("broken")))).describedAs("an Error block() rethrew").isFalse();
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

    private static Throwable failureResolvingOnAParallelThread(ConfigurableApplicationContext context, String beanName) {
        assertThat(context.getBeanFactory().containsSingleton(beanName)).describedAs("%s is still unbuilt after startup", beanName).isFalse();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Mono.fromCallable(() -> context.getBean(beanName))
                .subscribeOn(Schedulers.parallel())
                .onErrorResume(e -> {
                    failure.set(e);
                    return Mono.empty();
                })
                .block(Duration.ofSeconds(10));
        assertThat(failure.get()).describedAs("the failure resolving %s on a Reactor parallel thread", beanName).isNotNull();
        return failure.get();
    }

    private static void onAParallelThread(Runnable runnable) {
        Mono.fromRunnable(runnable).subscribeOn(Schedulers.parallel()).block(Duration.ofSeconds(5));
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("The condition did not hold within 5 seconds");
            }
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
    }

    // An event store that writes a position is what lets a registration start at BEGINNING. Nothing reads from it
    // here, since the recording model replays nothing, and a snapshot only reads it for a redelivery or a gap.
    @Configuration(proxyBeanMethods = false)
    static class PositionWritingEventStoreConfiguration {
        @Bean
        EventStore eventStore() {
            EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
            when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(true);
            return eventStore;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyBeginningProjectionConfiguration {
        @Lazy
        @Bean
        Marker beginningProjectionHolder() {
            return new BeginningProjectionHolder();
        }
    }

    // Built at startup, unlike every other holder here.
    @Configuration(proxyBeanMethods = false)
    static class EagerBeginningProjectionConfiguration {
        @Bean
        Marker beginningProjectionHolder() {
            return new BeginningProjectionHolder();
        }
    }

    static class BeginningProjectionHolder implements Marker {
        @Projection(id = "late-beginning-projection", startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection("k");
        }
    }

    // Stands in for the model ReactorDurableSubscriptionModel wraps. Its position read is deferred rather than a plain
    // Mono.just, so the durable model's block() on it checks the thread it runs on.
    static class RecordingDelegate implements SubscriptionModel, CheckpointAwareSubscriptionModel {
        final Map<String, Function<CloudEvent, Mono<Void>>> actions = new ConcurrentHashMap<>();
        final CountDownLatch shutDown = new CountDownLatch(1);
        final Map<String, StartAt> startedAt = new ConcurrentHashMap<>();
        final AtomicInteger refusals = new AtomicInteger();
        private final AtomicInteger subscribesToRefuse = new AtomicInteger();
        private volatile Function<String, RuntimeException> refusal = id -> new IllegalStateException("Refusing " + id);

        record Holder(RecordingDelegate delegate) {
        }

        void refuseNextSubscribes(int count) {
            subscribesToRefuse.set(count);
        }

        void refuseNextSubscribes(int count, Function<String, RuntimeException> refusal) {
            this.refusal = refusal;
            subscribesToRefuse.set(count);
        }

        @Override
        public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            if (subscribesToRefuse.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                refusals.incrementAndGet();
                throw refusal.apply(subscriptionId);
            }
            startedAt.put(subscriptionId, startAt);
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

    // Stores nothing unless a test puts a position in stored, so every other subscription starts from the position the
    // model reports. A test can make the read wait, to hold a subscribe where it is.
    static class ParkingCheckpointStorage implements CheckpointStorage {
        final CountDownLatch parked = new CountDownLatch(1);
        final Map<String, Checkpoint> stored = new ConcurrentHashMap<>();
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
                return Mono.justOrEmpty(stored.get(subscriptionId));
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

        @org.occurrent.annotation.Subscription(id = "late-subscription", startAt = StartPosition.BEGINNING)
        Mono<Void> on(TestEvent event) {
            handled.add(event.id());
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyDefaultStartSubscriptionConfiguration {
        @Lazy
        @Bean
        Marker defaultStartSubscriptionHolder() {
            return new DefaultStartSubscriptionHolder();
        }
    }

    static class DefaultStartSubscriptionHolder implements Marker {
        @org.occurrent.annotation.Subscription(id = "late-default-start-subscription")
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DurableDcbSubscriptionsConfiguration {
        @Bean
        DcbSubscriptions<TestEvent> dcbSubscriptions(ReactorDurableSubscriptionModel model, CloudEventConverter<TestEvent> converter) {
            return new DcbSubscriptions<>(model, converter);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyStartPositionsConfiguration {
        @Lazy
        @Bean
        Marker beginningAndNowHolder() {
            return new BeginningAndNowHolder();
        }

        @Lazy
        @Bean
        Marker beginningAndDefaultHolder() {
            return new BeginningAndDefaultHolder();
        }

        @Lazy
        @Bean
        Marker globalPositionHolder() {
            return new GlobalPositionHolder();
        }

        @Lazy
        @Bean
        Marker nowHolder() {
            return new NowHolder();
        }

        @Lazy
        @Bean
        Marker dcbSubscriptionHolder(List<String> handled) {
            return new DcbSubscriptionHolder(handled);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyTwoBeginningSubscriptionsConfiguration {
        @Lazy
        @Bean
        Marker twoBeginningSubscriptionsHolder() {
            return new TwoBeginningSubscriptionsHolder();
        }
    }

    static class TwoBeginningSubscriptionsHolder implements Marker {
        @org.occurrent.annotation.Subscription(id = "late-first-beginning", startAt = StartPosition.BEGINNING)
        Mono<Void> first(TestEvent event) {
            return Mono.empty();
        }

        @org.occurrent.annotation.Subscription(id = "late-second-beginning", startAt = StartPosition.BEGINNING)
        Mono<Void> second(TestEvent event) {
            return Mono.empty();
        }
    }

    static class BeginningAndNowHolder implements Marker {
        @org.occurrent.annotation.Subscription(id = "late-mixed-beginning", startAt = StartPosition.BEGINNING)
        Mono<Void> fromTheBeginning(TestEvent event) {
            return Mono.empty();
        }

        @org.occurrent.annotation.Subscription(id = "late-mixed-now", startAt = StartPosition.NOW)
        Mono<Void> fromNow(TestEvent event) {
            return Mono.empty();
        }
    }

    static class BeginningAndDefaultHolder implements Marker {
        @org.occurrent.annotation.Subscription(id = "late-mixed-default-beginning", startAt = StartPosition.BEGINNING)
        Mono<Void> fromTheBeginning(TestEvent event) {
            return Mono.empty();
        }

        @org.occurrent.annotation.Subscription(id = "late-mixed-default")
        Mono<Void> fromWhereItStopped(TestEvent event) {
            return Mono.empty();
        }
    }

    static class GlobalPositionHolder implements Marker {
        @org.occurrent.annotation.Subscription(id = "late-global-position", startAtGlobalPosition = 3)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    static class NowHolder implements Marker {
        @org.occurrent.annotation.Subscription(id = "late-now", startAt = StartPosition.NOW)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    static class DcbSubscriptionHolder implements Marker {
        private final List<String> handled;

        DcbSubscriptionHolder(List<String> handled) {
            this.handled = handled;
        }

        @DcbSubscription(id = "late-dcb-subscription", startAt = StartPosition.BEGINNING)
        Mono<Void> on(TestEvent event) {
            handled.add(event.id());
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LazyBeginningSnapshotConfiguration {
        @Lazy
        @Bean
        Marker beginningSnapshotHolder() {
            return new BeginningSnapshotHolder();
        }
    }

    static class BeginningSnapshotHolder implements Marker {
        // BEGINNING is the default start of a snapshot.
        @Snapshot(id = "late-beginning-snapshot")
        SnapshotView<Integer, TestEvent> snapshot() {
            return countSnapshot();
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
