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
import org.occurrent.annotation.Catchup;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.PushCatchupStatus;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.occurrent.springboot.common.SubscriptionMode;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The reactive half of the pair invariant behind
 * <a href="https://github.com/johanhaleby/occurrent/issues/988">issue 988</a>. Every replay a registration starts is
 * either stopped by the {@code close()} that follows it, or never started because that {@code close()} has already
 * gone past.
 * <p>
 * There is no reactive saga registrar, but the reactive {@code ManualStartPushSources} defers a
 * {@code @Projection(source = PUSH)}, which reaches the same registrar code from application code on an application
 * thread. That is what lets this close the context first and register second, with nothing interleaved, so what it
 * exercises is the recheck of the closing flag rather than a race.
 * <p>
 * Container-free, because a reader that counts how often it was asked for history is all this needs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RegistrationRacingCloseTest {

    private static final String PROJECTION_ID = "closing-domain-push-projection";
    private static final String NO_CATCHUP_PROJECTION_ID = "closing-domain-push-projection-without-catchup";
    private static final String MODEL_PROJECTION_ID = "closing-model-push-projection";

    private final ApplicationContextRunner runner = runnerWith(CatchingUpProjectionConfiguration.class);
    private final ApplicationContextRunner noCatchupRunner = runnerWith(NoCatchupProjectionConfiguration.class);
    private final ApplicationContextRunner pushModelRunner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withBean(ManualStartPushSources.class, ManualStartPushSources::new)
            .withUserConfiguration(ManualPushModelProjectionConfiguration.class);

    private static ApplicationContextRunner runnerWith(Class<?> projectionConfiguration) {
        return new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withBean(ManualStartPushSources.class, ManualStartPushSources::new)
                .withUserConfiguration(ManualDomainFeedConfiguration.class, projectionConfiguration);
    }

    // Reading history is what a catch-up replay does first, so a reader nobody asked is a replay that never started.
    @Test
    void a_push_projection_started_after_the_context_closed_starts_no_replay() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            AtomicInteger historyReads = context.getBean(HistoryReads.class).count;

            ((ConfigurableApplicationContext) context).close();
            pushSources.start(PROJECTION_ID).block();

            assertThat(historyReads).describedAs("history reads after the context closed").hasValue(0);
        });
    }

    // Starting no replay was never the whole invariant. register(...) on its own puts the feed into buffering mode,
    // and a feed has no unregister, so a registration that survives a refused start leaves the feed buffering into a
    // bounded buffer that nothing will ever drain, until it overflows into the application's own publish path.
    @Test
    void a_push_projection_started_after_the_context_closed_leaves_the_feed_unregistered() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            DomainEventFeed<?> feed = context.getBean(DomainEventFeed.class);

            ((ConfigurableApplicationContext) context).close();
            pushSources.start(PROJECTION_ID).block();

            assertThat(feed.hasProjection()).describedAs("a projection registered on the feed after the context closed").isFalse();
        });
    }

    // catchup = NONE takes the branch that goes live instead of replaying, so a reader that was never asked for
    // history says nothing about it either way. The feed and the status are what say it.
    @Test
    void a_push_projection_that_does_not_catch_up_and_is_started_after_the_context_closed_neither_registers_nor_goes_live() {
        noCatchupRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            DomainEventFeed<?> feed = context.getBean(DomainEventFeed.class);
            PushCatchupStatus status = context.getBean(PushCatchupStatusImpl.class);

            ((ConfigurableApplicationContext) context).close();
            pushSources.start(NO_CATCHUP_PROJECTION_ID).block();

            assertThat(feed.hasProjection()).describedAs("a projection registered on the feed after the context closed").isFalse();
            assertThat(status.of(NO_CATCHUP_PROJECTION_ID))
                    .describedAs("the reported status of a projection that was refused")
                    .isEqualTo(new PushCatchupStatus.Unknown(NO_CATCHUP_PROJECTION_ID));
        });
    }

    // The ordinary path, so the test above cannot pass by the projection never starting at all.
    @Test
    void a_push_projection_started_while_the_context_is_open_does_replay() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            AtomicInteger historyReads = context.getBean(HistoryReads.class).count;

            pushSources.start(PROJECTION_ID).block();

            assertThat(historyReads).describedAs("history reads while the context is open").hasValue(1);
        });
    }

    // The same for catchup = NONE, which has no replay to count, so registering and reporting live is the whole of
    // what starting it does.
    @Test
    void a_push_projection_that_does_not_catch_up_and_is_started_while_the_context_is_open_registers_and_goes_live() {
        noCatchupRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            DomainEventFeed<?> feed = context.getBean(DomainEventFeed.class);
            PushCatchupStatus status = context.getBean(PushCatchupStatusImpl.class);

            pushSources.start(NO_CATCHUP_PROJECTION_ID).block();

            assertThat(feed.hasProjection()).describedAs("a projection registered on the feed").isTrue();
            assertThat(status.of(NO_CATCHUP_PROJECTION_ID))
                    .describedAs("the reported status of a projection that started")
                    .isEqualTo(new PushCatchupStatus.Live(NO_CATCHUP_PROJECTION_ID));
        });
    }

    // What startAll() answers, rather than what the registration did. The two are separate invariants: everything
    // above checks that a refused registration built and left nothing, and these check that the list does not claim
    // it started anyway. Reporting on having found an entry to remove made a closing context tell a readiness probe
    // that every push source came up.
    @Test
    void a_push_projection_started_after_the_context_closed_is_not_reported_as_started() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();

            assertThat(pushSources.startAll().block()).describedAs("the ids startAll reported as started").isEmpty();
        });
    }

    @Test
    void a_push_projection_that_does_not_catch_up_and_is_started_after_the_context_closed_is_not_reported_as_started() {
        noCatchupRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();

            assertThat(pushSources.startAll().block()).describedAs("the ids startAll reported as started").isEmpty();
        });
    }

    // The subscription-model path rather than the DomainEventFeed one, and the ordering is why it earns its own
    // fixture. The call that subscribes runs before the closing check, so this is the refusal taken with real work
    // already done, and the one a list built from what was claimed could least afford to get wrong.
    @Test
    void a_push_projection_on_a_subscription_model_started_after_the_context_closed_is_not_reported_as_started() {
        pushModelRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();

            assertThat(pushSources.startAll().block()).describedAs("the ids startAll reported as started").isEmpty();
        });
    }

    // Being left out of the list is only half of it. Under catchup = NONE the projection subscribes straight onto the
    // application's own PushSubscriptionModel, which nothing in the context shuts down, so a registration left behind
    // there goes on handling pushed events while startAll() reports the id as never started.
    @SuppressWarnings("unchecked")
    @Test
    void a_push_projection_on_a_subscription_model_started_after_the_context_closed_takes_no_live_events() {
        pushModelRunner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            ViewStateRepository<Integer, String> store = context.getBean(ViewStateRepository.class);

            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);

            ((ConfigurableApplicationContext) context).close();
            pushSources.startAll().block();
            feed.accept(cloudEvent("live")).block();

            assertThat(store.findById("k")).describedAs("state updated after close").isEmpty();
        });
    }

    // The ordinary paths, so none of the three above can pass by the projection never starting at all.
    @Test
    void a_push_projection_started_while_the_context_is_open_is_reported_as_started() {
        runner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            AtomicInteger historyReads = context.getBean(HistoryReads.class).count;

            List<String> started = pushSources.startAll().block();

            assertThat(started).describedAs("the ids startAll reported as started").containsExactly(PROJECTION_ID);
            assertThat(historyReads).describedAs("history reads while the context is open").hasValue(1);
        });
    }

    @Test
    void a_push_projection_that_does_not_catch_up_and_is_started_while_the_context_is_open_is_reported_as_started() {
        noCatchupRunner.run(context -> {
            ManualStartPushSources pushSources = context.getBean(ManualStartPushSources.class);
            DomainEventFeed<?> feed = context.getBean(DomainEventFeed.class);

            List<String> started = pushSources.startAll().block();

            assertThat(started).describedAs("the ids startAll reported as started").containsExactly(NO_CATCHUP_PROJECTION_ID);
            assertThat(feed.hasProjection()).describedAs("a projection registered on the feed").isTrue();
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void a_push_projection_on_a_subscription_model_started_while_the_context_is_open_is_reported_as_started() {
        pushModelRunner.run(context -> {
            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            ViewStateRepository<Integer, String> store = context.getBean(ViewStateRepository.class);

            List<String> started = context.getBean(ManualStartPushSources.class).startAll().block();
            feed.accept(cloudEvent("live")).block();

            assertThat(started).describedAs("the ids startAll reported as started").containsExactly(MODEL_PROJECTION_ID);
            assertThat(store.findById("k")).describedAs("the state a started projection updated from a live event").contains(1);
        });
    }

    // --- Fixtures ---

    record TestEvent(String id) {
    }

    static final class HistoryReads {
        final AtomicInteger count = new AtomicInteger();
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("TestEvent")
                .build();
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

        // A PushSubscriptionModel is itself a Subscribable, unlike a DomainEventFeed, so this is the only feed bean
        // the configuration needs. destroyMethod = "" because these tests are about a feed that outlives the context.
        // Spring would otherwise call the model's own shutdown() as an inferred destroy method, which drops every
        // registration and hides whether the registrar cancelled its own.
        @Bean(destroyMethod = "")
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
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return testConverter();
        }

        @Bean
        ModelBackedPushProjection modelBackedPushProjection() {
            return new ModelBackedPushProjection();
        }
    }

    static class ModelBackedPushProjection {
        @Projection(id = MODEL_PROJECTION_ID, source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ManualDomainFeedConfiguration {

        @Bean
        OccurrentProperties occurrentProperties() {
            OccurrentProperties properties = new OccurrentProperties();
            properties.getSubscription().setMode(SubscriptionMode.MANUAL);
            return properties;
        }

        // A DomainEventFeed is not itself a Subscribable, so without this bean afterSingletonsInstantiated's
        // early-return guard would skip annotation processing entirely.
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
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
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return testConverter();
        }

        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter, HistoryReads reads) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.defer(() -> {
                        reads.count.incrementAndGet();
                        return Flux.just(cloudEvent("history"));
                    });
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
            return new DomainEventFeed<>(reader, converter, TestEvent::id);
        }

    }

    // One projection per configuration, because one feed feeds one projection and a second DomainEventFeed bean in
    // the same context would leave the registrar with two candidates and no way to pick.
    @Configuration(proxyBeanMethods = false)
    static class CatchingUpProjectionConfiguration {
        @Bean
        ClosingPushProjection closingPushProjection() {
            return new ClosingPushProjection();
        }
    }

    static class ClosingPushProjection {
        @Projection(id = PROJECTION_ID, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
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
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    private static CloudEventConverter<TestEvent> testConverter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(TestEvent domainEvent) {
                return cloudEvent(domainEvent.id());
            }

            @Override
            public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                return new TestEvent(cloudEvent.getId());
            }

            @Override
            public String getCloudEventType(Class<? extends TestEvent> type) {
                return type.getSimpleName();
            }
        };
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> countProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }
}
