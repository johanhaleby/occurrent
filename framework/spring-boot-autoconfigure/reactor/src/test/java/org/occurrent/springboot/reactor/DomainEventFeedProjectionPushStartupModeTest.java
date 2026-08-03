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
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The reactor twin of the blocking {@code DomainEventFeedProjectionPushStartupModeTest}: a domain-push catch-up also
 * replays the whole event store on this stack, so {@code startupMode = BACKGROUND} has to keep that off the startup
 * path here too. The replay is parked on a {@link CompletableFuture} (rather than a blocking latch inside the
 * publisher) so the two outcomes are distinguishable: with {@code BACKGROUND} the context refreshes while the replay
 * is still parked, and without it the refresh waits.
 * <p>
 * Container-free, because a fake reader and the real {@code DomainEventFeed} are all this needs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
// Backstop for the case this class exists to guard against: if BACKGROUND stopped being honoured, the context
// refresh would block on the parked replay, the test body (which completes RELEASE_REPLAY) would never run, and
// without this the whole suite would hang until the CI job's own timeout instead of failing here.
@Timeout(30)
class DomainEventFeedProjectionPushStartupModeTest {

    private static final AtomicReference<CountDownLatch> REPLAY_REACHED = new AtomicReference<>(new CountDownLatch(1));
    private static final AtomicReference<CompletableFuture<Void>> RELEASE_REPLAY = new AtomicReference<>(new CompletableFuture<>());

    private ApplicationContextRunner runnerWith(Class<?> projectionConfiguration) {
        REPLAY_REACHED.set(new CountDownLatch(1));
        RELEASE_REPLAY.set(new CompletableFuture<>());
        return new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(DomainFeedConfiguration.class, projectionConfiguration);
    }

    @Test
    void a_background_domain_feed_projection_lets_the_context_refresh_while_its_replay_is_still_running() {
        runnerWith(BackgroundProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(REPLAY_REACHED.get().await(5, TimeUnit.SECONDS)).isTrue();

            RELEASE_REPLAY.get().complete(null);

            @SuppressWarnings("unchecked")
            ViewStateRepository<Integer, String> store = context.getBean(ViewStateRepository.class);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (store.findById("k").isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(store.findById("k")).contains(1);
        });
    }

    @Test
    void the_default_startup_mode_holds_the_context_refresh_until_the_history_is_folded() throws Exception {
        ApplicationContextRunner runner = runnerWith(DefaultProjectionConfiguration.class);
        AtomicBoolean refreshReturned = new AtomicBoolean(false);
        CountDownLatch refreshFinished = new CountDownLatch(1);
        // The refresh has to run somewhere this thread can watch, because with the default it is supposed to block.
        Thread.ofVirtual().start(() -> {
            runner.run(context -> {
                assertThat(context).hasNotFailed();
                @SuppressWarnings("unchecked")
                ViewStateRepository<Integer, String> store = context.getBean(ViewStateRepository.class);
                assertThat(store.findById("k")).contains(1);
            });
            refreshReturned.set(true);
            refreshFinished.countDown();
        });

        // The replay is parked inside the fold, so a refresh that honours the default cannot have returned. This is
        // what fails if DEFAULT ever starts meaning BACKGROUND, and it fails every time rather than losing a race.
        assertThat(REPLAY_REACHED.get().await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(refreshReturned).isFalse();

        RELEASE_REPLAY.get().complete(null);
        assertThat(refreshFinished.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DomainFeedConfiguration {

        // A DomainEventFeed is not itself a Subscribable, unlike PushSubscriptionModel, so without this bean
        // afterSingletonsInstantiated's early-return guard would skip annotation processing entirely.
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, (id, value) -> {
                store.put(id, value);
            });
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
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

        // Parks on the way into the single replayed event, so a test can observe whether the context refreshed
        // without waiting for it. Bounded the same way the blocking twin's reader throws after a 5-second await: if
        // BACKGROUND stopped being honoured, the refresh thread would otherwise block here forever instead of the
        // test that never releases the future ever completing it.
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.just(cloudEvent("history"))
                            .delayUntil(ignored -> Mono.fromRunnable(() -> REPLAY_REACHED.get().countDown())
                                    .then(Mono.fromFuture(RELEASE_REPLAY.get()).timeout(Duration.ofSeconds(5))));
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

    @Configuration(proxyBeanMethods = false)
    static class BackgroundProjectionConfiguration {
        @Bean
        BackgroundProjection backgroundProjection() {
            return new BackgroundProjection();
        }
    }

    static class BackgroundProjection {
        @Projection(id = "domain-feed-push-background-reactive", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DefaultProjectionConfiguration {
        @Bean
        DefaultProjection defaultProjection() {
            return new DefaultProjection();
        }
    }

    static class DefaultProjection {
        @Projection(id = "domain-feed-push-default-reactive", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> countProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("TestEvent").build();
    }

    record TestEvent(String id) {
    }
}
