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
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code DomainEventFeed} twin of {@code ProjectionPushStartupModeTest}: a domain-push catch-up also replays the
 * whole event store, so {@code startupMode = BACKGROUND} has to keep that off the startup path here too, not only for
 * a {@code PushSubscriptionModel} feed. The replay is parked so the two outcomes are distinguishable: with
 * {@code BACKGROUND} the context refreshes while the replay is still parked, and without it the refresh waits.
 * <p>
 * Container-free, because a fake reader and the real {@code DomainEventFeed} are all this needs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedProjectionPushStartupModeTest {

    // Held by the feed bean so the test can park a replay and release it. Static because the Spring context builds
    // the beans, and reset per test.
    private static final CountDownLatch[] REPLAY_REACHED = {new CountDownLatch(1)};
    private static final CountDownLatch[] RELEASE_REPLAY = {new CountDownLatch(1)};

    private ApplicationContextRunner runnerWith(Class<?> projectionConfiguration) {
        REPLAY_REACHED[0] = new CountDownLatch(1);
        RELEASE_REPLAY[0] = new CountDownLatch(1);
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(DomainFeedConfiguration.class, projectionConfiguration);
    }

    // A pull feed drives its own replay, so nothing registers it for catch-up boundaries, and before this it was
    // left out of the poll along with them. The clear its replay owes can still fail, and a feed that then goes
    // quiet has nothing else to retry it, so a membership the rebuild discarded would survive.
    @Test
    void a_recording_domain_feed_projection_that_catches_up_is_registered_with_the_clear_poll() {
        FailingOnceClearStore store = new FailingOnceClearStore();
        AppendId beforeTheRebuild = AppendId.mint();
        store.recordApplied("domain-feed-push-recording", beforeTheRebuild);

        runnerWith(RecordingProjectionConfiguration.class)
                .withBean(AppliedAppendStore.class, () -> store)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(REPLAY_REACHED[0].await(5, TimeUnit.SECONDS)).isTrue();
                    RELEASE_REPLAY[0].countDown();

                    // Every clear the replay itself runs fails, both the one a history delivery attempts and the one
                    // replayCompleted retries, so by the time the store is allowed to succeed the replay is over and
                    // this feed has nothing left to deliver. Only a poll tick can clear it from here.
                    long replayDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (store.clearAttempts() < 2 && System.nanoTime() < replayDeadline) {
                        Thread.sleep(20);
                    }
                    assertThat(store.clearAttempts())
                            .as("the replay ran its own clear attempts and they failed")
                            .isGreaterThanOrEqualTo(2);
                    store.allowClear();

                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (store.hasApplied("domain-feed-push-recording", beforeTheRebuild) && System.nanoTime() < deadline) {
                        Thread.sleep(20);
                    }

                    assertThat(store.hasApplied("domain-feed-push-recording", beforeTheRebuild))
                            .as("a poll tick retried the clear the feed replay left failing")
                            .isFalse();
                });
    }

    // Fails every clear until the test allows one, the way a store that is unavailable for a while does, so what
    // eventually clears is whatever the projection was registered with rather than the replay itself.
    static final class FailingOnceClearStore implements AppliedAppendStore {
        private final AppliedAppendStore delegate = AppliedAppendStore.inMemory();
        private final AtomicInteger clearAttempts = new AtomicInteger();
        private final AtomicBoolean clearAllowed = new AtomicBoolean(false);

        int clearAttempts() {
            return clearAttempts.get();
        }

        void allowClear() {
            clearAllowed.set(true);
        }

        @Override
        public void recordApplied(String projectionId, AppendId appendId) {
            delegate.recordApplied(projectionId, appendId);
        }

        @Override
        public boolean hasApplied(String projectionId, AppendId appendId) {
            return delegate.hasApplied(projectionId, appendId);
        }

        @Override
        public void clear(String projectionId) {
            clearAttempts.incrementAndGet();
            if (!clearAllowed.get()) {
                throw new RuntimeException("the store is unavailable");
            }
            delegate.clear(projectionId);
        }
    }

    @Test
    void a_background_domain_feed_projection_lets_the_context_refresh_while_its_replay_is_still_running() {
        runnerWith(BackgroundProjectionConfiguration.class).run(context -> {
            // Refreshed with the replay still parked, which is the whole feature: without it this line is not reached
            // until the replay finishes.
            assertThat(context).hasNotFailed();
            assertThat(REPLAY_REACHED[0].await(5, TimeUnit.SECONDS)).isTrue();

            RELEASE_REPLAY[0].countDown();

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
        assertThat(REPLAY_REACHED[0].await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(refreshReturned).isFalse();

        RELEASE_REPLAY[0].countDown();
        assertThat(refreshFinished.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DomainFeedConfiguration {

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
        // without waiting for it.
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.of(cloudEvent("history")).peek(ignored -> {
                        REPLAY_REACHED[0].countDown();
                        try {
                            if (!RELEASE_REPLAY[0].await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Timed out waiting to be released");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                    });
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
        @Projection(id = "domain-feed-push-background", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RecordingProjectionConfiguration {
        @Bean
        RecordingProjection recordingProjection() {
            return new RecordingProjection();
        }
    }

    static class RecordingProjection {
        @Projection(id = "domain-feed-push-recording", source = Source.PUSH, startupMode = StartupMode.BACKGROUND,
                recordAppliedAppends = true)
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
        @Projection(id = "domain-feed-push-default", source = Source.PUSH)
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
