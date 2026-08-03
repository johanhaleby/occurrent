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
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * {@code @Projection(source = PUSH, startupMode = BACKGROUND)}: the catch-up replay stops holding up startup.
 * <p>
 * A push catch-up replays the whole event store, so an application with a large history had no way to start until it
 * finished. The replay is parked here so the two outcomes are distinguishable: with {@code BACKGROUND} the context
 * refreshes while the replay is still parked, and without it the refresh waits.
 * <p>
 * Container-free, because a fake reader and the real push model are all this needs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionPushStartupModeTest {

    // Held by the reader bean so the test can park a replay and release it. Static because the Spring context builds
    // the beans, and reset per test.
    private static final CountDownLatch[] REPLAY_REACHED = {new CountDownLatch(1)};
    private static final CountDownLatch[] RELEASE_REPLAY = {new CountDownLatch(1)};

    private ApplicationContextRunner runnerWith(Class<?> projectionConfiguration) {
        REPLAY_REACHED[0] = new CountDownLatch(1);
        RELEASE_REPLAY[0] = new CountDownLatch(1);
        return new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(PushConfiguration.class, projectionConfiguration);
    }

    @Test
    void a_background_push_projection_lets_the_context_refresh_while_its_replay_is_still_running() {
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
    static class PushConfiguration {

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        CheckpointStorage checkpointStorage() {
            return mock(CheckpointStorage.class);
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, (id, value) -> {
                store.put(id, value);
            });
        }

        // Parks on the way into the single replayed event, so a test can observe whether the context refreshed
        // without waiting for it.
        @Bean
        PositionOrderedReader reader() {
            return new PositionOrderedReader() {
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
    }

    @Configuration(proxyBeanMethods = false)
    static class BackgroundProjectionConfiguration {
        @Bean
        BackgroundProjection backgroundProjection() {
            return new BackgroundProjection();
        }
    }

    static class BackgroundProjection {
        @Projection(id = "push-background", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
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
        @Projection(id = "push-default", source = Source.PUSH)
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
