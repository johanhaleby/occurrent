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
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.PushCatchupStatus;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The readiness half of {@link PushCatchupStatus}: under {@code startupMode = BACKGROUND} the application starts while
 * the replay is still filling the read model, so an application needs to be able to tell "still catching up" from
 * "ready to serve". A failures-only view cannot, because it is empty in both cases.
 * <p>
 * Container-free: a reader parked on a latch, so the catching-up window is held open rather than raced for.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PushCatchupReadinessTest {

    // Held while the assertions about the catching-up window run, then released so the replay finishes.
    private static final CountDownLatch RELEASE_REPLAY = new CountDownLatch(1);

    @Test
    void a_background_push_catch_up_reports_catching_up_until_it_hands_over_and_live_afterwards() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ParkedPushConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    PushCatchupStatus status = context.getBean(PushCatchupStatus.class);

                    // The application has started, so BACKGROUND did its job, but the read model behind this id is
                    // still empty. isRunning(id) would say true here, which is exactly why it cannot answer this.
                    assertThat(status.of("push-background-parked")).isEqualTo(new PushCatchupStatus.CatchingUp("push-background-parked"));
                    assertThat(status.isCaughtUp("push-background-parked")).isFalse();

                    RELEASE_REPLAY.countDown();

                    // No Awaitility dependency in this module: a manual poll matches the idiom this module's other
                    // async tests already use.
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (!status.isCaughtUp("push-background-parked") && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertThat(status.of("push-background-parked")).isEqualTo(new PushCatchupStatus.Live("push-background-parked"));
                });
    }

    @Test
    void a_push_projection_that_replays_nothing_is_live_from_the_start() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(NoCatchupConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    PushCatchupStatus status = context.getBean(PushCatchupStatus.class);

                    // catchup = NONE has no history to work through, so leaving it unknown would make a readiness
                    // probe useless for the one projection that is always ready.
                    assertThat(status.of("push-no-catchup")).isEqualTo(new PushCatchupStatus.Live("push-no-catchup"));
                    assertThat(status.isCaughtUp("push-no-catchup")).isTrue();
                });
    }

    @Test
    void an_id_this_application_never_registered_is_unknown_rather_than_ready() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(NoCatchupConfiguration.class)
                .run(context -> {
                    PushCatchupStatus status = context.getBean(PushCatchupStatus.class);

                    assertThat(status.of("misspelled")).isEqualTo(new PushCatchupStatus.Unknown("misspelled"));
                    assertThat(status.isCaughtUp("misspelled")).isFalse();
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ParkedPushConfiguration extends PushConfigurationSupport {

        // Parks the replay rather than failing it, so the catching-up window can be asserted on instead of raced for.
        @Bean
        PositionOrderedReader reader() {
            return new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    try {
                        RELEASE_REPLAY.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return Stream.of();
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
        CheckpointStorage checkpointStorage() {
            return mock(CheckpointStorage.class);
        }

        @Bean
        ParkedBackgroundProjection parkedBackgroundProjection() {
            return new ParkedBackgroundProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class NoCatchupConfiguration extends PushConfigurationSupport {

        @Bean
        NoCatchupProjection noCatchupProjection() {
            return new NoCatchupProjection();
        }
    }

    // The beans both configurations need, kept in one place so each one only declares what it is actually about.
    static class PushConfigurationSupport {

        @Bean
        PushCatchupStatus pushCatchupStatus() {
            return new PushCatchupStatus();
        }

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return CloudEventBuilder.v1().withId(domainEvent.id()).withSource(URI.create("urn:test")).withType("TestEvent").build();
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

    static class ParkedBackgroundProjection {
        @Projection(id = "push-background-parked", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    static class NoCatchupProjection {
        @Projection(id = "push-no-catchup", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    record TestEvent(String id) {
    }
}
