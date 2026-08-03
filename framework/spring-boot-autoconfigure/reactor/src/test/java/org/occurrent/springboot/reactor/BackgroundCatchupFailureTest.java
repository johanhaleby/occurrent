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
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.BackgroundCatchupFailures;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactor twin of the blocking {@code BackgroundCatchupFailureTest}: a background push catch-up that fails does
 * not fail the context (that is the whole point of {@code BACKGROUND}), and the failure lands in
 * {@link BackgroundCatchupFailures} instead of vanishing.
 * <p>
 * Container-free: a fake reader whose replay errors, the real push model, and the real fake {@code CheckpointStorage}
 * (a mock's null default for {@code read()} breaks the reactive catch-up before the failure is ever reached, see
 * {@code ReactiveProjectionPushCatchupTunablesTest}).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class BackgroundCatchupFailureTest {

    @Test
    void a_background_push_catch_up_that_fails_does_not_fail_the_context_and_the_failure_lands_in_background_catchup_failures() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(FailingPushConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    BackgroundCatchupFailures failures = context.getBean(BackgroundCatchupFailures.class);
                    // No Awaitility dependency in this module: a manual poll matches the idiom this module's other
                    // async tests (ReactiveProjectionPushCatchupTunablesTest) already use.
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (failures.failureFor("push-background-failing-reactive").isEmpty() && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertThat(failures.failureFor("push-background-failing-reactive")).isPresent();
                    assertThat(failures.failureFor("push-background-failing-reactive").orElseThrow())
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("replay boom");
                    assertThat(failures.isEmpty()).isFalse();
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FailingPushConfiguration {

        @Bean
        BackgroundCatchupFailures backgroundCatchupFailures() {
            return new BackgroundCatchupFailures();
        }

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        CheckpointStorage checkpointStorage() {
            return new CheckpointStorage() {
                @Override
                public Mono<Checkpoint> read(String subscriptionId) {
                    return Mono.empty();
                }

                @Override
                public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                    return Mono.just(checkpoint);
                }

                @Override
                public Mono<Void> delete(String subscriptionId) {
                    return Mono.empty();
                }
            };
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }

        // Fails the replay outright, rather than parking it: this test is about where the failure ends up, not about
        // timing.
        @Bean
        PositionOrderedReader reader() {
            return new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.error(new RuntimeException("replay boom"));
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

        @Bean
        FailingBackgroundProjection failingBackgroundProjection() {
            return new FailingBackgroundProjection();
        }
    }

    static class FailingBackgroundProjection {
        @Projection(id = "push-background-failing-reactive", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("TestEvent").build();
    }

    record TestEvent(String id) {
    }
}
