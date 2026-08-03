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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.BackgroundCatchupFailures;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code DomainEventFeed} twin of {@code BackgroundCatchupFailureTest}: a background domain-push catch-up that
 * fails does not fail the context either, and the failure lands in {@link BackgroundCatchupFailures} the same way.
 * {@code BackgroundCatchupFailureTest} only exercises the {@code PushSubscriptionModel} flavour, and the domain-feed
 * background path is separate code in {@code ProjectionAnnotationRegistrar} ({@code runInBackground}, reached only
 * through {@code catchUpCollectedFeeds()}), so a regression there would otherwise be uncovered.
 * <p>
 * Container-free: a fake reader that throws, and the real {@code DomainEventFeed}. Unlike the reactive stack, the
 * blocking annotation post-processor scans every bean definition directly in {@code afterSingletonsInstantiated}
 * rather than gating on a {@code Subscribable} bean, so none is needed here.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedBackgroundCatchupFailureTest {

    @Test
    void a_background_domain_feed_catch_up_that_fails_does_not_fail_the_context_and_the_failure_lands_in_background_catchup_failures() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(FailingDomainFeedConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    BackgroundCatchupFailures failures = context.getBean(BackgroundCatchupFailures.class);
                    // No Awaitility dependency in this module: a manual poll matches the idiom this module's other
                    // async tests (ProjectionPushStartupModeTest) already use.
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (failures.failureFor("domain-feed-push-background-failing").isEmpty() && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertThat(failures.failureFor("domain-feed-push-background-failing")).isPresent();
                    assertThat(failures.failureFor("domain-feed-push-background-failing").orElseThrow())
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("replay boom");
                    assertThat(failures.isEmpty()).isFalse();
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FailingDomainFeedConfiguration {

        @Bean
        BackgroundCatchupFailures backgroundCatchupFailures() {
            return new BackgroundCatchupFailures();
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

        // Fails the replay outright, rather than parking it: this test is about where the failure ends up, not
        // about timing.
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    throw new RuntimeException("replay boom");
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

        @Bean
        FailingBackgroundProjection failingBackgroundProjection() {
            return new FailingBackgroundProjection();
        }
    }

    static class FailingBackgroundProjection {
        @Projection(id = "domain-feed-push-background-failing", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
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
