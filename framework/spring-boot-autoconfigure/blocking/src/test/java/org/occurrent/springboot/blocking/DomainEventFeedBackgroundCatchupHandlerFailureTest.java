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
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.PushCatchupStatus;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The projection itself throwing, rather than the store the replay reads from, which is what
 * {@code DomainEventFeedBackgroundCatchupFailureTest} covers. Nobody joins a background catch-up except {@code close()},
 * so a failure it does not record reaches no one at all: no log, and a status that still reports the projection as
 * catching up while nothing is.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedBackgroundCatchupHandlerFailureTest {

    // Read by the projection bean the context below builds, so both tests can share one configuration.
    private static final AtomicReference<Exception> FOLD_FAILURE = new AtomicReference<>();

    // A projection written in Kotlin can throw a checked exception from its fold without declaring it anywhere.
    @Test
    void a_background_domain_feed_catch_up_a_checked_exception_from_the_fold_ended_records_the_failure() {
        assertThatTheFoldFailureIsRecorded(new IOException("the view this projection writes to is down"));
    }

    @Test
    void a_background_domain_feed_catch_up_a_runtime_exception_from_the_fold_ended_records_the_failure() {
        assertThatTheFoldFailureIsRecorded(new IllegalStateException("the view this projection writes to is down"));
    }

    private static void assertThatTheFoldFailureIsRecorded(Exception foldFailure) {
        FOLD_FAILURE.set(foldFailure);
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(FailingFoldConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    PushCatchupStatus status = context.getBean(PushCatchupStatus.class);
                    // No Awaitility dependency in this module: a manual poll matches the idiom this module's other
                    // async tests already use.
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (!(status.of("domain-feed-push-background-throwing-fold") instanceof PushCatchupStatus.Failed) && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertThat(status.of("domain-feed-push-background-throwing-fold"))
                            .as("the status of a projection whose background catch-up ended on a fold failure")
                            .isInstanceOfSatisfying(PushCatchupStatus.Failed.class, failed -> assertThat(failed.cause()).isSameAs(foldFailure));
                    assertThat(status.isCaughtUp("domain-feed-push-background-throwing-fold")).isFalse();
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FailingFoldConfiguration {

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

        // One event to replay, so the fold below runs at all.
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.of(cloudEvent("1"));
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
        ThrowingFoldProjection throwingFoldProjection() {
            return new ThrowingFoldProjection();
        }
    }

    static class ThrowingFoldProjection {
        @Projection(id = "domain-feed-push-background-throwing-fold", source = Source.PUSH, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> {
                        sneakyThrow(FOLD_FAILURE.get());
                        return state + 1;
                    })
                    .build();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("TestEvent").build();
    }

    record TestEvent(String id) {
    }
}
