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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.SubscriptionMode;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
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

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withBean(ManualStartPushSources.class, ManualStartPushSources::new)
            .withUserConfiguration(ManualDomainFeedConfiguration.class);

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

        @Bean
        ClosingPushProjection closingPushProjection() {
            return new ClosingPushProjection();
        }
    }

    static class ClosingPushProjection {
        @Projection(id = PROJECTION_ID, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }
}
