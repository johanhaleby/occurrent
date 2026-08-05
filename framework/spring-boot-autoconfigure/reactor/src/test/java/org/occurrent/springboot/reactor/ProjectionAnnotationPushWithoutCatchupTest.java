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
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscribable;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * {@code @Projection(source = PUSH, catchup = NONE)} for both feed types on the reactor stack: no event-store bean is
 * required and no history is replayed, mirroring the blocking twin of this test.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationPushWithoutCatchupTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(ConverterConfiguration.class);

    @Test
    void a_push_model_projection_with_catchup_none_needs_no_event_store_beans_and_folds_a_live_event() {
        runner.withUserConfiguration(PushModelConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            // Not incidental: catch-up resolves both of these by type, so a projection fed by a foreign broker could
            // not start if the registrar asked for them regardless of catchup.
            assertThat(context.getBeanNamesForType(PositionOrderedReader.class)).isEmpty();
            assertThat(context.getBeanNamesForType(CheckpointStorage.class)).isEmpty();

            PushSubscriptionModel feed = context.getBean(PushSubscriptionModel.class);
            @SuppressWarnings("unchecked")
            ViewStateRepository<Integer, String> store = context.getBean(ViewStateRepository.class);
            feed.accept(cloudEvent("1")).block();

            assertThat(store.findById("counter")).contains(1);
        });
    }

    @Test
    void a_domain_feed_projection_with_catchup_none_goes_live_without_reading_the_feeds_own_history() {
        runner.withUserConfiguration(DomainFeedConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            DomainEventFeed<TestEvent> feed = context.getBean("domainFeed", DomainEventFeed.class);
            Map<String, Integer> repo = context.getBean("domainNoneRepo", Map.class);

            // The feed's own reader would answer with history if it were ever consulted; goLive() must not consult it.
            feed.accept(new TestEvent("live")).block();

            assertThat(repo.get("counter")).isEqualTo(1);
        });
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("TestEvent").build();
    }

    record TestEvent(String eventId) {
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> countProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "counter")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return cloudEvent(domainEvent.eventId());
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
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PushModelConfiguration {
        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, (id, value) -> store.put(id, value));
        }

        @Bean
        PushNoneProjection pushNoneProjection() {
            return new PushNoneProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DomainFeedConfiguration {
        // A DomainEventFeed is not itself a Subscribable, unlike PushSubscriptionModel, so without this bean the
        // registrar's whole annotation scan is skipped rather than reaching this projection at all.
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        Map<String, Integer> domainNoneRepo() {
            return new ConcurrentHashMap<>();
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository(Map<String, Integer> domainNoneRepo) {
            return ViewStateRepository.create(domainNoneRepo::get, domainNoneRepo::put);
        }

        @Bean
        DomainEventFeed<TestEvent> domainFeed(CloudEventConverter<TestEvent> converter) {
            // Answers with history if ever consulted, so a passing test proves goLive() never reads it.
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.just(cloudEvent("history-1"), cloudEvent("history-2"));
                }

                @Override
                public Mono<Long> currentPosition() {
                    return Mono.just(2L);
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(reader, converter, TestEvent::eventId);
        }

        @Bean
        DomainNoneProjection domainNoneProjection() {
            return new DomainNoneProjection();
        }
    }

    static class DomainNoneProjection {
        @Projection(id = "domain-none-projection-reactive", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    static class PushNoneProjection {
        @Projection(id = "push-none-projection-reactive", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }
}
