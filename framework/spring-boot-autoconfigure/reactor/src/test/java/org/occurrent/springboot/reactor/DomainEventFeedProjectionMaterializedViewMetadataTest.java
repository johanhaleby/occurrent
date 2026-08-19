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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A domain-push {@code @Projection} materializing into a plain {@link MaterializedView}, rather than a
 * {@link org.occurrent.dsl.view.ViewStateRepository}, now folds a live event with its real {@link EventMetadata}
 * instead of {@link EventMetadata#empty()}. Regression coverage for the fix noted in the changelog under "A reactive
 * {@code @Projection(source = PUSH)} projection fed by a {@code DomainEventFeed} and materializing into a
 * {@code MaterializedView}".
 * <p>
 * {@code catchup = NONE} keeps this container-free and focused on the live path, the same shape
 * {@code ProjectionAnnotationPushWithoutCatchupTest} already uses for a {@code ViewStateRepository} target.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedProjectionMaterializedViewMetadataTest {

    private static final AtomicReference<EventMetadata> RECEIVED_METADATA = new AtomicReference<>();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(DomainFeedConfiguration.class);

    @Test
    void a_live_events_real_metadata_reaches_a_materializedView_target_instead_of_being_dropped() {
        RECEIVED_METADATA.set(null);
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            DomainEventFeed<TestEvent> feed = context.getBean(DomainEventFeed.class);
            EventMetadata metadata = new EventMetadata(Map.of(OccurrentCloudEventExtension.STREAM_ID, "order-1"));

            feed.accept(metadata, new TestEvent("live")).block();

            assertThat(RECEIVED_METADATA.get()).isNotNull();
            assertThat(RECEIVED_METADATA.get().getStreamId()).contains("order-1");
        });
    }

    record TestEvent(String eventId) {
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("TestEvent").build();
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

        @Bean
        MaterializedView<TestEvent> materializedView() {
            return new MaterializedView<>() {
                @Override
                public void update(TestEvent event) {
                    update(EventMetadata.empty(), event);
                }

                @Override
                public void update(EventMetadata metadata, TestEvent event) {
                    RECEIVED_METADATA.set(metadata);
                }
            };
        }

        @Bean
        DomainEventFeed<TestEvent> domainFeed(CloudEventConverter<TestEvent> converter) {
            // catchup = NONE never consults this, so it only has to satisfy the constructor's non-null reader.
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.just(cloudEvent("history"));
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
            return new DomainEventFeed<>(reader, converter, TestEvent::eventId);
        }

        @Bean
        MaterializedViewProjection materializedViewProjection() {
            return new MaterializedViewProjection();
        }
    }

    // The store the live event actually reaches is the MaterializedView bean above, resolved independently by
    // ProjectionAnnotationRegistrar.resolveStore. This descriptor only has to satisfy validatePushDescriptor's
    // requirement that a source=PUSH factory return a Projection; its own id/evolve are never consulted once the
    // resolved store is a MaterializedView rather than a ViewStateRepository.
    static class MaterializedViewProjection {
        @Projection(id = "domain-feed-materializedview-metadata", source = Source.PUSH, catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "unused")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }
}
