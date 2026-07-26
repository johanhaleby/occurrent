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

package org.occurrent.springboot.mongo.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Proves that {@code occurrent.subscription.catchup-then-live.*} actually reaches the catch-up-then-live subscription
 * model the {@code @Projection(source = PUSH)} wiring builds, which is the one link the resolution and binding tests
 * cannot cover.
 * <p>
 * The tunables are private with no accessor, so the assertion goes through the only observable consequence: the
 * fail-loud buffer overflow names its cap. The reader pushes three live events into the feed while the replay is being
 * iterated, so with a cap of two the third overflows. Container-free, because everything it needs is a fake reader and
 * the real push model.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionPushCatchupTunablesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(PushCatchupConfiguration.class);

    @Test
    void the_configured_buffer_cap_reaches_the_catch_up_then_live_model_that_the_push_projection_is_bootstrapped_with() {
        runner.withPropertyValues("occurrent.subscription.catchup-then-live.max-buffered-events=2").run(context -> {
            assertThat(context).hasFailed();

            // "cap 2" rather than just "buffer overflowed": the default cap is 100000, so naming the number is what
            // distinguishes the property having been applied from it having been dropped.
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("buffer overflowed")
                    .hasMessageContaining("(cap 2)");
        });
    }

    @Test
    void the_default_cap_applies_when_no_property_is_set_so_the_same_three_events_are_buffered_fine() {
        runner.run(context -> assertThat(context).hasNotFailed());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PushCatchupConfiguration {

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
            return ViewStateRepository.create(store::get, store::put);
        }

        // Pushes three live events into the feed while the single replayed event is being consumed, so they are
        // buffered by the handover rather than folded directly.
        @Bean
        PositionOrderedReader reader(PushSubscriptionModel pushModel) {
            CloudEvent replayed = cloudEvent("history");
            List<CloudEvent> live = List.of(cloudEvent("l1"), cloudEvent("l2"), cloudEvent("l3"));
            return new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    boolean[] pushed = {false};
                    return Stream.of(replayed).peek(ignored -> {
                        if (!pushed[0]) {
                            pushed[0] = true;
                            live.forEach(pushModel::accept);
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

        @Bean
        PushProjection pushProjection() {
            return new PushProjection();
        }
    }

    static class PushProjection {
        @Projection(id = "push-tunables", source = Source.PUSH)
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
