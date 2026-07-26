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

package org.occurrent.springboot.mongo.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactor twin of the blocking {@code ProjectionPushCatchupTunablesTest}: proves that
 * {@code occurrent.subscription.catchup-then-live.*} reaches the catch-up-then-live model this stack's
 * {@code @Projection(source = PUSH)} wiring builds. Without it the reactive registrar's one wiring line would be
 * covered only by a resolution unit test, so dropping the options there would go unnoticed.
 * <p>
 * The tunables are private with no accessor, so the assertion goes through the fail-loud buffer overflow, whose message
 * names its cap. The reader pushes three live events into the feed while the replay is being consumed, so with a cap of
 * two the third overflows. Container-free, because it needs only a fake reader and the real push model.
 * <p>
 * Where this differs from the blocking twin: there, {@code accept} throws into the caller, so the overflow travels up
 * the replay and fails context startup. Here {@code accept} returns a {@link Mono} and the engine routes the overflow
 * to that payload's ack, so the context starts fine and the pusher is the only one who sees it. The fake reader
 * therefore records what its own acks report.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveProjectionPushCatchupTunablesTest {

    // Static so the reader bean, which the runner builds per context, can record into the same list the test
    // asserts on. Cleared before each test, since the list survives between them.
    private static final List<Throwable> ACK_FAILURES = new CopyOnWriteArrayList<>();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(PushCatchupConfiguration.class);

    @BeforeEach
    void clearRecordedFailures() {
        ACK_FAILURES.clear();
    }

    @Test
    void the_configured_buffer_cap_reaches_the_catch_up_then_live_model_that_the_push_projection_is_bootstrapped_with() {
        runner.withPropertyValues("occurrent.subscription.catchup-then-live.max-buffered-events=2").run(context -> {
            // "cap 2" rather than just "buffer overflowed": the default cap is 100000, so naming the number is what
            // distinguishes the property having been applied from it having been dropped.
            assertThat(ACK_FAILURES).hasSize(1);
            assertThat(ACK_FAILURES.get(0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("buffer overflowed")
                    .hasMessageContaining("(cap 2)");
        });
    }

    @Test
    void the_default_cap_applies_when_no_property_is_set_so_the_same_three_events_are_buffered_fine() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(ACK_FAILURES).isEmpty();
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PushCatchupConfiguration {

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        // A real fake, not a mock: the reactive catch-up calls hasElement() on what read() returns, and a mock's null
        // default fails there before the buffer cap is ever reached.
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

        // Pushes three live events into the feed while the single replayed event is being consumed, so they are
        // buffered by the handover rather than folded directly.
        @Bean
        PositionOrderedReader reader(PushSubscriptionModel pushModel) {
            CloudEvent replayed = cloudEvent("history");
            List<CloudEvent> live = List.of(cloudEvent("l1"), cloudEvent("l2"), cloudEvent("l3"));
            return new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    boolean[] pushed = {false};
                    return Flux.just(replayed).doOnNext(ignored -> {
                        if (!pushed[0]) {
                            pushed[0] = true;
                            live.forEach(cloudEvent -> pushModel.accept(cloudEvent).subscribe(ignoredValue -> {
                            }, ACK_FAILURES::add));
                        }
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
        @Projection(id = "push-tunables-reactive", source = Source.PUSH)
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
