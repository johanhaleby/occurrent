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
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A {@code @Projection} factory on a scoped bean runs once, on the instance the scope held when the scan read it, never
 * through the scoped proxy (issue #1492). A proxy of {@code ScopedProxyMode.INTERFACES} has no factory the class alone
 * declares, so a factory reached through it would be refused. The descriptor is read once at startup, so a later
 * refresh runs it no more. Every descriptor kind reads its instance through the same lookup, so projection stands for
 * {@code @Snapshot} and {@code @Saga} too. This test needs no Docker, an empty domain-feed reader is all a
 * {@code source = PUSH} projection needs to register.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ScopedDescriptorFactoryTest {

    private static final List<String> FACTORY_RAN_ON = new CopyOnWriteArrayList<>();
    private static final AtomicInteger INSTANCES = new AtomicInteger();

    private final RefreshableScope scope = new RefreshableScope();
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(DomainFeedConfiguration.class)
            .withInitializer(scope::registerIn);

    @Test
    void a_projection_factory_on_a_target_class_scoped_bean_runs_once_on_the_instance_the_scope_held_at_startup() {
        reset();
        runner.withUserConfiguration(TargetClassConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            scope.refresh();
            context.getBean(TargetClassProjectionHolder.class).toString();

            assertThat(FACTORY_RAN_ON).containsExactly("instance-1");
        });
    }

    @Test
    void a_projection_factory_only_the_class_declares_on_an_interfaces_scoped_bean_runs_on_the_target_instead_of_through_the_proxy() {
        reset();
        runner.withUserConfiguration(InterfacesConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            scope.refresh();
            context.getBean(Marker.class).toString();

            assertThat(FACTORY_RAN_ON).containsExactly("instance-1");
        });
    }

    private static void reset() {
        FACTORY_RAN_ON.clear();
        INSTANCES.set(0);
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projectionFrom(String instance) {
        FACTORY_RAN_ON.add(instance);
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    interface Marker {
    }

    public static class TargetClassProjectionHolder {
        private final String instance = "instance-" + INSTANCES.incrementAndGet();

        @Projection(id = "scoped-target-class-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return projectionFrom(instance);
        }
    }

    static class InterfacesProjectionHolder implements Marker {
        private final String instance = "instance-" + INSTANCES.incrementAndGet();

        @Projection(id = "scoped-interfaces-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return projectionFrom(instance);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TargetClassConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.TARGET_CLASS)
        TargetClassProjectionHolder targetClassProjectionHolder() {
            return new TargetClassProjectionHolder();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InterfacesConfiguration {
        @Bean
        @Scope(value = RefreshableScope.NAME, proxyMode = ScopedProxyMode.INTERFACES)
        InterfacesProjectionHolder interfacesProjectionHolder() {
            return new InterfacesProjectionHolder();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DomainFeedConfiguration {
        // A DomainEventFeed is not itself a Subscribable, so without this bean the reactive bean post processor skips
        // annotation processing entirely.
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("TestEvent").build();
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new TestEvent();
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }

        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader emptyReader = new PositionOrderedReader() {
                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.empty();
                }

                @Override
                public Mono<Long> currentPosition() {
                    return Mono.just(0L);
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(emptyReader, converter, event -> "k");
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, (id, value) -> store.put(id, value));
        }
    }

    record TestEvent() {
    }
}
