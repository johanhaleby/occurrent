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
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Snapshot;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Characterizes what the blocking stack does when a store-neutral module is used without a store starter: each of the
 * three zero-config default-store seams ({@link DefaultProjectionStoreProvider}, {@link DefaultSnapshotStoreProvider}
 * and {@link DefaultSagaStateStoreProvider}) must fail fast naming the annotation id, rather than NPE on a missing
 * provider or silently drop the store.
 * <p>
 * Every factory here declares a concrete generic return type on purpose. A raw return type is rejected one step earlier
 * by state-type reflection, which is what the sibling {@code *AnnotationValidationTest}s cover, so it never reaches the
 * provider lookup.
 * <p>
 * Container-free, since no store is ever created.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DefaultStoreProviderSeamTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(ConverterConfiguration.class);

    @Test
    void projection_without_a_store_bean_and_without_a_default_store_provider_fails_fast() {
        runner.withUserConfiguration(ProjectionConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("projection-default-store")
                    .hasMessageContaining("found no read-model store bean and this starter contributes no zero-config default");
        });
    }

    @Test
    void snapshot_without_a_store_bean_and_without_a_default_store_provider_fails_fast() {
        runner.withUserConfiguration(SnapshotConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("snapshot-default-store")
                    .hasMessageContaining("found no SnapshotStore bean and this starter contributes no zero-config default");
        });
    }

    @Test
    void saga_without_a_store_bean_and_without_a_default_store_provider_fails_fast() {
        runner.withUserConfiguration(SubscribableConfiguration.class, SagaConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("saga-default-store")
                    .hasMessageContaining("found no SagaStateStore bean and this starter contributes no zero-config default");
        });
    }

    @Test
    void projection_with_two_default_store_providers_fails_fast_naming_both_provider_beans() {
        // The projection seam stands in for all three here: they share the same getIfAvailable/NoUniqueBeanDefinitionException
        // shape, and this is the one that needs no Subscribable or store fixture beans.
        runner.withUserConfiguration(TwoProvidersConfiguration.class, ProjectionConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(seamFailure(context.getStartupFailure()))
                    .hasMessageContaining("projection-default-store")
                    .hasMessageContaining("cannot pick one to create the zero-config default read-model store")
                    .hasMessageContaining("providerA")
                    .hasMessageContaining("providerB");
        });
    }

    /**
     * The seam's own {@link IllegalStateException}, not the most specific cause. The ambiguous branch rethrows with
     * Spring's {@code NoUniqueBeanDefinitionException} as the cause, and that cause names the provider beans too, so
     * asserting on the cause or on the whole stack trace would pass even if the seam's own message said nothing.
     */
    private static Throwable seamFailure(Throwable startupFailure) {
        for (Throwable current = startupFailure; current != null; current = current.getCause()) {
            if (current instanceof IllegalStateException && current.getMessage() != null && current.getMessage().startsWith("@Projection")) {
                return current;
            }
        }
        throw new AssertionError("No @Projection IllegalStateException in the failure chain", startupFailure);
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterConfiguration {
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
    }

    @Configuration(proxyBeanMethods = false)
    static class SubscribableConfiguration {
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoProvidersConfiguration {
        @Bean
        DefaultProjectionStoreProvider providerA() {
            return new NeverCalledProjectionStoreProvider();
        }

        @Bean
        DefaultProjectionStoreProvider providerB() {
            return new NeverCalledProjectionStoreProvider();
        }
    }

    // Throws rather than returning a store, because an ambiguous seam must be reported before any provider is picked.
    static class NeverCalledProjectionStoreProvider implements DefaultProjectionStoreProvider {
        @Override
        public <S, ID> ViewStateRepository<S, ID> createDefaultProjectionStore(String projectionId, Class<S> stateType) {
            throw new UnsupportedOperationException("must not be called when two providers exist");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ProjectionConfiguration {
        @Bean
        DefaultStoreProjection defaultStoreProjection() {
            return new DefaultStoreProjection();
        }
    }

    static class DefaultStoreProjection {
        @Projection(id = "projection-default-store")
        org.occurrent.dsl.projection.Projection<TestState, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<TestState, TestEvent, String>builder(new TestState())
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SnapshotConfiguration {
        @Bean
        DefaultStoreSnapshot defaultStoreSnapshot() {
            return new DefaultStoreSnapshot();
        }
    }

    static class DefaultStoreSnapshot {
        @Snapshot(id = "snapshot-default-store")
        SnapshotView<TestState, TestEvent> snapshot() {
            return SnapshotView.<TestState, TestEvent>builder(new TestState())
                    .on(TestEvent.class, (state, event) -> state)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SagaConfiguration {
        @Bean
        DefaultStoreSaga defaultStoreSaga() {
            return new DefaultStoreSaga();
        }
    }

    static class DefaultStoreSaga {
        @Saga(id = "saga-default-store")
        org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> saga() {
            return org.occurrent.dsl.saga.Saga.<TestEvent, TestState, TestCommand>builder(new TestState())
                    .correlateAll(event -> "k")
                    .startsOn(TestEvent.class)
                    .build();
        }
    }

    record TestState() {
    }

    record TestEvent() {
    }

    record TestCommand() {
    }
}
