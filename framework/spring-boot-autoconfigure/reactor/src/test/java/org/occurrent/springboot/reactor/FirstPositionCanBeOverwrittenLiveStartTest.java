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

import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A live start that pins the start boundary as the first checkpoint of a subscription writes it unconditionally when
 * the application's own {@code CheckpointStorage} answers {@code false} from {@code evaluatesWriteConditionsFor(id)}.
 * Two nodes starting at the same moment can then overwrite each other's boundary and lose the events between the two,
 * so startup is refused, unless {@code occurrent.subscription.start-when-first-position-can-be-overwritten} is true.
 * Container-free, with an event store whose boundary is fixed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class FirstPositionCanBeOverwrittenLiveStartTest {

    private static final String PROPERTY = "occurrent.subscription.start-when-first-position-can-be-overwritten";
    private static final String BOUNDARY = "7";
    private static final SubscriptionModelContext ANY_MODEL = new SubscriptionModelContext(Object.class);

    private final RecordingCheckpointStorage storage = new RecordingCheckpointStorage();

    private ApplicationContextRunner runner(Class<?> handlers) {
        return new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withBean(CheckpointStorage.class, () -> storage)
                .withUserConfiguration(InfrastructureConfiguration.class, handlers);
    }

    @Test
    void startup_is_refused_before_anything_is_written_when_the_storage_cannot_evaluate_write_conditions() {
        storage.idsWithoutConditions.add("pinning-handler");

        runner(PinningHandlerConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(PROPERTY)
                    .hasMessageContaining("pinning-handler");
            assertThat(context.getStartupFailure()).hasStackTraceContaining(PROPERTY);
            assertThat(storage.saves).as("the saves the storage saw").isEmpty();
            assertThat(storage.read("pinning-handler").blockOptional()).isEmpty();
        });
    }

    @Test
    void a_default_start_that_pins_the_boundary_is_refused_the_same_way() {
        storage.idsWithoutConditions.add("default-handler");

        runner(DefaultHandlerConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(PROPERTY);
            assertThat(storage.saves).as("the saves the storage saw").isEmpty();
        });
    }

    @Test
    void the_property_starts_it_with_the_boundary_written_unconditionally() {
        storage.idsWithoutConditions.add("pinning-handler");

        runner(PinningHandlerConfiguration.class).withPropertyValues(PROPERTY + "=true").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(storage.saves).hasSize(1);
            assertThat(storage.saves.getFirst().subscriptionId()).isEqualTo("pinning-handler");
            assertThat(storage.saves.getFirst().position()).isEqualTo(GlobalCheckpoint.of(Long.parseLong(BOUNDARY)).asString());
            assertThat(storage.saves.getFirst().condition()).isEqualTo(CheckpointWriteCondition.any());
            assertThat(startOf(context, "pinning-handler").isDefault()).as("the registration starts at the model default").isTrue();
        });
    }

    @Test
    void only_the_subscription_id_the_storage_cannot_evaluate_write_conditions_for_is_refused() {
        storage.idsWithoutConditions.add("pinning-handler");

        runner(OtherHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(storage.saves).hasSize(1);
            assertThat(storage.saves.getFirst().subscriptionId()).isEqualTo("other-handler");
            assertThat(storage.saves.getFirst().condition()).isEqualTo(CheckpointWriteCondition.ifAbsent());
            assertThat(startOf(context, "other-handler").isDefault()).isTrue();
        });
    }

    @Test
    void a_checkpoint_already_stored_is_not_refused_since_nothing_is_pinned_over_it() {
        storage.idsWithoutConditions.add("pinning-handler");
        storage.writeWithoutRecording("pinning-handler", GlobalCheckpoint.of(3));

        runner(PinningHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(storage.saves).as("the saves the storage saw").isEmpty();
            assertThat(storage.read("pinning-handler").block()).isEqualTo(GlobalCheckpoint.of(3));
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "true"})
    void a_live_start_that_pins_nothing_is_never_refused(String property) {
        storage.idsWithoutConditions.add("same-as-start-at-handler");

        runner(SameAsStartAtHandlerConfiguration.class).withPropertyValues(PROPERTY + "=" + property).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(storage.saves).as("the saves the storage saw").isEmpty();
            StartAt resolved = startOf(context, "same-as-start-at-handler").get(ANY_MODEL);
            assertThat(resolved).isInstanceOf(StartAt.StartAtCheckpoint.class);
            assertThat(((StartAt.StartAtCheckpoint) resolved).checkpoint).isEqualTo(GlobalCheckpoint.of(Long.parseLong(BOUNDARY)));
        });
    }

    @SuppressWarnings("unchecked")
    private static StartAt startOf(ApplicationContext context, String subscriptionId) {
        ArgumentCaptor<StartAt> startAt = ArgumentCaptor.forClass(StartAt.class);
        verify(context.getBean(Subscriptions.class)).subscribe(eq(subscriptionId), any(AgnosticSubscriptionFilter.class), startAt.capture(), any(Function2.class));
        return startAt.getValue();
    }

    private record Save(String subscriptionId, String position, CheckpointWriteCondition condition) {
    }

    /**
     * Answers {@code false} from {@code evaluatesWriteConditionsFor} for the ids in {@link #idsWithoutConditions}, and
     * {@code true} for every other. Records every save.
     */
    static final class RecordingCheckpointStorage implements CheckpointStorage {
        final Set<String> idsWithoutConditions = ConcurrentHashMap.newKeySet();
        final List<Save> saves = new CopyOnWriteArrayList<>();
        private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

        void writeWithoutRecording(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> Mono.justOrEmpty(checkpoints.get(subscriptionId)));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.fromSupplier(() -> {
                saves.add(new Save(subscriptionId, checkpoint.asString(), condition));
                checkpoints.put(subscriptionId, checkpoint);
                return checkpoint;
            });
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return !idsWithoutConditions.contains(subscriptionId);
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.fromRunnable(() -> checkpoints.remove(subscriptionId));
        }
    }

    static class PinningHandler {
        @Subscription(id = "pinning-handler", startAt = StartPosition.NOW, startupMode = StartupMode.BACKGROUND)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class DefaultHandler {
        @Subscription(id = "default-handler", startAt = StartPosition.DEFAULT, startupMode = StartupMode.BACKGROUND)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class OtherHandler {
        @Subscription(id = "other-handler", startAt = StartPosition.NOW, startupMode = StartupMode.BACKGROUND)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class SameAsStartAtHandler {
        @Subscription(id = "same-as-start-at-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void on(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PinningHandlerConfiguration {
        @Bean
        PinningHandler pinningHandler() {
            return new PinningHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DefaultHandlerConfiguration {
        @Bean
        DefaultHandler defaultHandler() {
            return new DefaultHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OtherHandlerConfiguration {
        @Bean
        OtherHandler otherHandler() {
            return new OtherHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SameAsStartAtHandlerConfiguration {
        @Bean
        SameAsStartAtHandler sameAsStartAtHandler() {
            return new SameAsStartAtHandler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InfrastructureConfiguration {
        @Bean
        CloudEventConverter<LateBeanAnnotationRegistrationTest.TestEvent> testEventCloudEventConverter() {
            return new LateBeanAnnotationRegistrationTest.NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<LateBeanAnnotationRegistrationTest.TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        LiveStartCheckpoints liveStartCheckpoints() {
            return liveStart -> GlobalCheckpoint.of(Long.parseLong(liveStart));
        }

        @Bean
        EventStore eventStore() {
            EventStore store = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class, CatchupReads.class));
            when(((CatchupReads) store).openCatchupReadScope(any())).thenAnswer(invocation -> Mono.just(mock(CatchupReadScope.class, scopeCall -> switch (scopeCall.getMethod().getName()) {
                case "liveStart" -> BOUNDARY;
                case "close" -> Mono.empty();
                default -> null;
            })));
            return store;
        }
    }
}
