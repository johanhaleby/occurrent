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
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * The data-correctness half of <a href="https://github.com/johanhaleby/occurrent/issues/996">#996</a>, kept out of
 * {@code ProjectionAnnotationRecordAppliedAppendsWarningTest} because nothing here is about a warning. This is about
 * which object the applied-append recorder ends up listening to, and it needs no log appender to say so.
 * <p>
 * A context can hold more than one subscription model. Handing a projection the catch-up layer of a composition it
 * does not run on is worse than handing it nothing at all. That layer has never heard of this subscription id, so it
 * answers "not catching up" for a whole replay, the replay is recorded as live appends, and {@code waitUntilApplied}
 * then says yes for an append the rebuilt read model has not applied.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationRecordAppliedAppendsCatchupModelTest {

    private static final String PROJECTION_ID = "orders";
    private static final String THE_MODEL_THE_PROJECTION_RUNS_ON = "the model the projection runs on";
    private static final String THE_STARTERS_CATCHUP_LAYER = "the starter's catch-up layer";

    @Test
    void the_recorder_registers_for_catchups_with_the_model_the_projection_runs_on_and_never_with_the_starters_catchup_layer() {
        List<String> registrations = new CopyOnWriteArrayList<>();

        // The starter composed its own model and handed the holder the catch-up layer hidden inside it. Nothing in
        // this context subscribes through that model, so nothing may ever register with its catch-up layer.
        Subscribable starterModel = mock(Subscribable.class, withSettings().name("starterModel"));
        ReplayAwareSubscriptions starterCatchupLayer = mock(ReplayAwareSubscriptions.class, withSettings().name(THE_STARTERS_CATCHUP_LAYER));
        recordRegistrationsOn(starterCatchupLayer, registrations, THE_STARTERS_CATCHUP_LAYER);
        ComposedCatchupModel composedCatchupModel = new ComposedCatchupModel();
        composedCatchupModel.suppliedBy(starterModel, starterCatchupLayer);
        composedCatchupModel.defaultBypassesCatchup();

        // The model the application actually supplied, exposing a catch-up capability of its own. This is the one
        // the projection subscribes through, so this is the one that knows its subscription id.
        Subscribable runningModel = mock(Subscribable.class, withSettings().name(THE_MODEL_THE_PROJECTION_RUNS_ON).extraInterfaces(ReplayAwareSubscriptions.class));
        doReturn(java.util.Optional.of((ReplayAwareSubscriptions) runningModel)).when(runningModel).capability(ReplayAwareSubscriptions.class);
        recordRegistrationsOn((ReplayAwareSubscriptions) runningModel, registrations, THE_MODEL_THE_PROJECTION_RUNS_ON);
        Subscription subscription = mock(Subscription.class);
        when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
        when(runningModel.subscribe(anyString(), any(), any(StartAt.class), any())).thenReturn(subscription);

        // startAt = BEGINNING genuinely replays, which is the only configuration where getting this wrong costs
        // anything, and it needs a store that writes a global position for the reactive catch-up to be possible.
        EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
        when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(true);

        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean("startAtBeginningProjection", StartAtBeginningProjection.class, StartAtBeginningProjection::new)
                .withBean(AppliedAppendStore.class, AppliedAppendStore::inMemory)
                .withBean("subscribable", Subscribable.class, () -> runningModel)
                .withBean(ComposedCatchupModel.class, () -> composedCatchupModel)
                .withBean("eventStore", EventStore.class, () -> eventStore)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(registrations)
                            .as("the layer the applied-append recorder for @Projection '%s' registered its catch-up listener with", PROJECTION_ID)
                            .containsExactly(THE_MODEL_THE_PROJECTION_RUNS_ON);
                });
    }

    // Returns true, the listener path, rather than false, the polled fallback. Both branches call listenForCatchup
    // and so both record here, but the listener path is what a layer that really knows this subscription answers,
    // and picking it keeps the recorded registration the thing under test instead of a poll registration that would
    // happen either way.
    private static void recordRegistrationsOn(ReplayAwareSubscriptions layer, List<String> registrations, String name) {
        doAnswer(invocation -> {
            registrations.add(name);
            return true;
        }).when(layer).listenForCatchup(anyString(), any(CatchupListener.class));
    }

    private static CloudEventConverter<TestEvent> testEventConverter() {
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

    record TestEvent() {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(org.occurrent.springboot.common.OccurrentProperties.class)
    static class TestConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return testEventConverter();
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }
    }

    static class StartAtBeginningProjection {
        @Projection(id = PROJECTION_ID, recordAppliedAppends = true, startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }
}
