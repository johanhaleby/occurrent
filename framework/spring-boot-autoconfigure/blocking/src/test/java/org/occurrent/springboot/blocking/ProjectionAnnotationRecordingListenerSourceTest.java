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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * The recording phase for an event-store projection registered through {@link Subscriptions} must come from the
 * exact model that {@link Subscriptions} bean runs on, not from an independently resolved {@code Subscribable} bean
 * of the same type that happens to exist elsewhere in the context.
 * <p>
 * {@code correctModel} is the one {@link Subscriptions} was actually built from here, and it announces a catch-up
 * the moment a listener registers, the way a real model does. {@code distractorModel} is a second, unrelated
 * {@code Subscribable} bean the context also happens to have, and it announces nothing. Registering on the wrong one
 * would record the delivered event's append id anyway, since nothing would have told the projection a catch-up was
 * running. Container-free, since a mock captures the delivery callback {@link Subscriptions} passes to whichever
 * model it wraps, so no real subscription infrastructure runs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationRecordingListenerSourceTest {

    private static final String PROJECTION_ID = "orders";

    @Test
    void the_recording_listener_is_registered_on_the_subscriptions_beans_own_model_not_an_unrelated_subscribable_bean() {
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        AppendId appendId = AppendId.mint();

        Subscribable correctModel = mock(Subscribable.class, withSettings().extraInterfaces(ReplayAwareSubscriptions.class));
        // capability(...) is a default method, so a plain mock does not run its real instanceof check and must be
        // told directly what it exposes.
        doReturn(java.util.Optional.of((ReplayAwareSubscriptions) correctModel)).when(correctModel).capability(ReplayAwareSubscriptions.class);
        when(((ReplayAwareSubscriptions) correctModel).isCatchingUp(PROJECTION_ID)).thenReturn(true);
        // Announces a catch-up as the listener registers, which is what a real model does when one is already
        // running, and then delivers its history through the same action.
        when(((ReplayAwareSubscriptions) correctModel).listenForCatchup(eq(PROJECTION_ID), any())).thenAnswer(invocation -> {
            CatchupListener listener = invocation.getArgument(1);
            listener.catchupStarted(new Object());
            return true;
        });
        when(correctModel.subscribe(anyString(), any(), any(StartAt.class), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<CloudEvent> action = invocation.getArgument(3);
            action.accept(cloudEvent(appendId));
            return mock(SubscriptionHandle.class);
        });

        Subscribable distractorModel = mock(Subscribable.class, withSettings().extraInterfaces(ReplayAwareSubscriptions.class));
        doReturn(java.util.Optional.of((ReplayAwareSubscriptions) distractorModel)).when(distractorModel).capability(ReplayAwareSubscriptions.class);
        when(((ReplayAwareSubscriptions) distractorModel).isCatchingUp(anyString())).thenReturn(false);
        when(((ReplayAwareSubscriptions) distractorModel).listenForCatchup(anyString(), any())).thenReturn(true);

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean("distractorModel", Subscribable.class, () -> distractorModel)
                .withBean(AppliedAppendStore.class, () -> store)
                .withBean(Subscriptions.class, () -> new Subscriptions<>(correctModel, testEventConverter()))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // correctModel (what Subscriptions actually runs on) announced a catch-up, so the event it
                    // then delivered must not be recorded. A listener registered on distractorModel instead would
                    // have left the projection recording, since nothing would have announced anything.
                    assertThat(store.hasApplied(PROJECTION_ID, appendId)).isFalse();
                    verify((ReplayAwareSubscriptions) distractorModel, never()).listenForCatchup(anyString(), any());
                });
    }

    private static CloudEvent cloudEvent(AppendId appendId) {
        return CloudEventBuilder.v1()
                .withId("1")
                .withSource(URI.create("urn:test"))
                .withType("TestEvent")
                .withExtension(OccurrentCloudEventExtension.APPEND_ID, appendId.toString())
                .build();
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

        @Bean
        RecordingProjection recordingProjection() {
            return new RecordingProjection();
        }
    }

    static class RecordingProjection {
        @Projection(id = PROJECTION_ID, recordAppliedAppends = true)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }
}
