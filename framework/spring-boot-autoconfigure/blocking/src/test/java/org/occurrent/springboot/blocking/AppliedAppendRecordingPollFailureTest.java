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
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * The applied-append recording poll retries a clear a catch-up left owed, and each tick is the only thing that
 * schedules the one after it. A store that throws has to leave that poll running, or the projection stops recording
 * for the rest of the application's life with a clear still owed and nothing left to retry it.
 * <p>
 * Container-free, the same way {@code ProjectionAnnotationRecordingListenerSourceTest} is: a mocked model announces
 * a catch-up as the listener registers, which is what makes a clear owed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class AppliedAppendRecordingPollFailureTest {

    private static final String PROJECTION_ID = "orders";

    // An AppliedAppendStore written in Kotlin can throw a checked exception from clear() without declaring it.
    @Test
    void the_recording_poll_keeps_retrying_a_clear_that_throws_a_checked_exception() {
        assertThatTheClearIsRetried(new IOException("the store this projection records in is down"));
    }

    @Test
    void the_recording_poll_keeps_retrying_a_clear_that_throws_a_runtime_exception() {
        assertThatTheClearIsRetried(new IllegalStateException("the store this projection records in is down"));
    }

    private static void assertThatTheClearIsRetried(Exception clearFailure) {
        AtomicInteger clearAttempts = new AtomicInteger();
        AppliedAppendStore store = new AppliedAppendStore() {
            @Override
            public void recordApplied(String projectionId, AppendId appendId) {
            }

            @Override
            public boolean hasApplied(String projectionId, AppendId appendId) {
                return false;
            }

            @Override
            public void clear(String projectionId) {
                clearAttempts.incrementAndGet();
                sneakyThrow(clearFailure);
            }
        };

        Subscribable model = mock(Subscribable.class, withSettings().extraInterfaces(ReplayAwareSubscriptions.class));
        doReturn(java.util.Optional.of((ReplayAwareSubscriptions) model)).when(model).capability(ReplayAwareSubscriptions.class);
        when(((ReplayAwareSubscriptions) model).isCatchingUp(PROJECTION_ID)).thenReturn(false);
        // Announces a catch-up as the listener registers, which is what leaves a clear owed for the poll to retry.
        when(((ReplayAwareSubscriptions) model).listenForCatchup(eq(PROJECTION_ID), any())).thenAnswer(invocation -> {
            CatchupListener listener = invocation.getArgument(1);
            listener.catchupStarted(new Object());
            return true;
        });
        when(model.subscribe(anyString(), any(), any(StartAt.class), any())).thenAnswer(invocation -> mock(Subscription.class));

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean(AppliedAppendStore.class, () -> store)
                .withBean(Subscriptions.class, () -> new Subscriptions<>(model, testEventConverter()))
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    // No Awaitility dependency in this module: a manual poll matches the idiom this module's other
                    // async tests already use.
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (clearAttempts.get() < 2 && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertThat(clearAttempts).as("clear attempts by the recording poll").hasValueGreaterThanOrEqualTo(2);
                });
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
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
    @EnableConfigurationProperties(OccurrentProperties.class)
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
