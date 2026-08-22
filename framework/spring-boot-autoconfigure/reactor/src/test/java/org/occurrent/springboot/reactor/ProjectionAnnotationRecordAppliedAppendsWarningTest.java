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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * ADR 132 decision 9's third case: {@code @Projection(recordAppliedAppends = true)} left at the default start
 * position never replays ({@code StartPositionSupport} bypasses the catch-up layer for it unconditionally), so it
 * records but never clears its own memberships on a rebuild. The registrar must warn once at startup naming the
 * projection, and must not warn when {@link StartPosition#BEGINNING} is what the projection actually declared.
 * Container-free: a mocked {@code Subscribable} stands in for the asynchronous model, mirroring the blocking stack's
 * {@code ProjectionAnnotationRecordAppliedAppendsWarningTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationRecordAppliedAppendsWarningTest {

    private static final String PROJECTION_ID = "orders";

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;
    private boolean loggerWasAdditive;

    @BeforeEach
    void attachAppender() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        logger = context.getLogger(ProjectionAnnotationRegistrar.class);
        loggerWasAdditive = logger.isAdditive();
        logger.setAdditive(false);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        logger.detachAppender(appender);
        logger.setAdditive(loggerWasAdditive);
    }

    private List<ILoggingEvent> warnings() {
        return appender.list.stream().filter(event -> event.getLevel() == Level.WARN).toList();
    }

    @Test
    void the_shipped_compositions_default_start_position_warns_naming_the_projection_because_the_starter_registered_that_it_never_replays() {
        // Simulates what OccurrentReactiveMongoAutoConfiguration actually does, suppliedBy for the capability lookup,
        // and defaultBypassesCatchup() as the separate, owner-supplied fact this warning is keyed on (issue 865).
        Subscribable model = mock(Subscribable.class, withSettings().extraInterfaces(ReplayAwareSubscriptions.class));
        doReturn(java.util.Optional.of((ReplayAwareSubscriptions) model)).when(model).capability(ReplayAwareSubscriptions.class);
        when(((ReplayAwareSubscriptions) model).isCatchingUp(anyString())).thenReturn(false);
        Subscription subscription = mock(Subscription.class);
        when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
        when(model.subscribe(anyString(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(StartAt.class), org.mockito.ArgumentMatchers.any()))
                .thenReturn(subscription);
        ComposedCatchupModel composedCatchupModel = new ComposedCatchupModel();
        composedCatchupModel.suppliedBy(model);
        composedCatchupModel.defaultBypassesCatchup();

        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean("defaultStartPositionProjection", DefaultStartPositionProjection.class, DefaultStartPositionProjection::new)
                .withBean(org.occurrent.dsl.projection.AppliedAppendStore.class, org.occurrent.dsl.projection.AppliedAppendStore::inMemory)
                .withBean("subscribable", Subscribable.class, () -> model)
                .withBean(ComposedCatchupModel.class, () -> composedCatchupModel)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(warnings()).hasSize(1);
                    assertThat(warnings().get(0).getFormattedMessage())
                            .contains(PROJECTION_ID)
                            .contains("recordAppliedAppends = true")
                            .contains("never replays");
                });
    }

    @Test
    void startAtGlobalPosition_does_not_warn_even_on_the_shipped_composition_because_it_overrides_default_and_genuinely_replays() {
        // Same owner-registered fact as the test above, but startAtGlobalPosition = 0 takes precedence over the
        // left-at-default startAt (generateAgnosticStartAt checks it first) and genuinely replays, so the DEFAULT
        // branch of verifiedNeverReplays must not treat this as the live-only case.
        Subscribable model = mock(Subscribable.class, withSettings().extraInterfaces(ReplayAwareSubscriptions.class));
        doReturn(java.util.Optional.of((ReplayAwareSubscriptions) model)).when(model).capability(ReplayAwareSubscriptions.class);
        when(((ReplayAwareSubscriptions) model).isCatchingUp(anyString())).thenReturn(true);
        Subscription subscription = mock(Subscription.class);
        when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
        when(model.subscribe(anyString(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(StartAt.class), org.mockito.ArgumentMatchers.any()))
                .thenReturn(subscription);
        ComposedCatchupModel composedCatchupModel = new ComposedCatchupModel();
        composedCatchupModel.suppliedBy(model);
        composedCatchupModel.defaultBypassesCatchup();
        EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
        when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(true);

        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean("startAtGlobalPositionProjection", StartAtGlobalPositionProjection.class, StartAtGlobalPositionProjection::new)
                .withBean(org.occurrent.dsl.projection.AppliedAppendStore.class, org.occurrent.dsl.projection.AppliedAppendStore::inMemory)
                .withBean("subscribable", Subscribable.class, () -> model)
                .withBean(ComposedCatchupModel.class, () -> composedCatchupModel)
                .withBean("eventStore", EventStore.class, () -> eventStore)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(warnings()).isEmpty();
                });
    }

    @Test
    void startAt_beginning_does_not_warn_because_a_rebuild_actually_replays_and_clears() {
        runWith(StartAtBeginningProjection.class, "startAtBeginningProjection", StartAtBeginningProjection::new);

        assertThat(warnings()).isEmpty();
    }

    @Test
    void a_custom_compositions_default_start_position_does_not_claim_it_never_replays_when_its_capability_is_unreadable() {
        // A model with no ReplayAwareSubscriptions capability at all (a custom or third-party one), and no
        // ComposedCatchupModel bean in this context either, the same as an application that supplied its own
        // Subscribable rather than the shipped Mongo composition. Nothing here registered the DEFAULT fact, and
        // DEFAULT resolves to StartAt.subscriptionModelDefault(), a marker whose actual behavior this specific,
        // unobservable composition is free to interpret however it wants, so it might genuinely replay. Only
        // resolveEventStorePhase's own "cannot tell" warning fires here, the same as the BEGINNING case below.
        Subscribable model = unobservableModel();

        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean("defaultStartPositionProjection", DefaultStartPositionProjection.class, DefaultStartPositionProjection::new)
                .withBean(org.occurrent.dsl.projection.AppliedAppendStore.class, org.occurrent.dsl.projection.AppliedAppendStore::inMemory)
                .withBean("subscribable", Subscribable.class, () -> model)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(warnings()).hasSize(1);
                    assertThat(warnings().get(0).getFormattedMessage())
                            .contains("does not expose")
                            .doesNotContain("its resolved start position or composition never replays");
                });
    }

    @Test
    void a_custom_compositions_default_start_position_does_not_warn_even_when_its_capability_is_readable() {
        // Unlike the shipped composition test above, this model does expose ReplayAwareSubscriptions, so
        // resolveEventStorePhase reads it directly and logs no "cannot tell" warning of its own, but there is still
        // no ComposedCatchupModel bean registering the DEFAULT fact, the same as an application-supplied composition
        // nobody told this registrar bypasses catch-up for DEFAULT. A capability-observable composition is not by
        // itself proof of what DEFAULT does, so this stays silent rather than guessing.
        Subscribable model = mock(Subscribable.class, withSettings().extraInterfaces(ReplayAwareSubscriptions.class));
        doReturn(java.util.Optional.of((ReplayAwareSubscriptions) model)).when(model).capability(ReplayAwareSubscriptions.class);
        when(((ReplayAwareSubscriptions) model).isCatchingUp(anyString())).thenReturn(false);
        Subscription subscription = mock(Subscription.class);
        when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
        when(model.subscribe(anyString(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(StartAt.class), org.mockito.ArgumentMatchers.any()))
                .thenReturn(subscription);

        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean("defaultStartPositionProjection", DefaultStartPositionProjection.class, DefaultStartPositionProjection::new)
                .withBean(org.occurrent.dsl.projection.AppliedAppendStore.class, org.occurrent.dsl.projection.AppliedAppendStore::inMemory)
                .withBean("subscribable", Subscribable.class, () -> model)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(warnings()).isEmpty();
                });
    }

    @Test
    void startAt_now_still_warns_that_it_never_replays_even_when_the_composition_cannot_say_whether_it_would() {
        // Unlike DEFAULT, an explicit NOW resolves to StartAt.now(), a documented, composition-independent contract
        // (subscribe at this moment, no replay) every Subscribable must honor regardless of whether it also exposes
        // ReplayAwareSubscriptions, so that fact holds and is worth its own warning alongside resolveEventStorePhase's
        // "cannot tell" one, not instead of it.
        Subscribable model = unobservableModel();

        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean("startAtNowProjection", StartAtNowProjection.class, StartAtNowProjection::new)
                .withBean(org.occurrent.dsl.projection.AppliedAppendStore.class, org.occurrent.dsl.projection.AppliedAppendStore::inMemory)
                .withBean("subscribable", Subscribable.class, () -> model)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(warnings()).hasSize(2);
                    assertThat(warnings()).anySatisfy(event -> assertThat(event.getFormattedMessage()).contains("does not expose"));
                    assertThat(warnings()).anySatisfy(event -> assertThat(event.getFormattedMessage())
                            .contains("its resolved start position or composition never replays"));
                });
    }

    @Test
    void startAt_beginning_on_a_composition_whose_replay_awareness_cannot_be_read_does_not_claim_it_never_replays() {
        // Here the projection's own configuration does ask for a replay (startAt = BEGINNING), so whether it ever
        // actually happens depends entirely on a composition this registrar cannot read. resolveEventStorePhase's
        // "cannot tell" warning already covers that ambiguity, and warnIfRecordingNeverResets must not also claim
        // the composition never replays, since it might genuinely be replaying right now.
        Subscribable model = unobservableModel();
        EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
        when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(true);

        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean("startAtBeginningProjection", StartAtBeginningProjection.class, StartAtBeginningProjection::new)
                .withBean(org.occurrent.dsl.projection.AppliedAppendStore.class, org.occurrent.dsl.projection.AppliedAppendStore::inMemory)
                .withBean("subscribable", Subscribable.class, () -> model)
                .withBean("eventStore", EventStore.class, () -> eventStore)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(warnings()).hasSize(1);
                    assertThat(warnings().get(0).getFormattedMessage())
                            .contains("does not expose")
                            .doesNotContain("its resolved start position or composition never replays");
                });
    }

    private static Subscribable unobservableModel() {
        Subscribable model = mock(Subscribable.class);
        doReturn(java.util.Optional.empty()).when(model).capability(ReplayAwareSubscriptions.class);
        Subscription subscription = mock(Subscription.class);
        when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
        when(model.subscribe(anyString(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(StartAt.class), org.mockito.ArgumentMatchers.any()))
                .thenReturn(subscription);
        return model;
    }

    private <P> void runWith(Class<P> projectionType, String beanName, java.util.function.Supplier<P> projectionFactory) {
        Subscribable model = mock(Subscribable.class, withSettings().extraInterfaces(ReplayAwareSubscriptions.class));
        // capability(...) is a default method, so a plain mock does not run its real instanceof check and must be
        // told directly what it exposes.
        doReturn(java.util.Optional.of((ReplayAwareSubscriptions) model)).when(model).capability(ReplayAwareSubscriptions.class);
        when(((ReplayAwareSubscriptions) model).isCatchingUp(anyString())).thenReturn(false);
        Subscription subscription = mock(Subscription.class);
        when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
        when(model.subscribe(anyString(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(StartAt.class), org.mockito.ArgumentMatchers.any()))
                .thenReturn(subscription);

        // Needed so StartPositionSupport.positionReplaySupported() answers true: startAt = BEGINNING only replays
        // when the reactive position-based catch-up can actually run, which this test's second case exercises.
        EventStore eventStore = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class));
        when(((PositionOrderedReader) eventStore).writesPosition()).thenReturn(true);

        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(TestConfiguration.class)
                .withBean(beanName, projectionType, projectionFactory)
                .withBean(org.occurrent.dsl.projection.AppliedAppendStore.class, org.occurrent.dsl.projection.AppliedAppendStore::inMemory)
                .withBean("subscribable", Subscribable.class, () -> model)
                .withBean("eventStore", EventStore.class, () -> eventStore)
                .run(context -> assertThat(context).hasNotFailed());
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

    static class DefaultStartPositionProjection {
        @Projection(id = PROJECTION_ID, recordAppliedAppends = true)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
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

    static class StartAtNowProjection {
        @Projection(id = PROJECTION_ID, recordAppliedAppends = true, startAt = StartPosition.NOW)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    // startAt is left at its default value on purpose. startAtGlobalPosition >= 0 overrides it and replays from
    // that position regardless (generateAgnosticStartAt checks it first), so this is a valid, genuinely replaying
    // configuration that must not trip the DEFAULT branch of verifiedNeverReplays.
    static class StartAtGlobalPositionProjection {
        @Projection(id = PROJECTION_ID, recordAppliedAppends = true, startAtGlobalPosition = 0)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }
}
