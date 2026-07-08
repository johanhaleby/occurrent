/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClients;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.dcb.annotation.AnnotationTagGenerator;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

@Testcontainers
class OccurrentReactiveMongoAutoConfigurationWiringTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true);
        mongoDBContainer.setPortBindings(ports);
    }

    private static ReactiveMongoTemplate mongoTemplate;
    private static ReactiveMongoDatabaseFactory databaseFactory;

    @BeforeAll
    static void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        var client = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        databaseFactory = new SimpleReactiveMongoDatabaseFactory(client, database);
        mongoTemplate = new ReactiveMongoTemplate(client, database);
    }

    private ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentReactiveMongoAutoConfiguration.class))
                .withBean(ReactiveMongoDatabaseFactory.class, () -> databaseFactory)
                .withBean(ReactiveMongoTemplate.class, () -> mongoTemplate)
                .withPropertyValues("occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test");
    }

    @Test
    void wires_the_stream_beans_by_default_and_no_dcb_beans() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(EventStore.class);
            assertThat(context).hasSingleBean(ApplicationService.class);
            assertThat(context).hasSingleBean(DomainEventQueries.class);
            assertThat(context).hasSingleBean(StreamSubscriptions.class);
            assertThat(context).hasSingleBean(Subscribable.class);
            assertThat(context).hasSingleBean(ReactorDurableSubscriptionModel.class);
            assertThat(context).doesNotHaveBean(DcbSubscriptions.class);
            assertThat(context).doesNotHaveBean(DcbDomainEventQueries.class);
        });
    }

    @Test
    void wires_the_dcb_dsl_beans_when_the_dcb_capability_is_enabled() {
        contextRunner().withPropertyValues("occurrent.event-store.capabilities=stream,dcb").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(DcbSubscriptions.class);
            assertThat(context).hasSingleBean(DcbDomainEventQueries.class);
        });
    }

    @Test
    void dcb_capability_auto_configures_dcb_application_service_when_tag_generator_exists() {
        contextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .withBean(TagGenerator.class, () -> tagsForTestEvent())
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(DcbApplicationService.class);
                });
    }

    @Test
    void dcb_capability_auto_configures_dcb_application_service_with_annotation_tag_generator_default_when_no_user_tag_generator_is_defined() {
        // dcb-annotation-taggenerator is an optional starter dependency and therefore present on this module's own
        // test classpath, so with no user-defined TagGenerator bean the auto-configured AnnotationTagGenerator kicks
        // in and the DcbApplicationService is created, unlike before this default existed.
        contextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(DcbApplicationService.class);
                    assertThat(context.getBean(TagGenerator.class)).isInstanceOf(AnnotationTagGenerator.class);
                });
    }

    @Test
    void dcb_capability_auto_configures_dcb_application_service_without_tag_generator_even_when_annotation_tag_generator_module_is_absent() {
        // A global TagGenerator is now optional. A DcbDecider carries the tags for the events it emits, so the service
        // is auto-configured even with no TagGenerator bean and no AnnotationTagGenerator fallback. Decider-less
        // execution with no tagger of any kind fails loudly at append time instead.
        contextRunner()
                .withClassLoader(new FilteredClassLoader(AnnotationTagGenerator.class))
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(TagGenerator.class);
                    assertThat(context).hasSingleBean(DcbApplicationService.class);
                });
    }

    @Test
    void user_defined_tag_generator_takes_precedence_over_the_annotation_tag_generator_default() {
        TagGenerator<TestEvent> userTagGenerator = tagsForTestEvent();

        contextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .withBean(TagGenerator.class, () -> userTagGenerator)
                .run(context -> assertThat(context.getBean(TagGenerator.class)).isSameAs(userTagGenerator));
    }

    @Test
    void custom_dcb_application_service_is_not_replaced() {
        DcbApplicationService customApplicationService = mock(DcbApplicationService.class);

        contextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .withBean(TagGenerator.class, () -> tagsForTestEvent())
                .withBean(DcbApplicationService.class, () -> customApplicationService)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(DcbApplicationService.class)).isSameAs(customApplicationService);
                });
    }

    private static TagGenerator<TestEvent> tagsForTestEvent() {
        return event -> Set.of(Tag.of("eventId", event.eventId()));
    }

    @Test
    void disabling_the_event_store_removes_the_event_store_beans() {
        contextRunner().withPropertyValues("occurrent.event-store.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(EventStore.class);
            assertThat(context).doesNotHaveBean(ApplicationService.class);
            assertThat(context).doesNotHaveBean(DomainEventQueries.class);
        });
    }

    @Test
    void disabling_subscriptions_removes_the_subscription_beans() {
        contextRunner().withPropertyValues("occurrent.subscription.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(StreamSubscriptions.class);
            assertThat(context).doesNotHaveBean(Subscribable.class);
            assertThat(context).doesNotHaveBean(ReactorDurableSubscriptionModel.class);
        });
    }

    @Test
    void a_user_provided_event_store_takes_precedence() {
        // A user override implements the event store, query, and DCB interfaces the same way the real store does, so the
        // query DSL still wires. Mockito bypasses the eager-initializing constructor.
        ReactorMongoEventStore userEventStore = mock(ReactorMongoEventStore.class);
        contextRunner().withBean(ReactorMongoEventStore.class, () -> userEventStore).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(EventStore.class);
            assertThat(context.getBean(EventStore.class)).isSameAs(userEventStore);
        });
    }

    @Test
    void a_stream_subscription_that_replays_history_fails_loud_when_stream_position_is_off() {
        // Stream position is on by default, which makes reactive stream history replay supported. With position
        // explicitly opted out there is no reactive stream history replay path, so a BEGINNING_OF_TIME
        // @StreamSubscription must fail loud rather than silently start live.
        contextRunner()
                .withPropertyValues("occurrent.event-store.stream.position=false")
                .withUserConfiguration(BeginningOfTimeStreamSubscriptionConfiguration.class).run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(IllegalArgumentException.class);
                    assertThat(context.getStartupFailure()).rootCause().hasMessageContaining("does not support reactive stream history replay");
                });
    }

    @Test
    void a_stream_subscription_with_a_specific_start_time_fails_loud_even_when_history_replay_is_supported() {
        // Stream position is on by default, so BEGINNING_OF_TIME would be supported here. A specific start time has
        // no position to map to though, so the reactive stack must fail loud rather than silently replaying all
        // history from position 0, which would ignore the requested start time.
        contextRunner()
                .withUserConfiguration(SpecificTimeStreamSubscriptionConfiguration.class).run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(IllegalArgumentException.class);
                    assertThat(context.getStartupFailure()).rootCause().hasMessageContaining("cannot honor a specific historical start time");
                });
    }

    @Test
    void a_stream_subscription_with_an_explicit_epoch_millis_start_fails_loud() {
        // An epoch-millis start is a specific historical time, same as an ISO8601 one, so it fails loud for the same
        // reason: a wall-clock time has no position to map to.
        contextRunner().withUserConfiguration(EpochMillisStreamSubscriptionConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(IllegalArgumentException.class);
            assertThat(context.getStartupFailure()).rootCause().hasMessageContaining("cannot honor a specific historical start time");
        });
    }

    @Test
    void a_stream_subscription_that_replays_history_fails_loud_on_a_dcb_only_store() {
        // A DCB-only store writes position too (DCB always does), but it has no STREAM capability, so a
        // @StreamSubscription started from the beginning must fail loud instead of being wrongly treated as
        // stream-catchup-capable. This guards the STREAM-capability gate in occurrentReactorCatchupSubscriptionModel
        // and in streamHistoryReplaySupported(): gating on writesPosition() alone would let this case slip through.
        contextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=dcb")
                .withUserConfiguration(BeginningOfTimeStreamSubscriptionConfiguration.class).run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(IllegalArgumentException.class);
                    assertThat(context.getStartupFailure()).rootCause().hasMessageContaining("does not support reactive stream history replay");
                });
    }

    @Test
    void a_combined_stream_and_dcb_store_starts_a_beginning_of_time_stream_subscription_and_replays_history_without_failing() {
        // With both capabilities on, the reactive stack wires the dual-mode ReactorCatchupSubscriptionModel (see
        // OccurrentReactiveMongoAutoConfiguration#occurrentReactorCatchupSubscriptionModel). A @StreamSubscription
        // carries a StreamSubscriptionFilter, which the routing fix always sends to the stream inner model, so
        // BEGINNING_OF_TIME must replay rather than fail loud like the position-off/specific-time scenarios above.
        contextRunner()
                .withPropertyValues("occurrent.event-store.capabilities=stream,dcb")
                .withUserConfiguration(BeginningOfTimeStreamSubscriptionConfiguration.class).run(context -> {
                    assertThat(context).hasNotFailed();

                    ApplicationService<TestEvent> applicationService = context.getBean(ApplicationService.class);
                    TestEvent historic = new TestEvent(UUID.randomUUID().toString());
                    applicationService.execute(UUID.randomUUID().toString(), events -> List.of(historic)).block();

                    ReplayingListener replayingListener = context.getBean(ReplayingListener.class);
                    await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                            assertThat(replayingListener.received()).contains(historic.eventId()));
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class BeginningOfTimeStreamSubscriptionConfiguration {
        @Bean
        ReplayingListener replayingListener() {
            return new ReplayingListener();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SpecificTimeStreamSubscriptionConfiguration {
        @Bean
        SpecificTimeListener specificTimeListener() {
            return new SpecificTimeListener();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EpochMillisStreamSubscriptionConfiguration {
        @Bean
        EpochMillisListener epochMillisListener() {
            return new EpochMillisListener();
        }
    }

    static class ReplayingListener {
        private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "replaying", startAt = StartPosition.BEGINNING_OF_TIME)
        Mono<Void> on(TestEvent event) {
            received.add(event.eventId());
            return Mono.empty();
        }

        List<String> received() {
            return received;
        }
    }

    static class SpecificTimeListener {
        @StreamSubscription(id = "specificTime", startAtISO8601 = "2024-01-01T00:00:00Z")
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    static class EpochMillisListener {
        @StreamSubscription(id = "epochMillis", startAtTimeEpochMillis = 946684800000L)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    record TestEvent(String eventId) {
    }
}
