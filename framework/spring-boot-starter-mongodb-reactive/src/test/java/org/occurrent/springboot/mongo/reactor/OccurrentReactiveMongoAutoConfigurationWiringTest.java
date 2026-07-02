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
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
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
    void a_stream_subscription_that_replays_history_fails_loud() {
        contextRunner().withUserConfiguration(BeginningOfTimeStreamSubscriptionConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(IllegalArgumentException.class);
            assertThat(context.getStartupFailure()).rootCause().hasMessageContaining("no stream catch-up");
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class BeginningOfTimeStreamSubscriptionConfiguration {
        @Bean
        ReplayingListener replayingListener() {
            return new ReplayingListener();
        }
    }

    static class ReplayingListener {
        @StreamSubscription(id = "replaying", startAt = StartPosition.BEGINNING_OF_TIME)
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    record TestEvent(String eventId) {
    }
}
