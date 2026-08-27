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

package org.occurrent.springboot.mongo.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Saga;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.SagaInstancesRegistry;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Verifies {@link SagaInstancesRegistry} with two {@code @Saga} beans registered in one real application context, the
 * gap {@link SagaAnnotationValidationTest} (each scenario lives in its own deliberately-failing context) and
 * {@link SagaAnnotationMongoTest} (a single saga) leave uncovered: does a second saga collide with the first, and do the
 * two really resolve to independent instances. Docker-based.
 */
@DisplayName("SagaInstancesRegistry (two @Saga beans in one context)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = SagaInstancesRegistryMongoTest.TwoSagaApplication.class,
        properties = "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:saga-instances-registry-test"
)
@Import(SagaInstancesRegistryMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(90)
class SagaInstancesRegistryMongoTest {

    @Autowired
    private ApplicationService<TrackedEvent> applicationService;
    @Autowired
    private SagaInstancesRegistry sagaInstancesRegistry;
    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void each_saga_lands_in_the_registry_and_its_own_named_singleton_and_the_two_are_genuinely_distinct() {
        applicationService.execute("a-1", events -> List.of(new AStarted("a-1")));
        applicationService.execute("b-1", events -> List.of(new BStarted("b-1")));

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(sagaInstancesRegistry.get("saga-a").find("a-1")).isPresent();
            assertThat(sagaInstancesRegistry.get("saga-b").find("b-1")).isPresent();
        });

        SagaInstances sagaAInstances = sagaInstancesRegistry.get("saga-a");
        SagaInstances sagaBInstances = sagaInstancesRegistry.get("saga-b");
        assertAll(
                () -> assertThat(sagaAInstances).isNotSameAs(sagaBInstances),
                () -> assertThat(sagaAInstances.find("b-1")).as("saga-a's store never saw saga-b's instance").isEmpty(),
                () -> assertThat(sagaBInstances.find("a-1")).as("saga-b's store never saw saga-a's instance").isEmpty(),
                () -> assertThat(applicationContext.getBean("sagaInstances-saga-a", SagaInstances.class)).isSameAs(sagaAInstances),
                () -> assertThat(applicationContext.getBean("sagaInstances-saga-b", SagaInstances.class)).isSameAs(sagaBInstances)
        );
    }

    @Test
    void each_saga_also_publishes_its_running_subscription_under_its_own_name() {
        SagaSubscription sagaA = applicationContext.getBean("sagaSubscription-saga-a", SagaSubscription.class);
        SagaSubscription sagaB = applicationContext.getBean("sagaSubscription-saga-b", SagaSubscription.class);

        assertAll(
                () -> assertThat(sagaA.id()).isEqualTo("saga-a"),
                () -> assertThat(sagaB.id()).isEqualTo("saga-b"),
                // The published bean answers with the same SagaInstances the registry holds, so an application on the
                // annotation path reaches one saga's instances either way.
                () -> assertThat(sagaA.instances()).isSameAs(sagaInstancesRegistry.get("saga-a")),
                () -> assertThat(sagaB.instances()).isSameAs(sagaInstancesRegistry.get("saga-b"))
        );
    }

    @Test
    void sagaIds_enumerates_every_registered_saga() {
        assertThat(sagaInstancesRegistry.sagaIds()).containsExactlyInAnyOrder("saga-a", "saga-b");
    }

    @Test
    void find_returns_empty_for_an_unknown_saga_id() {
        assertThat(sagaInstancesRegistry.find("unknown-saga")).isEmpty();
    }

    @Test
    void get_throws_for_an_unknown_saga_id_listing_the_registered_ids() {
        assertThatThrownBy(() -> sagaInstancesRegistry.get("unknown-saga"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No saga is registered with id 'unknown-saga'")
                .hasMessageContaining("saga-a")
                .hasMessageContaining("saga-b");
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class TwoSagaApplication {
        @Bean
        CloudEventTypeMapper<TrackedEvent> trackedEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TrackedEvent> trackedEventCloudEventConverter(CloudEventTypeMapper<TrackedEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TrackedEvent>(new ObjectMapper(), URI.create("urn:occurrent:saga-instances-registry-test"))
                    .typeMapper(typeMapper)
                    .idMapper(TrackedEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        // Shared by both sagas: neither issues a command (no react(...) registered), so this exists only to satisfy the
        // "a CommandDispatcher bean is required" resolution and is never invoked.
        @Bean
        CommandDispatcher<Object> noopCommandDispatcher() {
            return command -> {
            };
        }

        static class SagaBeans {
            @Saga(id = "saga-a")
            org.occurrent.dsl.saga.Saga<TrackedEvent, AState, Object> sagaA() {
                return org.occurrent.dsl.saga.Saga.<TrackedEvent, AState, Object>builder(new AState("new"))
                        .correlateAll(TrackedEvent::id)
                        .startsOn(AStarted.class)
                        .evolve(AStarted.class, (state, e) -> new AState("started"))
                        .build();
            }

            @Saga(id = "saga-b")
            org.occurrent.dsl.saga.Saga<TrackedEvent, BState, Object> sagaB() {
                return org.occurrent.dsl.saga.Saga.<TrackedEvent, BState, Object>builder(new BState("new"))
                        .correlateAll(TrackedEvent::id)
                        .startsOn(BStarted.class)
                        .evolve(BStarted.class, (state, e) -> new BState("started"))
                        .build();
            }
        }

        @Bean
        SagaBeans sagaBeans() {
            return new SagaBeans();
        }
    }

    record AState(String value) {
    }

    record BState(String value) {
    }

    sealed interface TrackedEvent {
        String id();

        String eventId();

        Date timestamp();
    }

    record AStarted(String id, String eventId, Date timestamp) implements TrackedEvent {
        AStarted(String id) {
            this(id, UUID.randomUUID().toString(), new Date());
        }
    }

    record BStarted(String id, String eventId, Date timestamp) implements TrackedEvent {
        BStarted(String id) {
            this(id, UUID.randomUUID().toString(), new Date());
        }
    }
}
