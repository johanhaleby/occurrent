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

package org.occurrent.springboot.mongo.reactor;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A checkpoint storage written for 0.32.0 and brought to 0.33.0 with the migration guide's stub writes only
 * unconditionally, and refuses {@code ifAbsent()} with {@link UnsupportedOperationException}. A subscription at
 * {@code DEFAULT} with nothing stored pins its start boundary as the first checkpoint, and with such a storage it
 * does so unconditionally rather than failing startup.
 */
@DisplayName("A reactive checkpoint storage that only writes unconditionally")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveUnconditionalCheckpointStorageMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-unconditional-checkpoint-storage");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void starts_the_application_pins_the_first_checkpoint_of_a_default_subscription_and_the_subscription_gets_what_is_written_once_started() {
        ConfigurableApplicationContext[] started = new ConfigurableApplicationContext[1];
        Throwable thrown = catchThrowable(() -> started[0] = SpringApplication.run(new Class<?>[]{UnconditionalStorageConfiguration.class, UnconditionalStorageApplication.class}, new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("reactive-unconditional-checkpoint-storage"),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.subscription.competing-consumer.fence-checkpoints=false",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE}));
        assertThat(thrown).as("what starting the application threw").isNull();

        try (ConfigurableApplicationContext context = started[0]) {
            UnconditionalCheckpointStorage storage = context.getBean(UnconditionalCheckpointStorage.class);
            assertThat(storage.checkpoints).as("checkpoints stored once the application started").containsKey("reactiveDefaultReceiver");

            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-once-started"))).block();

            DefaultReceiver receiver = context.getBean(DefaultReceiver.class);
            try {
                await().atMost(ofSeconds(15)).pollInterval(ofMillis(100)).until(() -> !receiver.received.isEmpty());
            } catch (ConditionTimeoutException ignored) {
                // reported by the assertion that follows
            }
            assertThat(receiver.received).extracting(TestEvent::name).containsExactly("written-once-started");
        }
    }

    @SuppressWarnings("unchecked")
    private static ApplicationService<TestEvent> applicationServiceOf(ConfigurableApplicationContext context) {
        return context.getBean(ApplicationService.class);
    }

    // The 0.33.0 migration guide's stub over an existing unconditional two-argument save
    static class UnconditionalCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.fromSupplier(() -> checkpoints.get(subscriptionId));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
            return Mono.fromSupplier(() -> {
                checkpoints.put(subscriptionId, checkpoint);
                return checkpoint;
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (!(condition instanceof CheckpointWriteCondition.Any)) {
                return Mono.error(new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported."));
            }
            return save(subscriptionId, checkpoint);
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

    static class DefaultReceiver {
        final List<TestEvent> received = new CopyOnWriteArrayList<>();

        @Subscription(id = "reactiveDefaultReceiver", startAt = StartPosition.DEFAULT)
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    // Registered ahead of the application, since @EnableOccurrentReactive imports the starter's configuration and its
    // checkpoint storage only steps aside for a bean defined before it
    @Configuration(proxyBeanMethods = false)
    static class UnconditionalStorageConfiguration {

        @Bean
        UnconditionalCheckpointStorage unconditionalCheckpointStorage() {
            return new UnconditionalCheckpointStorage();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    static class UnconditionalStorageApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        DefaultReceiver defaultReceiver() {
            return new DefaultReceiver();
        }
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Written(String eventId, Date timestamp, String name) implements TestEvent {
        Written(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
