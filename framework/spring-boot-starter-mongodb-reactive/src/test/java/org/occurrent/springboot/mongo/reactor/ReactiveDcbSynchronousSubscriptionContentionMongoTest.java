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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive counterpart of {@code DcbSynchronousSubscriptionContentionMongoTest}. With a synchronous subscription
 * registered, the reactive DCB application service runs every command through a {@code ReactiveTransactionExecutor},
 * so the event store joins a transaction it did not open and stops retrying a write conflict itself (ADR 0074).
 * <p>
 * Inside that transaction the service appends, then re-reads the block it just appended to dispatch it. The position
 * counter is reserved outside the transaction, so the transaction's snapshot holds a counter below its own events, and
 * the re-read has to reach them anyway. Every command appends an event of the same type, so concurrent commands share
 * the type's conflict marker and collide on it, and the executor has to retry them.
 */
@DisplayName("Reactive DCB commands with a synchronous subscription under contention")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveDcbSynchronousSubscriptionContentionMongoTest.DcbContentionApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-dcb-synchronous-contention-test"
        }
)
@Import(ReactiveDcbSynchronousSubscriptionContentionMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(120)
class ReactiveDcbSynchronousSubscriptionContentionMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-dcb-synchronous-contention-test");

    @Autowired
    private DcbApplicationService<TestEvent> dcbApplicationService;

    @Autowired
    private CountingSubscriber countingSubscriber;

    @BeforeEach
    void forget_earlier_deliveries() {
        countingSubscriber.reset();
    }

    @Test
    void the_handler_sees_the_event_a_command_appended_inside_its_transaction() {
        String subject = "single-" + UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();

        dcbApplicationService.execute(DcbCriteria.tags(Tag.parse(subject)), __ -> List.of(new TestEvent(eventId, new Date(), "single", subject))).block();

        assertThat(countingSubscriber.deliveries()).containsExactly(Map.entry(eventId, 1));
    }

    @Test
    void every_command_commits_when_the_executor_owns_the_transaction() {
        int commandCount = 6;
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<String> eventIds = new ArrayList<>();
        List<Mono<?>> commands = new ArrayList<>();
        for (int i = 0; i < commandCount; i++) {
            String subject = "contended-" + UUID.randomUUID();
            String eventId = UUID.randomUUID().toString();
            eventIds.add(eventId);
            commands.add(dcbApplicationService.execute(DcbCriteria.tags(Tag.parse(subject)), __ -> List.of(new TestEvent(eventId, new Date(), "contended", subject)))
                    .doOnError(failures::add)
                    .onErrorResume(__ -> Mono.empty()));
        }

        Flux.merge(commands).blockLast();

        assertThat(failures)
                .as("Every command must commit. A conflict on the shared type marker has to be retried by the executor, because the store joins the transaction the executor opened and cannot retry it")
                .isEmpty();
        assertThat(countingSubscriber.deliveries().keySet())
                .as("The synchronous handler must see every committed event")
                .containsAll(eventIds);
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
    @EnableOccurrentReactive
    static class DcbContentionApplication {

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
        TagGenerator<TestEvent> testEventTagGenerator() {
            return event -> Set.of(Tag.parse(event.subject()));
        }

        @Bean
        CountingSubscriber countingSubscriber() {
            return new CountingSubscriber();
        }
    }

    /**
     * Registering this makes the application service open a transaction per command, which pushes the retry up from the
     * store to the executor.
     */
    static class CountingSubscriber {
        private final Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();

        @SynchronousSubscription(id = "reactive-dcb-contention-counting-subscriber")
        Mono<Void> on(TestEvent event) {
            deliveries.computeIfAbsent(event.eventId(), __ -> new AtomicInteger()).incrementAndGet();
            return Mono.empty();
        }

        void reset() {
            deliveries.clear();
        }

        Map<String, Integer> deliveries() {
            Map<String, Integer> copy = new TreeMap<>();
            deliveries.forEach((eventId, count) -> copy.put(eventId, count.get()));
            return copy;
        }
    }

    record TestEvent(String eventId, Date timestamp, String name, String subject) {
    }
}
