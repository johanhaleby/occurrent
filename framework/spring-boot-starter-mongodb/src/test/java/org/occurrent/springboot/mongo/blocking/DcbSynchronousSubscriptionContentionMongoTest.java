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
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With a synchronous subscription registered, the DCB application service runs every command through a
 * {@code TransactionExecutor}, so the event store always joins a transaction it did not open and therefore stops
 * retrying a write conflict itself (ADR 0074). The retry has to come from the executor instead, and this is the only
 * test covering that combination against a real MongoDB transaction: the other synchronous-subscription tests use the
 * in-memory store, where none of this applies.
 * <p>
 * Contention is guaranteed rather than engineered, because every append increments the one global position counter
 * document, so concurrent commands collide there whatever boundary they target. Remove the retry from
 * {@code SpringTransactionExecutor} and this test fails.
 */
@DisplayName("DCB commands with a synchronous subscription under contention")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbSynchronousSubscriptionContentionMongoTest.DcbContentionApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dcb-synchronous-contention-test"
        }
)
@Import(DcbSynchronousSubscriptionContentionMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(120)
class DcbSynchronousSubscriptionContentionMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:dcb-synchronous-contention-test");

    @Autowired
    private DcbApplicationService<TestEvent> dcbApplicationService;

    @Autowired
    private CountingSubscriber countingSubscriber;

    @Test
    void every_command_commits_when_the_executor_owns_the_transaction() throws Exception {
        int threadCount = 6;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Future<Void>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final String subject = "contended-" + UUID.randomUUID();
            futures.add(pool.submit(() -> {
                barrier.await();
                try {
                    dcbApplicationService.execute(DcbCriteria.tags(Tag.parse(subject)),
                            __ -> List.of(new TestEvent(UUID.randomUUID().toString(), new Date(), "contended", subject)));
                } catch (Throwable e) {
                    failures.add(e);
                }
                return null;
            }));
        }

        pool.shutdown();
        for (Future<Void> f : futures) {
            f.get();
        }

        assertThat(failures)
                .as("Every command must commit. A conflict on the shared position counter has to be retried by the executor, because the store joins the transaction the executor opened and cannot retry it")
                .isEmpty();
        assertThat(countingSubscriber.count())
                .as("The synchronous handler must see every committed event at least once")
                .isGreaterThanOrEqualTo(threadCount);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
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
     * Registering this is the whole point: it makes the application service open a transaction per command, which is
     * what pushes the retry up from the store to the executor.
     */
    static class CountingSubscriber {
        private final AtomicInteger count = new AtomicInteger();

        @SynchronousSubscription(id = "dcb-contention-counting-subscriber")
        void on(TestEvent event) {
            count.incrementAndGet();
        }

        int count() {
            return count.get();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name, String subject) {
    }
}
