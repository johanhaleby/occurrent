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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import jakarta.annotation.PostConstruct;
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
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A write through the application service is refused while the application starts, until the synchronous handlers
 * are registered, since a handler that is not registered yet would never see it. The refusal is a
 * {@link WriteRefusedException} that fails startup and names where such a write belongs, and nothing is stored.
 * A write from an {@code ApplicationRunner} or an {@code ApplicationReadyEvent} listener goes through and reaches the
 * synchronous handlers, and a write straight to the event store is not refused, since it never dispatches to a handler.
 * <p>
 * Each scenario boots its own application with {@code SpringApplication.run}, on a database of its own.
 */
@DisplayName("A write through the application service while the application starts")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class WriteDuringStartupMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:write-during-startup");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_write_from_a_post_construct_method_fails_startup_and_stores_nothing() {
        String database = "post-construct-with-handler";

        Throwable thrown = catchThrowable(() -> boot(ReceiverAfterWriterApplication.class, database).close());

        assertStartupWasRefused(thrown);
        assertThat(storedEvents(database)).as("events stored").isZero();
    }

    @Test
    void a_write_from_a_post_construct_method_is_refused_when_no_synchronous_handler_is_declared() {
        String database = "zero-synchronous";

        Throwable thrown = catchThrowable(() -> boot(ZeroSynchronousApplication.class, database).close());

        assertStartupWasRefused(thrown);
        assertThat(storedEvents(database)).as("events stored").isZero();
    }

    @Test
    void a_synchronous_handler_bean_that_writes_from_its_own_post_construct_method_is_refused() {
        String database = "writer-with-handler";

        Throwable thrown = catchThrowable(() -> boot(WriterWithItsOwnHandlerApplication.class, database).close());

        assertStartupWasRefused(thrown);
        assertThat(storedEvents(database)).as("events stored").isZero();
    }

    @Test
    void a_write_from_an_application_runner_goes_through_and_reaches_a_synchronous_handler() {
        String database = "runner-writer";

        try (ConfigurableApplicationContext context = bootSuccessfully(RunnerWriterApplication.class, database)) {
            assertThat(context.getBean(SynchronousReceiver.class).received()).extracting(TestEvent::name).containsExactly("written-by-runner");
            assertThat(storedEvents(database)).as("events stored").isEqualTo(1);
        }
    }

    @Test
    void a_write_from_an_application_ready_event_listener_goes_through_and_reaches_a_synchronous_handler() {
        String database = "ready-listener-writer";

        try (ConfigurableApplicationContext context = bootSuccessfully(ReadyListenerWriterApplication.class, database)) {
            assertThat(context.getBean(SynchronousReceiver.class).received()).extracting(TestEvent::name).containsExactly("written-on-ready");
            assertThat(storedEvents(database)).as("events stored").isEqualTo(1);
        }
    }

    @Test
    void a_write_straight_to_the_event_store_from_a_post_construct_method_is_not_refused() {
        String database = "direct-store-write";

        try (ConfigurableApplicationContext context = bootSuccessfully(DirectStoreWriterApplication.class, database)) {
            assertThat(context.getBean(DirectStoreWriter.class).written).as("the writer's write went through").isTrue();
            assertThat(storedEvents(database)).as("events stored").isEqualTo(1);
        }
    }

    @Test
    void a_write_from_a_second_thread_started_during_startup_is_refused_and_the_same_write_goes_through_once_started() {
        String database = "second-thread-writer";

        try (ConfigurableApplicationContext context = bootSuccessfully(SecondThreadWriterApplication.class, database)) {
            Throwable refused = context.getBean(SecondThreadWriter.class).outcome.get();

            assertThat(refused).as("what the second thread's write threw while the application started").isNotNull();
            assertThat(causeChainOf(refused)).as("what caused it").anyMatch(WriteRefusedException.class::isInstance);
            assertThat(storedEvents(database)).as("events stored after the refusal").isZero();

            context.getBean(ApplicationService.class).execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-once-started")));

            assertThat(storedEvents(database)).as("events stored once started").isEqualTo(1);
        }
    }

    private static void assertStartupWasRefused(Throwable thrown) {
        assertThat(thrown).as("what booting threw").isNotNull();
        List<Throwable> chain = causeChainOf(thrown);
        assertThat(chain).as("the failure of startup and what caused it").anyMatch(WriteRefusedException.class::isInstance);
        Throwable refusal = chain.stream().filter(WriteRefusedException.class::isInstance).findFirst().orElseThrow();
        assertThat(refusal.getMessage()).as("where the refusal says such a write belongs").contains("ApplicationRunner").contains("ApplicationReadyEvent");
    }

    private static List<Throwable> causeChainOf(Throwable thrown) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable t = thrown; t != null && !chain.contains(t); t = t.getCause()) {
            chain.add(t);
        }
        return chain;
    }

    private static long storedEvents(String database) {
        // The container prefixes the database name, so the name the application uses is read from the url
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(database));
        try (MongoClient client = MongoClients.create(connectionString)) {
            return client.getDatabase(requireNonNull(connectionString.getDatabase())).getCollection("events").countDocuments();
        }
    }

    private static ConfigurableApplicationContext bootSuccessfully(Class<?> application, String database) {
        ConfigurableApplicationContext[] started = new ConfigurableApplicationContext[1];
        Throwable thrown = catchThrowable(() -> started[0] = boot(application, database));
        assertThat(thrown).as("what starting " + application.getSimpleName() + " threw").isNull();
        return started[0];
    }

    private static ConfigurableApplicationContext boot(Class<?> application, String database) {
        return SpringApplication.run(application,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    // Writes through the application service while it is being created, which is before the startup scan has registered anything
    static class StartupWriter {
        private final ApplicationService<TestEvent> applicationService;

        StartupWriter(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void write() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-at-startup")));
        }
    }

    // Starts a thread that writes and waits for it, so the write is made before the application has started
    static class SecondThreadWriter {
        private final ApplicationService<TestEvent> applicationService;
        final AtomicReference<Throwable> outcome = new AtomicReference<>();

        SecondThreadWriter(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void write() throws InterruptedException {
            Thread thread = new Thread(() -> outcome.set(catchThrowable(() ->
                    applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-second-thread"))))));
            thread.start();
            thread.join();
        }
    }

    static class DirectStoreWriter {
        private final EventStore eventStore;
        private final CloudEventConverter<TestEvent> converter;
        volatile boolean written;

        DirectStoreWriter(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            this.eventStore = eventStore;
            this.converter = converter;
        }

        @PostConstruct
        void write() {
            eventStore.write(UUID.randomUUID().toString(), List.of(converter.toCloudEvent(new Written("written-straight-to-the-store"))));
            written = true;
        }
    }

    static class ReadyListenerWriter {
        private final ApplicationService<TestEvent> applicationService;

        ReadyListenerWriter(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @EventListener(ApplicationReadyEvent.class)
        void write() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-on-ready")));
        }
    }

    static class SynchronousReceiver {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "synchronousReceiver")
        void on(Written event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    // Its own handler is one the write needs registered, and the bean is the one being created
    static class WriterWithItsOwnHandler {
        WriterWithItsOwnHandler(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        private final ApplicationService<TestEvent> applicationService;

        @PostConstruct
        void write() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-at-startup")));
        }

        @SynchronousSubscription(id = "writerSynchronousHandler")
        void on(Written event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EventConverters {

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
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class ZeroSynchronousApplication {

        @Bean
        StartupWriter startupWriter(ApplicationService<TestEvent> applicationService) {
            return new StartupWriter(applicationService);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class ReceiverAfterWriterApplication {

        @Bean
        StartupWriter startupWriter(ApplicationService<TestEvent> applicationService) {
            return new StartupWriter(applicationService);
        }

        @Bean
        SynchronousReceiver synchronousReceiver() {
            return new SynchronousReceiver();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class WriterWithItsOwnHandlerApplication {

        @Bean
        WriterWithItsOwnHandler writerWithItsOwnHandler(ApplicationService<TestEvent> applicationService) {
            return new WriterWithItsOwnHandler(applicationService);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class RunnerWriterApplication {

        @Bean
        SynchronousReceiver synchronousReceiver() {
            return new SynchronousReceiver();
        }

        @Bean
        ApplicationRunner runnerWriter(ApplicationService<TestEvent> applicationService) {
            return args -> applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-runner")));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class ReadyListenerWriterApplication {

        @Bean
        SynchronousReceiver synchronousReceiver() {
            return new SynchronousReceiver();
        }

        @Bean
        ReadyListenerWriter readyListenerWriter(ApplicationService<TestEvent> applicationService) {
            return new ReadyListenerWriter(applicationService);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class DirectStoreWriterApplication {

        @Bean
        SynchronousReceiver synchronousReceiver() {
            return new SynchronousReceiver();
        }

        @Bean
        DirectStoreWriter directStoreWriter(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            return new DirectStoreWriter(eventStore, converter);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class SecondThreadWriterApplication {

        @Bean
        SynchronousReceiver synchronousReceiver() {
            return new SynchronousReceiver();
        }

        @Bean
        SecondThreadWriter secondThreadWriter(ApplicationService<TestEvent> applicationService) {
            return new SecondThreadWriter(applicationService);
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
