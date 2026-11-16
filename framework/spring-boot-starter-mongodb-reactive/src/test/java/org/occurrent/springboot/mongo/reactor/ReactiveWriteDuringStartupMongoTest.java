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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import jakarta.annotation.PostConstruct;
import org.jspecify.annotations.Nullable;
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
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanCurrentlyInCreationException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The reactive counterpart of {@code WriteDuringStartupMongoTest}: a write made while the application starts, from a
 * {@code @PostConstruct} method, reaches the non-lazy synchronous handlers that the bean definitions show, and fails
 * with {@code BeanCurrentlyInCreationException}, storing nothing, when a handler's bean is still being created.
 * <p>
 * Each scenario boots its own application with {@code SpringApplication.run}, on a database of its own.
 */
@DisplayName("A reactive write made while the application starts")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveWriteDuringStartupMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-write-during-startup");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void an_application_with_no_synchronous_handler_can_write_from_a_post_construct_method() {
        String database = "zero-synchronous";

        try (ConfigurableApplicationContext context = bootSuccessfully(ZeroSynchronousApplication.class, database)) {
            assertThat(context.getBean(StartupWriter.class).written).as("the writer's write went through").isTrue();
            assertThat(storedEvents(database)).as("events stored").isEqualTo(1);
        }
    }

    @Test
    void a_write_from_a_post_construct_method_reaches_a_synchronous_handler_on_a_bean_created_after_the_writer() {
        try (ConfigurableApplicationContext context = bootSuccessfully(ReceiverAfterWriterApplication.class, "receiver-after-writer")) {
            SynchronousReceiver receiver = context.getBean(SynchronousReceiver.class);

            assertThat(receiver.received()).extracting(TestEvent::name).containsExactly("written-at-startup");
        }
    }

    @Test
    void a_lazy_synchronous_handler_bean_is_not_created_by_the_write_and_does_not_see_it() {
        try (ConfigurableApplicationContext context = bootSuccessfully(LazyReceiverApplication.class, "lazy-receiver")) {
            StartupWriter writer = context.getBean(StartupWriter.class);
            LazySynchronousReceiver receiver = context.getBean(LazySynchronousReceiver.class);

            assertThat(writer.created).as("beans the writer found created right after its write").doesNotContain("lazyReceiver");
            assertThat(receiver.received()).as("what the lazy handler received").isEmpty();
        }
    }

    @Test
    void a_write_made_by_a_bean_that_declares_a_synchronous_handler_itself_fails_startup_and_stores_nothing() {
        String database = "writer-with-handler";

        Throwable thrown = catchThrowable(() -> boot(WriterWithItsOwnHandlerApplication.class, database).close());

        assertThat(causeChainOf(thrown)).as("the failure of startup and what caused it").anyMatch(BeanCurrentlyInCreationException.class::isInstance);
        assertThat(storedEvents(database)).as("events stored").isZero();
    }

    private static List<Throwable> causeChainOf(@Nullable Throwable thrown) {
        assertThat(thrown).as("what booting threw").isNotNull().isInstanceOf(BeanCreationException.class);
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

    // Writes while it is being created, which is before the startup scan has registered anything
    static class StartupWriter {
        private final ApplicationService<TestEvent> applicationService;
        private final BeanFactory beanFactory;
        volatile boolean written;
        // Which of the lazy bean's names the container holds a singleton for, right after the write
        final List<String> created = new CopyOnWriteArrayList<>();

        StartupWriter(ApplicationService<TestEvent> applicationService, BeanFactory beanFactory) {
            this.applicationService = applicationService;
            this.beanFactory = beanFactory;
        }

        @PostConstruct
        void write() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-at-startup"))).block();
            written = true;
            if (((ConfigurableBeanFactory) beanFactory).containsSingleton("lazyReceiver")) {
                created.add("lazyReceiver");
            }
        }
    }

    static class SynchronousReceiver {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "reactiveSynchronousReceiver")
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class LazySynchronousReceiver {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "reactiveLazySynchronousReceiver")
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
    }

    // Its own handler is one the write needs registered, and the bean is the one being created
    static class WriterWithItsOwnHandler {
        private final ApplicationService<TestEvent> applicationService;

        WriterWithItsOwnHandler(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void write() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-at-startup"))).block();
        }

        @SynchronousSubscription(id = "reactiveWriterSynchronousHandler")
        Mono<Void> on(Written event) {
            return Mono.empty();
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
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class ZeroSynchronousApplication {

        @Bean
        StartupWriter startupWriter(ApplicationService<TestEvent> applicationService, BeanFactory beanFactory) {
            return new StartupWriter(applicationService, beanFactory);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class ReceiverAfterWriterApplication {

        @Bean
        StartupWriter startupWriter(ApplicationService<TestEvent> applicationService, BeanFactory beanFactory) {
            return new StartupWriter(applicationService, beanFactory);
        }

        @Bean
        SynchronousReceiver synchronousReceiver() {
            return new SynchronousReceiver();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class LazyReceiverApplication {

        @Bean
        StartupWriter startupWriter(ApplicationService<TestEvent> applicationService, BeanFactory beanFactory) {
            return new StartupWriter(applicationService, beanFactory);
        }

        // Declared as its own type, which the startup scan reads a handler from. It is built there, after the write.
        @Bean
        @Lazy
        LazySynchronousReceiver lazyReceiver() {
            return new LazySynchronousReceiver();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class WriterWithItsOwnHandlerApplication {

        @Bean
        WriterWithItsOwnHandler writerWithItsOwnHandler(ApplicationService<TestEvent> applicationService) {
            return new WriterWithItsOwnHandler(applicationService);
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
