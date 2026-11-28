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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.BeanCreationException;
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
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The reactive counterpart of {@code SynchronousHandlersRegisterFirstMongoTest}: a synchronous handler is registered
 * before every other handler at startup, so a write made while the startup scan is still registering reaches it. There
 * is no saga scenario here, since the reactive starter has no saga registrar.
 * <p>
 * In every scenario the handler that writes is declared on a bean that comes before the bean with the synchronous
 * handler, so a scan that registered in bean order would register the writer first. A replay with
 * {@code startupMode = WAIT_UNTIL_STARTED} runs to its end before the scan moves on, which makes the write happen
 * during registration rather than somewhere near it.
 * <p>
 * Each scenario boots its own application with {@code SpringApplication.run}, on a database of its own, after another
 * boot wrote the event the replay reads.
 */
@DisplayName("Reactive synchronous handlers register before the rest")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveSynchronousHandlersRegisterFirstMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-synchronous-handlers-register-first");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_write_made_by_a_replay_at_startup_reaches_a_synchronous_handler_declared_on_a_later_bean() {
        String database = "replay-write";
        seed(database, new Seeded("seeded"));

        try (ConfigurableApplicationContext context = boot(ReplayWritesApplication.class, database)) {
            SynchronousReceiver receiver = context.getBean(SynchronousReceiver.class);

            assertThat(receiver.received()).extracting(TestEvent::name).containsExactly("written-by-the-replay");
        }
    }

    @Test
    void a_write_made_by_a_replay_at_startup_reaches_the_synchronous_handler_of_a_lazy_collaborator_that_the_replaying_bean_pulled_in() {
        String database = "lazy-collaborator";
        seed(database, new Seeded("seeded"));

        try (ConfigurableApplicationContext context = boot(LazyCollaboratorApplication.class, database)) {
            LazyCollaborator collaborator = (LazyCollaborator) context.getBean("lazyCollaborator");

            assertThat(collaborator.received()).extracting(TestEvent::name).containsExactly("written-by-the-replay");
        }
    }

    @Test
    void a_lazy_bean_with_a_synchronous_handler_is_refused_when_it_is_built_after_the_application_started() {
        try (ConfigurableApplicationContext context = boot(LazyReceiverApplication.class, "lazy-receiver")) {
            Throwable refusal = catchThrowable(() -> context.getBean("lazySynchronousReceiver"));

            assertThat(refusal).as("[getBean of the lazy bean after startup]").isInstanceOf(BeanCreationException.class);
            assertThat(refusal.getCause()).as("[cause of the refused creation]").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Bean 'lazySynchronousReceiver' declares the synchronous handler")
                    .hasMessageContaining("is created after the startup scan of the Spring Boot starter registered the synchronous handlers");
        }
    }

    @Test
    void a_lazy_bean_with_a_replay_and_a_synchronous_projection_is_refused_when_it_is_built_after_the_application_started() {
        try (ConfigurableApplicationContext context = boot(LazyReplayWritesApplication.class, "lazy-replay-write")) {
            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new Seeded("seeded"))).block();

            Throwable refusal = catchThrowable(() -> context.getBean("lazyReplayWriter"));

            assertThat(refusal).as("[getBean of the lazy bean after startup]").isInstanceOf(BeanCreationException.class);
            assertThat(refusal.getCause()).as("[cause of the refused creation]").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Bean 'lazyReplayWriter' declares the synchronous handler")
                    .hasMessageContaining("is created after the startup scan of the Spring Boot starter registered the synchronous handlers");
        }
    }

    private static void seed(String database, TestEvent event) {
        try (ConfigurableApplicationContext context = boot(SeedApplication.class, database)) {
            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(event)).block();
        }
    }

    @SuppressWarnings("unchecked")
    private static ApplicationService<TestEvent> applicationServiceOf(ConfigurableApplicationContext context) {
        return context.getBean(ApplicationService.class);
    }

    private static ConfigurableApplicationContext boot(Class<?> application, String database) {
        return SpringApplication.run(application,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
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

    static class ReplayWriter {
        private final ApplicationService<TestEvent> applicationService;

        ReplayWriter(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @StreamSubscription(id = "reactiveReplayWriter", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        Mono<Void> on(Seeded event) {
            return applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-the-replay"))).then();
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

    // The replay is a stream subscription, which a scan that registers every handler at once registers before the
    // synchronous projection below. Only a scan of the synchronous handlers first lets the projection see the write
    // the replay makes.
    static class LazyReplayWriter {
        final CountDownLatch replayHasWritten = new CountDownLatch(1);
        private final ApplicationService<TestEvent> applicationService;

        LazyReplayWriter(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @StreamSubscription(id = "reactiveLazyReplayWriter", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        Mono<Void> on(Seeded event) {
            return applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-the-lazy-replay")))
                    .doOnSuccess(__ -> replayHasWritten.countDown())
                    .then();
        }

        @Projection(id = "reactiveWrittenCounter", mode = org.occurrent.annotation.Mode.SYNCHRONOUS)
        org.occurrent.dsl.projection.Projection<WrittenCount, TestEvent, String> writtenCounter() {
            return org.occurrent.dsl.projection.Projection.<WrittenCount, TestEvent, String>builder(new WrittenCount(0))
                    .id(event -> "count")
                    .on(Written.class, (state, event) -> new WrittenCount(state.count() + 1))
                    .build();
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
    static class SeedApplication {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class ReplayWritesApplication {

        @Bean
        ReplayWriter replayWriter(ApplicationService<TestEvent> applicationService) {
            return new ReplayWriter(applicationService);
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

        // Declared as Object, since the startup scan builds a lazy bean whose declared type shows a handler. Only a
        // bean whose declaration shows nothing is left to register when something else builds it.
        @Bean
        @Lazy
        Object lazySynchronousReceiver() {
            return new LazySynchronousReceiver();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class LazyReplayWritesApplication {

        // Built when the projection registers, and slow to build. The replay of the lazy bean is already running by
        // then if the projection registers after the stream subscription, so the delay only makes a registration in
        // the wrong order lose the write every time. Nothing a correct registration does depends on it.
        @Bean
        @Lazy
        ViewStateRepository<WrittenCount, String> writtenCountStore() throws InterruptedException {
            Thread.sleep(1_500);
            ConcurrentHashMap<String, WrittenCount> map = new ConcurrentHashMap<>();
            return ViewStateRepository.create(map::get, map::put);
        }

        @Bean
        @Lazy
        Object lazyReplayWriter(ApplicationService<TestEvent> applicationService) {
            return new LazyReplayWriter(applicationService);
        }
    }

    // Both lazy. The writer's declared type shows a stream subscription and no synchronous handler, so the scan of the
    // synchronous handlers leaves it alone, and the collaborator's declared type shows nothing at all. The scan of
    // every handler builds the writer, which pulls the collaborator in, and then registers the writer, whose replay
    // writes before the scan reaches the collaborator.
    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class LazyCollaboratorApplication {

        @Bean
        @Lazy
        ReplayWriterWithACollaborator replayWriterWithACollaborator(ApplicationService<TestEvent> applicationService, Collaborator collaborator) {
            return new ReplayWriterWithACollaborator(applicationService);
        }

        @Bean
        @Lazy
        Collaborator lazyCollaborator() {
            return new LazyCollaborator();
        }
    }

    interface Collaborator {
    }

    static class LazyCollaborator implements Collaborator {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "reactiveLazyCollaborator")
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class ReplayWriterWithACollaborator {
        private final ApplicationService<TestEvent> applicationService;

        ReplayWriterWithACollaborator(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @StreamSubscription(id = "reactiveReplayWriterWithACollaborator", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        Mono<Void> on(Seeded event) {
            return applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-the-replay"))).then();
        }
    }

    record WrittenCount(int count) {
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Seeded(String eventId, Date timestamp, String name) implements TestEvent {
        Seeded(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }

    record Written(String eventId, Date timestamp, String name) implements TestEvent {
        Written(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
