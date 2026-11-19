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

import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Proves which writes through the application service go through while the application starts. A synchronous handler
 * has no replay, so every write the application service admits has to reach it. Writes are refused until the startup
 * scan has registered every synchronous handler, and a bean that would register one after that fails its creation.
 * <p>
 * The synchronous handlers in these scenarios sit on {@code @Lazy} beans declared as {@code Object}, so nothing shows
 * the startup scan their handlers until something builds them.
 */
@DisplayName("Reactive writes through the application service while the application starts")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveStartupWriteAdmissionMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-startup-write-admission");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_write_from_another_thread_is_refused_while_a_synchronous_bean_that_is_built_has_not_registered_and_is_admitted_after_startup() {
        try (ConfigurableApplicationContext context = boot(WriteFromAnotherThreadApplication.class, "write-from-another-thread")) {
            WriterThread writer = context.getBean("writerThread", WriterThread.class);
            HiddenSynchronousReceiver receiver = context.getBean("hiddenSynchronousReceiver", HiddenSynchronousReceiver.class);

            assertThat(writer.outcome.get()).as("[the write from another thread during startup]").isInstanceOf(WriteRefusedException.class);
            assertThat(storedIn(context, writer.streamId)).as("[events stored by the refused write]").isEmpty();

            applicationServiceOf(context).execute(writer.streamId, __ -> List.of(new Written("written-after-startup"))).block();

            assertThat(storedIn(context, writer.streamId)).as("[events stored by the same write after startup]").hasSize(1);
            assertThat(receiver.received).as("[events given to the synchronous bean that was built during startup]").extracting(TestEvent::name).containsExactly("written-after-startup");
        }
    }

    @Test
    void a_replay_that_builds_a_hidden_synchronous_bean_for_the_first_time_has_the_creation_refused_naming_the_bean() {
        String database = "replay-builds-hidden-bean";
        seed(database, new Seeded("seeded"));

        try (ConfigurableApplicationContext context = boot(ReplayBuildsHiddenBeanApplication.class, database)) {
            Throwable refusal = context.getBean(ReplayBuildingTheHiddenBean.class).outcome.get();

            assertThat(refusal).as("[what the replay got when it built the hidden bean]").isInstanceOf(BeanCreationException.class);
            assertThat(rootCauseOf(refusal)).as("[root cause of the refused creation]")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Bean 'hiddenSynchronousReceiver' declares the synchronous handler")
                    .hasMessageContaining("is created after the startup scan of the Spring Boot starter registered the synchronous handlers");
        }
    }

    @Test
    void a_prototype_whose_synchronous_handler_registered_at_startup_can_still_be_fetched_after_startup() {
        try (ConfigurableApplicationContext context = boot(PrototypeReceiverApplication.class, "prototype-receiver")) {
            Object first = context.getBean("prototypeReceiver");
            Object second = context.getBean("prototypeReceiver");

            assertThat(first).as("[first instance fetched after startup]").isInstanceOf(PrototypeReceiver.class);
            assertThat(second).as("[second instance fetched after startup]").isInstanceOf(PrototypeReceiver.class).isNotSameAs(first);
        }
    }

    private static Throwable rootCauseOf(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }

    private static List<String> storedIn(ConfigurableApplicationContext context, String streamId) {
        return context.getBean(EventStore.class).read(streamId).flatMapMany(stream -> stream.events()).map(event -> event.getType()).collectList().block();
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

    static class HiddenSynchronousReceiver {
        final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "reactiveHiddenSynchronousReceiver")
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    // Its declared type shows an asynchronous handler, so the general scan builds it, and building it builds the hidden
    // synchronous bean it depends on first. From its post construct method another thread writes while that bean is
    // built and has not registered.
    static class WriterThread {
        final String streamId = UUID.randomUUID().toString();
        final AtomicReference<Object> outcome = new AtomicReference<>();
        private final ApplicationService<TestEvent> applicationService;

        WriterThread(ApplicationService<TestEvent> applicationService, Object hiddenSynchronousReceiver) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void writeFromAnotherThread() throws InterruptedException {
            Thread thread = new Thread(() -> {
                Throwable failure = catchThrowable(() -> applicationService.execute(streamId, __ -> List.of(new Written("written-during-startup"))).block());
                outcome.set(failure == null ? "written" : failure);
            });
            thread.start();
            thread.join(TimeUnit.SECONDS.toMillis(60));
        }

        @Subscription(id = "reactiveWriterThread", startAt = StartPosition.NOW)
        Mono<Void> on(Written event) {
            return Mono.empty();
        }
    }

    // Its replay runs after the startup scan, and builds the lazy bean that only it asks for
    static class ReplayBuildingTheHiddenBean {
        final AtomicReference<Throwable> outcome = new AtomicReference<>();
        private final BeanFactory beanFactory;

        ReplayBuildingTheHiddenBean(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @StreamSubscription(id = "reactiveReplayBuildingTheHiddenBean", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        Mono<Void> on(Seeded event) {
            outcome.set(catchThrowable(() -> beanFactory.getBean("hiddenSynchronousReceiver")));
            return Mono.empty();
        }
    }

    static class PrototypeReceiver {
        @SynchronousSubscription(id = "reactivePrototypeReceiver")
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
    static class SeedApplication {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class WriteFromAnotherThreadApplication {

        @Bean
        @Lazy
        Object hiddenSynchronousReceiver() {
            return new HiddenSynchronousReceiver();
        }

        @Bean
        @Lazy
        WriterThread writerThread(ApplicationService<TestEvent> applicationService, @Qualifier("hiddenSynchronousReceiver") Object hiddenSynchronousReceiver) {
            return new WriterThread(applicationService, hiddenSynchronousReceiver);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class ReplayBuildsHiddenBeanApplication {

        @Bean
        ReplayBuildingTheHiddenBean replayBuildingTheHiddenBean(BeanFactory beanFactory) {
            return new ReplayBuildingTheHiddenBean(beanFactory);
        }

        // Declared as Object, so nothing builds it during the startup scan
        @Bean
        @Lazy
        Object hiddenSynchronousReceiver() {
            return new HiddenSynchronousReceiver();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class PrototypeReceiverApplication {

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        PrototypeReceiver prototypeReceiver() {
            return new PrototypeReceiver();
        }
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
