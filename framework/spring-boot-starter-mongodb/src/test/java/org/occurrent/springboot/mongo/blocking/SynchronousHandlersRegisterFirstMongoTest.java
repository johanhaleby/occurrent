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
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
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
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that a synchronous handler is registered before every other handler at startup, so a write made while the
 * startup scan is still registering reaches it. A synchronous handler only ever runs on the writer's thread, so one
 * that is not registered yet when the write happens never sees it, and nothing replays it later.
 * <p>
 * In every scenario the handler that writes is declared on a bean that comes before the bean with the synchronous
 * handler, so a scan that registered in bean order would register the writer first. A replay with
 * {@code startupMode = WAIT_UNTIL_STARTED} runs to its end before the scan moves on, which makes the write happen
 * during registration rather than somewhere near it.
 * <p>
 * Each scenario boots its own application with {@code SpringApplication.run}, on a database of its own, after another
 * boot wrote the event the replay reads.
 */
@DisplayName("Synchronous handlers register before the rest")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class SynchronousHandlersRegisterFirstMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:synchronous-handlers-register-first");

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
    void a_write_made_by_a_saga_at_startup_reaches_a_synchronous_handler_declared_on_a_later_bean() {
        String database = "saga-write";
        seed(database, new OrderPlaced("order-1"));

        try (ConfigurableApplicationContext context = boot(SagaWritesApplication.class, database)) {
            SynchronousReceiver receiver = context.getBean(SynchronousReceiver.class);

            assertThat(receiver.received()).extracting(TestEvent::name).containsExactly("order-1");
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
    void a_lazy_bean_with_a_synchronous_handler_registers_when_it_is_built_and_its_handler_runs_on_the_next_write() {
        try (ConfigurableApplicationContext context = boot(LazyReceiverApplication.class, "lazy-receiver")) {
            ApplicationService<TestEvent> applicationService = applicationServiceOf(context);
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("before-the-bean-is-built")));

            LazySynchronousReceiver receiver = context.getBean("lazySynchronousReceiver", LazySynchronousReceiver.class);
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("after-the-bean-is-built")));

            // The write before the bean was built is not replayed to a synchronous handler, so only the later one counts
            assertThat(receiver.received()).extracting(TestEvent::name).containsExactly("after-the-bean-is-built");
        }
    }

    @Test
    void a_write_made_by_the_replay_of_a_lazy_bean_when_it_is_built_reaches_its_own_synchronous_handler() throws InterruptedException {
        try (ConfigurableApplicationContext context = boot(LazyReplayWritesApplication.class, "lazy-replay-write")) {
            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new Seeded("seeded")));

            LazyReplayWriter writer = context.getBean("lazyReplayWriter", LazyReplayWriter.class);

            // A bean built after startup replays in the background, so wait for the write the replay makes. The
            // write returns after every synchronous handler has run, so nothing is awaited past this point.
            assertThat(writer.replayHasWritten.await(30, TimeUnit.SECONDS)).as("the lazy bean's replay wrote").isTrue();
            assertThat(context.getBean(WrittenCounts.class).count())
                    .as("writes the lazy bean's replay made, counted by its synchronous projection").isEqualTo(1);
        }
    }

    private static void seed(String database, TestEvent event) {
        try (ConfigurableApplicationContext context = boot(SeedApplication.class, database)) {
            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(event));
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

        @SynchronousSubscription(id = "synchronousReceiver")
        void on(Written event) {
            received.add(event);
        }

        @SynchronousSubscription(id = "synchronousOrderReceiver")
        void on(OrderConfirmed event) {
            received.add(event);
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

        @StreamSubscription(id = "replayWriter", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        void on(Seeded event) {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-the-replay")));
        }
    }

    static class OrderSaga {
        @Saga(id = "orderSaga", startAt = StartPosition.BEGINNING, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        org.occurrent.dsl.saga.Saga<TestEvent, SagaState, SagaCommand> orderSaga() {
            return org.occurrent.dsl.saga.Saga.<TestEvent, SagaState, SagaCommand>builder(new SagaState("NEW"))
                    .correlateAll(TestEvent::name)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, event) -> new SagaState("PLACED"))
                    .react(OrderPlaced.class, (state, event) -> List.of(SagaEffect.issue(new ConfirmOrder(event.name()))))
                    .isTerminal(state -> state.phase().equals("PLACED"))
                    .build();
        }
    }

    static class ConfirmingDispatcher implements CommandDispatcher<SagaCommand> {
        private final ApplicationService<TestEvent> applicationService;

        ConfirmingDispatcher(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @Override
        public void dispatch(SagaCommand command) {
            if (command instanceof ConfirmOrder confirm) {
                applicationService.execute(confirm.orderId(), __ -> List.of(new OrderConfirmed(confirm.orderId())));
            }
        }
    }

    static class LazySynchronousReceiver {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "lazySynchronousReceiver")
        void on(Written event) {
            received.add(event);
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

        @StreamSubscription(id = "lazyReplayWriter", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        void on(Seeded event) {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-the-lazy-replay")));
            replayHasWritten.countDown();
        }

        @Projection(id = "writtenCounter", mode = org.occurrent.annotation.Mode.SYNCHRONOUS, storeName = "writtenCountStore")
        org.occurrent.dsl.projection.Projection<WrittenCount, TestEvent, String> writtenCounter() {
            return org.occurrent.dsl.projection.Projection.<WrittenCount, TestEvent, String>builder(new WrittenCount(0))
                    .id(event -> "count")
                    .on(Written.class, (state, event) -> new WrittenCount(state.count() + 1))
                    .build();
        }
    }

    static class WrittenCounts implements ViewStateRepository<WrittenCount, String> {
        private final ConcurrentHashMap<String, WrittenCount> store = new ConcurrentHashMap<>();

        @Override
        public Optional<WrittenCount> findById(String id) {
            return Optional.ofNullable(store.get(id));
        }

        @Override
        public void save(String id, WrittenCount state) {
            store.put(id, state);
        }

        int count() {
            WrittenCount current = store.get("count");
            return current == null ? 0 : current.count();
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
    static class SeedApplication {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
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
    @EnableOccurrent
    @Import(EventConverters.class)
    static class SagaWritesApplication {

        @Bean
        OrderSaga orderSaga() {
            return new OrderSaga();
        }

        @Bean
        ConfirmingDispatcher confirmingDispatcher(ApplicationService<TestEvent> applicationService) {
            return new ConfirmingDispatcher(applicationService);
        }

        // Lazy, so the startup scan builds it on its first pass and registers its handler on its second. A scan that
        // registers the saga on its first pass has the saga's write made before that.
        @Bean
        @Lazy
        SynchronousReceiver synchronousReceiver() {
            return new SynchronousReceiver();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
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
    @EnableOccurrent
    @Import(EventConverters.class)
    static class LazyReplayWritesApplication {

        // Built when the projection registers, and slow to build. The replay of the lazy bean is already running by
        // then if the projection registers after the stream subscription, so the delay only makes a registration in
        // the wrong order lose the write every time. Nothing a correct registration does depends on it.
        @Bean
        @Lazy
        WrittenCounts writtenCountStore() throws InterruptedException {
            Thread.sleep(1_500);
            return new WrittenCounts();
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
    @EnableOccurrent
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

        @SynchronousSubscription(id = "lazyCollaborator")
        void on(Written event) {
            received.add(event);
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

        @StreamSubscription(id = "replayWriterWithACollaborator", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        void on(Seeded event) {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-the-replay")));
        }
    }

    record SagaState(String phase) {
    }

    record WrittenCount(int count) {
    }

    sealed interface SagaCommand {
    }

    record ConfirmOrder(String orderId) implements SagaCommand {
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

    record OrderPlaced(String eventId, Date timestamp, String name) implements TestEvent {
        OrderPlaced(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }

    record OrderConfirmed(String eventId, Date timestamp, String name) implements TestEvent {
        OrderConfirmed(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
