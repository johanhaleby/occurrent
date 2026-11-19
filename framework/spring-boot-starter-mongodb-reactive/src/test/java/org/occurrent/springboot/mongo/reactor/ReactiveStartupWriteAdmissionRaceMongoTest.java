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

import io.cloudevents.CloudEvent;
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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import org.occurrent.filter.Filter;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.core.Ordered;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Proves that every write the application service admits reaches every synchronous handler the context registers,
 * or that the bean declaring the handler fails its creation, in the cases where the handler is declared by an object
 * the startup scan could not read ahead of time or is registered by a thread other than the startup thread.
 * <p>
 * Except for the prototype built by several threads, every bean with a synchronous handler here is declared as
 * {@code Object}, so nothing shows the startup scan its handler until something builds the bean.
 */
@DisplayName("A write the application service admits and a synchronous handler registered by another thread or on a replaced object")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveStartupWriteAdmissionRaceMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-startup-write-admission-race");
    private static final long WAIT_SECONDS = 30;
    private static final AtomicBoolean RUN_RETURNED = new AtomicBoolean();
    private static volatile Race race;
    private static volatile Hammer hammer;

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_bean_another_thread_finishes_after_the_gate_opened_either_gets_the_write_made_after_startup_or_fails_its_creation_with_the_refusal() throws Exception {
        Race theRace = new Race();
        race = theRace;
        try (ConfigurableApplicationContext context = boot(RaceApplication.class, "race-after-the-scan")) {
            assertThat(theRace.parked.getCount()).as("[precondition: the other thread is inside the creation of the bean the scan cannot see the handler of]").isZero();
            String streamId = UUID.randomUUID().toString();

            applicationServiceOf(context).execute(streamId, __ -> List.of(new Written("written-after-startup"))).block();
            theRace.release.countDown();
            assertThat(theRace.finished.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("[precondition: the other thread finished the creation]").isTrue();

            Object creation = theRace.creation.get();
            boolean refused = creation instanceof Throwable failure && rootCauseOf(failure).getMessage().contains("Bean 'racingReceiver' declares the synchronous handler");
            boolean delivered = theRace.deliveries.stream().map(TestEvent::name).toList().equals(List.of("written-after-startup"));
            assertThat(refused || delivered)
                    .as("[the bean the other thread finished after the gate opened was refused (%s) or got the write made after startup (%s), creation was %s]", refused, delivered, creation)
                    .isTrue();
        } finally {
            theRace.release.countDown();
            race = null;
        }
    }

    @Test
    void a_late_bean_replaced_by_another_post_processor_with_an_object_with_a_synchronous_handler_fails_its_creation_naming_the_bean() {
        try (ConfigurableApplicationContext context = boot(SwappedLateApplication.class, "swapped-late")) {
            AsksForTheLateBean runner = context.getBean(AsksForTheLateBean.class);
            assertThat(runner.existedWhenAsked.get()).as("[precondition: bean existed when the application runner asked for it]").isFalse();

            Object outcome = runner.outcome.get();

            assertThat(outcome).as("[what the application runner got when it asked for the bean]").isInstanceOf(Throwable.class);
            assertThat(rootCauseOf((Throwable) outcome)).as("[root cause of the refused creation]")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Bean 'swappedLate' declares the synchronous handler " + SwappedReceiver.class.getName() + "#on")
                    .hasMessageContaining("is created after the startup scan of the Spring Boot starter registered the synchronous handlers");
        }
    }

    @Test
    void a_bean_built_during_startup_and_replaced_by_another_post_processor_with_an_object_with_a_synchronous_handler_gets_the_write_made_after_startup() {
        try (ConfigurableApplicationContext context = boot(SwappedAtStartupApplication.class, "swapped-at-startup")) {
            Object bean = context.getBean("swappedAtStartup");
            assertThat(bean).as("[precondition: the bean is the object the other post-processor replaced it with]").isInstanceOf(SwappedReceiver.class);

            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-after-startup"))).block();

            assertThat(((SwappedReceiver) bean).received).as("[events given to the replacing object]").extracting(TestEvent::name).containsExactly("written-after-startup");
        }
    }

    @Test
    void a_prototype_instance_with_a_synchronous_handler_built_before_the_scan_gets_the_write_made_after_startup_when_a_later_instance_has_a_class_without_one() {
        VaryingPrototypes.built.set(0);
        VaryingPrototypes.first.set(null);
        try (ConfigurableApplicationContext context = boot(VaryingPrototypeApplication.class, "varying-prototype")) {
            VaryingWithHandler first = VaryingPrototypes.first.get();
            assertThat(first).as("[precondition: the first instance has the handler]").isNotNull();
            assertThat(VaryingPrototypes.built.get()).as("[precondition: a later instance was built, so the class the scan recorded for the name has no handler]").isGreaterThanOrEqualTo(2);

            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-after-startup"))).block();

            assertThat(first.received).as("[events given to the first instance]").extracting(TestEvent::name).containsExactly("written-after-startup");
        }
    }

    @Test
    void instances_of_a_prototype_with_a_synchronous_handler_built_by_several_threads_during_and_after_startup_are_never_refused() throws Exception {
        Hammer theHammer = new Hammer();
        hammer = theHammer;
        RUN_RETURNED.set(false);
        try (ConfigurableApplicationContext context = boot(HammeredPrototypeApplication.class, "hammered-prototype")) {
            RUN_RETURNED.set(true);
            theHammer.awaitCreationsAfterStartup(200);
            theHammer.stop();

            assertThat(theHammer.createdDuringStartup.get()).as("[precondition: instances built before the run returned]").isPositive();
            assertThat(theHammer.createdAfterStartup.get()).as("[precondition: instances built after the run returned]").isGreaterThanOrEqualTo(200);
            assertThat(theHammer.refusals).as("[instances refused]").extracting(Throwable::toString).isEmpty();
        } finally {
            theHammer.stop();
            hammer = null;
        }
    }

    private static Throwable rootCauseOf(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
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

    // The creation of the racing bean, parked by its start boundary read. The first read parks, every later one passes.
    static final class Race {
        final AtomicInteger created = new AtomicInteger();
        final AtomicInteger reads = new AtomicInteger();
        final CountDownLatch parked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicBoolean started = new AtomicBoolean();
        final AtomicReference<Object> creation = new AtomicReference<>();
        final List<TestEvent> deliveries = new CopyOnWriteArrayList<>();
    }

    // Every bean that is an event store has its start boundary read in the starter's post-processor as its creation
    // finishes. Parking that read holds the creation inside that callback while the startup thread opens the gate.
    static class RacingReceiver implements CatchupReads {

        @Override
        public Mono<CatchupReadScope> openCatchupReadScope(@Nullable String notBefore) {
            Race theRace = race;
            if (theRace != null && theRace.reads.getAndIncrement() == 0) {
                theRace.parked.countDown();
                try {
                    if (!theRace.release.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The creation was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return Mono.just(new CatchupReadScope() {
                @Override
                public @Nullable String liveStart() {
                    return null;
                }

                @Override
                public Mono<Long> currentPosition() {
                    return Mono.just(0L);
                }

                @Override
                public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Flux.empty();
                }

                @Override
                public Mono<Void> close() {
                    return Mono.empty();
                }
            });
        }

        @SynchronousSubscription(id = "reactiveRacingReceiver")
        Mono<Void> on(Written event) {
            race.deliveries.add(event);
            return Mono.empty();
        }
    }

    // The startup scan asks the type of every bean definition once per pass, which calls this on the startup thread. The
    // first call starts the thread that creates the racing bean, and then builds a second instance of another class
    // on the startup thread. The class the starter recorded for the name is then one without a handler, whatever
    // the number of passes, so the scan never registers the racing bean's handler.
    static class StartsTheRacingCreation implements FactoryBean<Object>, BeanFactoryAware {
        private BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public Object getObject() {
            return "racing-creation-starter";
        }

        @Override
        public Class<?> getObjectType() {
            Race theRace = race;
            if (theRace != null && insideTheStartupScan() && theRace.started.compareAndSet(false, true)) {
                Thread creating = new Thread(() -> {
                    Object outcome;
                    try {
                        outcome = beanFactory.getBean("racingReceiver");
                    } catch (Throwable e) {
                        outcome = e;
                    }
                    theRace.creation.set(outcome);
                    theRace.finished.countDown();
                }, "racing-creation");
                creating.setDaemon(true);
                creating.start();
                try {
                    theRace.parked.await(WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // A second instance of another class, so the class recorded for the name is one with no handler
                beanFactory.getBean("racingReceiver");
            }
            return Object.class;
        }

        private static boolean insideTheStartupScan() {
            return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("afterSingletonsInstantiated")
                                                                                     && frame.getClassName().contains("AnnotationBeanPostProcessor")));
        }
    }

    static class SwappedReceiver {
        final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "reactiveSwappedReceiver")
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    // What the bean's own class declares, which is no handler
    static class PlainShell {
    }

    // Runs ahead of the starter's post-processor, which is not ordered, so the starter is given the replacement
    static class ReplacesTheShell implements BeanPostProcessor, Ordered {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            return bean instanceof PlainShell ? new SwappedReceiver() : bean;
        }

        @Override
        public int getOrder() {
            return Ordered.HIGHEST_PRECEDENCE;
        }
    }

    static class AsksForTheLateBean implements ApplicationRunner {
        final AtomicReference<Object> outcome = new AtomicReference<>();
        final AtomicBoolean existedWhenAsked = new AtomicBoolean();
        private final ConfigurableBeanFactory beanFactory;

        AsksForTheLateBean(ConfigurableBeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public void run(ApplicationArguments args) {
            existedWhenAsked.set(beanFactory.containsSingleton("swappedLate"));
            Throwable failure = catchThrowable(() -> beanFactory.getBean("swappedLate"));
            outcome.set(failure == null ? "created" : failure);
        }
    }

    static final class VaryingPrototypes {
        static final AtomicInteger built = new AtomicInteger();
        static final AtomicReference<VaryingWithHandler> first = new AtomicReference<>();
    }

    static class VaryingWithHandler {
        final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "reactiveVaryingWithHandler")
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    static class VaryingWithoutHandler {
    }

    // Asks for the prototype twice before the startup scan, so the class the scan has recorded for its name is the
    // second instance's
    static class AsksForThePrototypeTwice {
        private final BeanFactory beanFactory;

        AsksForThePrototypeTwice(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @PostConstruct
        void ask() {
            VaryingPrototypes.first.set((VaryingWithHandler) beanFactory.getBean("varyingPrototype"));
            beanFactory.getBean("varyingPrototype");
        }
    }

    static class HammeredPrototype {
        @SynchronousSubscription(id = "reactiveHammeredPrototype")
        Mono<Void> on(Written event) {
            return Mono.empty();
        }
    }

    // Threads that build instances of the prototype from before the startup scan and until stopped
    static final class Hammer {
        final AtomicInteger createdDuringStartup = new AtomicInteger();
        final AtomicInteger createdAfterStartup = new AtomicInteger();
        final Queue<Throwable> refusals = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean stopped = new AtomicBoolean();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();

        void start(BeanFactory beanFactory, int count) {
            for (int i = 0; i < count; i++) {
                Thread thread = new Thread(() -> {
                    while (!stopped.get()) {
                        try {
                            beanFactory.getBean("hammeredPrototype");
                            (RUN_RETURNED.get() ? createdAfterStartup : createdDuringStartup).incrementAndGet();
                        } catch (Throwable e) {
                            refusals.add(e);
                        }
                    }
                }, "hammer-" + i);
                thread.setDaemon(true);
                threads.add(thread);
                thread.start();
            }
        }

        void awaitCreationsAfterStartup(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
            while (createdAfterStartup.get() < count && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        }

        void stop() throws InterruptedException {
            stopped.set(true);
            for (Thread thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            }
        }
    }

    static class StartsTheHammer {
        private final BeanFactory beanFactory;

        StartsTheHammer(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @PostConstruct
        void start() {
            hammer.start(beanFactory, 4);
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
    static class RaceApplication {

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        Object racingReceiver() {
            return race.created.getAndIncrement() == 0 ? new RacingReceiver() : new PlainShell();
        }

        @Bean
        StartsTheRacingCreation startsTheRacingCreation() {
            return new StartsTheRacingCreation();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class SwappedLateApplication {

        @Bean
        static ReplacesTheShell replacesTheShell() {
            return new ReplacesTheShell();
        }

        @Bean
        @Lazy
        Object swappedLate() {
            return new PlainShell();
        }

        @Bean
        AsksForTheLateBean asksForTheLateBean(ConfigurableBeanFactory beanFactory) {
            return new AsksForTheLateBean(beanFactory);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class SwappedAtStartupApplication {

        @Bean
        static ReplacesTheShell replacesTheShell() {
            return new ReplacesTheShell();
        }

        @Bean
        Object swappedAtStartup() {
            return new PlainShell();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class VaryingPrototypeApplication {

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        Object varyingPrototype() {
            return VaryingPrototypes.built.incrementAndGet() == 1 ? new VaryingWithHandler() : new VaryingWithoutHandler();
        }

        @Bean
        AsksForThePrototypeTwice asksForThePrototypeTwice(BeanFactory beanFactory) {
            return new AsksForThePrototypeTwice(beanFactory);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class HammeredPrototypeApplication {

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        HammeredPrototype hammeredPrototype() {
            return new HammeredPrototype();
        }

        @Bean
        StartsTheHammer startsTheHammer(BeanFactory beanFactory) {
            return new StartsTheHammer(beanFactory);
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
