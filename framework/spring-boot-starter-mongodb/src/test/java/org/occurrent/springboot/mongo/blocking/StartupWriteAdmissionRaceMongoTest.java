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

import io.cloudevents.CloudEvent;
import jakarta.annotation.PostConstruct;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Mode;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
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
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

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
class StartupWriteAdmissionRaceMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:startup-write-admission-race");
    private static final long WAIT_SECONDS = 30;
    private static final AtomicBoolean RUN_RETURNED = new AtomicBoolean();
    private static volatile Race race;
    private static volatile Claim claim;
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

            applicationServiceOf(context).execute(streamId, __ -> List.of(new Written("written-after-startup")));
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
    void a_synchronous_handler_another_thread_claimed_but_has_not_subscribed_when_the_startup_thread_drains_gets_the_write_made_after_startup() throws Exception {
        Claim theClaim = new Claim();
        claim = theClaim;
        try (ConfigurableApplicationContext context = boot(ClaimApplication.class, "claim-then-register")) {
            assertThat(theClaim.claimed.getCount()).as("[precondition: the other thread claimed the handler and is parked before it subscribes]").isZero();
            assertThat(theClaim.startupThread.get()).as("[precondition: the startup thread ran the drain]").isNotNull();

            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-after-startup")));
            theClaim.release.countDown();
            assertThat(theClaim.finished.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("[precondition: the other thread finished the creation]").isTrue();

            assertThat(theClaim.deliveries).as("[events given to the handler the other thread claimed before the gate opened]").extracting(TestEvent::name).containsExactly("written-after-startup");
            assertThat(theClaim.creation.get()).as("[the creation the other thread finished]").isInstanceOf(ClaimingReceiver.class);
            assertThat(theClaim.startupThreadWaited).as("[the startup thread waited in openIfNothingLeft for the registration]").isTrue();
        } finally {
            theClaim.release.countDown();
            claim = null;
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

            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-after-startup")));

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

            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-after-startup")));

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
        public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
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
            return new CatchupReadScope() {
                @Override
                public @Nullable String liveStart() {
                    return null;
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.empty();
                }

                @Override
                public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
                    return Stream.empty();
                }

                @Override
                public void close() {
                }
            };
        }

        @SynchronousSubscription(id = "racingReceiver")
        void on(Written event) {
            race.deliveries.add(event);
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

    // The two registrations of the claim test. The queued one blocks the startup thread inside the drain, which is
    // after startupScanComplete is set and before the gate opens. While it blocks, another thread creates the claiming
    // bean, whose registration then claims its handler and parks before the handler subscribes. The queued one is
    // then let go, and the drain skips the claimed handler.
    static final class Claim {
        final AtomicInteger queuedBuilt = new AtomicInteger();
        final AtomicBoolean started = new AtomicBoolean();
        final AtomicReference<Thread> startupThread = new AtomicReference<>();
        final AtomicReference<BeanFactory> beanFactory = new AtomicReference<>();
        final CountDownLatch claimed = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicBoolean startupThreadWaited = new AtomicBoolean();
        final AtomicReference<Object> creation = new AtomicReference<>();
        final List<TestEvent> deliveries = new CopyOnWriteArrayList<>();
    }

    // Registered by the drain on the startup thread. Its descriptor factory creates the claiming bean on another thread,
    // waits until that thread parked in the claiming bean's descriptor factory, and then returns, which is when the
    // drain moves on. A third thread releases the parked registration once the startup thread waits for it in
    // openIfNothingLeft. Without that wait the startup thread moves on, and the test releases the registration only
    // after it has written.
    static class QueuedReceiver {
        @Projection(id = "queuedReceiver", mode = Mode.SYNCHRONOUS)
        org.occurrent.dsl.projection.Projection<Count, TestEvent, String> queued() {
            Claim theClaim = claim;
            if (theClaim != null && theClaim.startupThread.compareAndSet(null, Thread.currentThread())) {
                Thread startup = Thread.currentThread();
                Thread creating = new Thread(() -> {
                    Object outcome;
                    try {
                        outcome = theClaim.beanFactory.get().getBean("claimingReceiver");
                    } catch (Throwable e) {
                        outcome = e;
                    }
                    theClaim.creation.set(outcome);
                    theClaim.finished.countDown();
                }, "claiming-creation");
                creating.setDaemon(true);
                creating.start();
                Thread releasing = new Thread(() -> {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
                    while (theClaim.release.getCount() > 0 && System.nanoTime() < deadline) {
                        if (waitsInOpenIfNothingLeft(startup)) {
                            theClaim.startupThreadWaited.set(true);
                            break;
                        }
                        Thread.onSpinWait();
                    }
                    theClaim.release.countDown();
                }, "releasing-the-claim");
                releasing.setDaemon(true);
                releasing.start();
                try {
                    if (!theClaim.claimed.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The claiming creation never reached its descriptor factory");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return org.occurrent.dsl.projection.Projection.<Count, TestEvent, String>builder(new Count(0))
                    .id(event -> "queued")
                    .on(Written.class, (state, event) -> state)
                    .build();
        }
    }

    // Parked by its descriptor factory, which runs after the handler is claimed and before it subscribes
    static class ClaimingReceiver {
        @Projection(id = "claimingReceiver", mode = Mode.SYNCHRONOUS)
        org.occurrent.dsl.projection.Projection<Count, TestEvent, String> claiming() {
            Claim theClaim = claim;
            theClaim.claimed.countDown();
            try {
                if (!theClaim.release.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The registration was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return org.occurrent.dsl.projection.Projection.<Count, TestEvent, String>builder(new Count(0))
                    .id(event -> "claiming")
                    .on(Written.class, (state, event) -> {
                        theClaim.deliveries.add(event);
                        return state;
                    })
                    .build();
        }
    }

    private static boolean waitsInOpenIfNothingLeft(Thread thread) {
        return thread.getState() == Thread.State.WAITING
               && Arrays.stream(thread.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("openIfNothingLeft"));
    }

    // The scan asks the type of every bean definition, which calls this on the startup thread. The first call builds
    // the queued bean on another thread, which hands it over to the drain, and then a second instance of another class
    // on the startup thread. The class the starter recorded for the name is then one without a handler, so the scan
    // never registers the queued handler and the drain does.
    static class StartsTheQueuedCreation implements FactoryBean<Object>, BeanFactoryAware {
        private BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
            Claim theClaim = claim;
            if (theClaim != null) {
                theClaim.beanFactory.set(beanFactory);
            }
        }

        @Override
        public Object getObject() {
            return "queued-creation-starter";
        }

        @Override
        public Class<?> getObjectType() {
            Claim theClaim = claim;
            if (theClaim != null && insideTheStartupScan() && theClaim.started.compareAndSet(false, true)) {
                Thread creating = new Thread(() -> beanFactory.getBean("queuedReceiver"), "queued-creation");
                creating.setDaemon(true);
                creating.start();
                try {
                    creating.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                beanFactory.getBean("queuedReceiver");
            }
            return Object.class;
        }

        private static boolean insideTheStartupScan() {
            return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("afterSingletonsInstantiated")
                                                                                     && frame.getClassName().contains("AnnotationBeanPostProcessor")));
        }
    }

    record Count(int count) {
    }

    static class SwappedReceiver {
        final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "swappedReceiver")
        void on(Written event) {
            received.add(event);
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

        @SynchronousSubscription(id = "varyingWithHandler")
        void on(Written event) {
            received.add(event);
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
        @SynchronousSubscription(id = "hammeredPrototype")
        void on(Written event) {
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
    @EnableOccurrent
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
    @EnableOccurrent
    @Import(EventConverters.class)
    static class ClaimApplication {

        @Bean
        ViewStateRepository<Count, String> countStore() {
            ConcurrentHashMap<String, Count> map = new ConcurrentHashMap<>();
            return ViewStateRepository.create(map::get, map::put);
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        Object queuedReceiver() {
            return claim.queuedBuilt.getAndIncrement() == 0 ? new QueuedReceiver() : new PlainShell();
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        Object claimingReceiver() {
            return new ClaimingReceiver();
        }

        @Bean
        StartsTheQueuedCreation startsTheQueuedCreation() {
            return new StartsTheQueuedCreation();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
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
    @EnableOccurrent
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
    @EnableOccurrent
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
    @EnableOccurrent
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
