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
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A bean built after the context has refreshed reads its start boundary as it is created. An event the bean's own
 * {@code @PostConstruct} method writes must reach the bean's own subscription. A bean with a synchronous handler is
 * refused before that method runs, since a synchronous handler has no replay.
 * <p>
 * Every bean here is {@code @Lazy} and declared as {@code Object}, so the startup scan cannot see its annotations and
 * the first {@code getBean} after {@code SpringApplication.run} returns builds it. Each test first proves that, by
 * checking that the bean did not exist when the context refreshed and that its constructor ran after the run returned.
 * The exceptions are a {@code FactoryBean} built as the context starts, whose product is still built after the run
 * returned and starts at the context's boundary, and a prototype, of which every {@code getBean} builds a new instance.
 */
@DisplayName("A bean built after the context has refreshed, with a start-up write of its own")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveLateBeanStartupWriteMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-late-bean-startup-write");
    private static final AtomicBoolean RUN_RETURNED = new AtomicBoolean();
    private static final long WAIT_SECONDS = 15;

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void an_event_written_directly_to_the_event_store_by_the_post_construct_method_is_delivered_to_the_beans_now_subscription() {
        try (ConfigurableApplicationContext context = boot(DirectWriteNowApplication.class, "direct-write-now")) {
            assertThat(context.getBeanFactory().containsSingleton("directWriteNow")).as("bean exists after refresh").isFalse();

            DirectWriteNow bean = context.getBean("directWriteNow", DirectWriteNow.class);

            assertThat(bean.builtAfterRunReturned).as("bean was built after SpringApplication.run returned").isTrue();
            awaitDelivery(bean.received, 1);
            assertThat(bean.received).extracting(TestEvent::name).containsExactly("written-by-post-construct");
        }
    }

    @Test
    void an_event_written_directly_to_the_event_store_by_the_post_construct_method_is_delivered_to_the_beans_default_subscription_with_no_checkpoint() {
        try (ConfigurableApplicationContext context = boot(DirectWriteDefaultApplication.class, "direct-write-default")) {
            assertThat(context.getBeanFactory().containsSingleton("directWriteDefault")).as("bean exists after refresh").isFalse();

            DirectWriteDefault bean = context.getBean("directWriteDefault", DirectWriteDefault.class);

            assertThat(bean.builtAfterRunReturned).as("bean was built after SpringApplication.run returned").isTrue();
            awaitDelivery(bean.received, 1);
            assertThat(bean.received).extracting(TestEvent::name).containsExactly("written-by-post-construct");
        }
    }

    @Test
    void an_event_written_through_the_application_service_by_the_post_construct_method_is_delivered_to_the_beans_now_subscription() {
        try (ConfigurableApplicationContext context = boot(ApplicationServiceWriteApplication.class, "application-service-write")) {
            assertThat(context.getBeanFactory().containsSingleton("applicationServiceWrite")).as("bean exists after refresh").isFalse();

            ApplicationServiceWrite bean = context.getBean("applicationServiceWrite", ApplicationServiceWrite.class);

            assertThat(bean.builtAfterRunReturned).as("bean was built after SpringApplication.run returned").isTrue();
            awaitDelivery(bean.received, 1);
            assertThat(bean.received).extracting(TestEvent::name).containsExactly("written-by-post-construct");
        }
    }

    @Test
    void the_events_written_by_a_late_factory_beans_post_construct_method_and_while_it_builds_its_product_are_delivered_to_the_products_now_subscription() {
        try (ConfigurableApplicationContext context = boot(FactoryApplication.class, "late-factory")) {
            assertThat(context.getBeanFactory().containsSingleton("lateProductFactory")).as("factory exists after refresh").isFalse();

            FactoryProduct product = (FactoryProduct) context.getBean("lateProductFactory");

            assertThat(context.getBean("&lateProductFactory", ProductFactory.class).builtAfterRunReturned).as("factory was built after SpringApplication.run returned").isTrue();
            assertThat(product.builtAfterRunReturned).as("product was built after SpringApplication.run returned").isTrue();
            awaitDelivery(product.received, 2);
            assertThat(product.received).extracting(TestEvent::name).containsExactly("written-by-the-factorys-post-construct", "written-by-get-object");
        }
    }

    @Test
    void the_product_of_a_factory_bean_built_at_startup_gets_what_its_get_object_method_wrote_and_what_is_written_once_it_exists() {
        try (ConfigurableApplicationContext context = boot(EagerFactoryApplication.class, "eager-factory-get-object")) {
            EagerProductFactory factory = context.getBean("&eagerProductFactory", EagerProductFactory.class);
            assertThat(factory.builtAfterRunReturned).as("factory was built after SpringApplication.run returned").isFalse();
            assertThat(context.getBeanFactory().getSingleton("eagerProductFactory")).as("factory exists after refresh").isSameAs(factory);
            factory.writeInGetObject = "written-by-get-object";

            EagerFactoryProduct product = (EagerFactoryProduct) context.getBean("eagerProductFactory");
            assertThat(product.builtAfterRunReturned).as("product was built after SpringApplication.run returned").isTrue();
            writeEvent(context, "written-once-the-product-exists");

            awaitDeliveryOf(product.received, "written-once-the-product-exists");
            assertThat(product.received).extracting(TestEvent::name).containsExactly("written-by-get-object", "written-once-the-product-exists");
        }
    }

    @Test
    void the_product_of_a_factory_bean_built_at_startup_gets_what_was_written_before_it_was_built_and_what_is_written_once_it_exists() {
        try (ConfigurableApplicationContext context = boot(EagerFactoryApplication.class, "eager-factory-write-before")) {
            EagerProductFactory factory = context.getBean("&eagerProductFactory", EagerProductFactory.class);
            assertThat(factory.builtAfterRunReturned).as("factory was built after SpringApplication.run returned").isFalse();
            writeEvent(context, "written-before-the-product-is-built");

            EagerFactoryProduct product = (EagerFactoryProduct) context.getBean("eagerProductFactory");
            assertThat(product.builtAfterRunReturned).as("product was built after SpringApplication.run returned").isTrue();
            writeEvent(context, "written-once-the-product-exists");

            awaitDeliveryOf(product.received, "written-once-the-product-exists");
            assertThat(product.received).extracting(TestEvent::name).containsExactly("written-before-the-product-is-built", "written-once-the-product-exists");
        }
    }

    @Test
    void two_first_creations_of_one_prototype_at_the_same_moment_each_keep_their_own_start_and_the_first_gets_what_its_post_construct_method_wrote() throws Exception {
        try (ConfigurableApplicationContext context = boot(PrototypeApplication.class, "prototype-at-the-same-moment")) {
            PrototypeRace race = context.getBean(PrototypeRace.class);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<Object> first = pool.submit(() -> context.getBean("racingPrototype"));
                assertThat(race.firstHasWritten.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("the first creation wrote from its post construct method").isTrue();
                Future<Object> second = pool.submit(() -> context.getBean("racingPrototype"));

                RacingPrototype firstInstance = (RacingPrototype) first.get(WAIT_SECONDS, TimeUnit.SECONDS);
                race.firstHasFinished.countDown();
                RacingPrototype secondInstance = (RacingPrototype) second.get(WAIT_SECONDS, TimeUnit.SECONDS);

                assertThat(firstInstance.first).as("the instance the first creation returned wrote the event").isTrue();
                assertThat(secondInstance.first).as("the instance the second creation returned wrote the event").isFalse();
                awaitDelivery(firstInstance.received, 1);
                assertThat(firstInstance.received).extracting(TestEvent::name).containsExactly("written-by-the-first-creation");
            } finally {
                race.firstHasFinished.countDown();
                pool.shutdownNow();
            }
        }
    }

    private static void writeEvent(ConfigurableApplicationContext context, String name) {
        @SuppressWarnings("unchecked")
        CloudEventConverter<TestEvent> converter = context.getBean(CloudEventConverter.class);
        context.getBean(EventStore.class).write(UUID.randomUUID().toString(), Flux.fromIterable(converter.toCloudEvents(List.of(new Written(name))))).block();
    }

    @Test
    void a_late_bean_with_a_synchronous_handler_is_refused_before_its_post_construct_method_writes_through_the_application_service() {
        try (ConfigurableApplicationContext context = boot(SynchronousWriteApplication.class, "synchronous-write")) {
            assertThat(context.getBeanFactory().containsSingleton("synchronousWrite")).as("bean exists after refresh").isFalse();

            Throwable refusal = catchThrowable(() -> context.getBean("synchronousWrite"));

            assertThat(refusal).as("[getBean of the late bean]").isInstanceOf(BeanCreationException.class);
            assertThat(refusal.getCause()).as("[cause of the refused creation]").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Bean 'synchronousWrite' declares the synchronous handler")
                    .hasMessageContaining("is created after the startup scan of the Spring Boot starter registered the synchronous handlers");
            assertThat(SynchronousWrite.POST_CONSTRUCT_RAN).as("[post construct method of the refused bean ran]").isFalse();
        }
    }

    private static ConfigurableApplicationContext boot(Class<?> application, String database) {
        RUN_RETURNED.set(false);
        ConfigurableApplicationContext context = SpringApplication.run(application,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
        RUN_RETURNED.set(true);
        return context;
    }

    // Waits for the last event written, which arrives whichever earlier events the subscription started with, so the
    // assertion that follows fails on the list it received and not on the wait.
    private static void awaitDeliveryOf(List<TestEvent> received, String eventName) {
        try {
            await().atMost(ofSeconds(15)).pollInterval(ofMillis(100)).until(() -> received.stream().anyMatch(event -> event.name().equals(eventName)));
        } catch (ConditionTimeoutException ignored) {
            // reported by the assertion that follows
        }
    }

    // The wait only gives a delivery the time to arrive. What was delivered is asserted by the caller, so a
    // delivery that never comes fails on that assertion and not on the wait.
    private static void awaitDelivery(List<TestEvent> received, int expected) {
        try {
            await().atMost(ofSeconds(15)).pollInterval(ofMillis(100)).until(() -> received.size() >= expected);
        } catch (ConditionTimeoutException ignored) {
            // reported by the assertion that follows
        }
    }

    abstract static class BuiltAfterRunReturnedProbe {
        final boolean builtAfterRunReturned = RUN_RETURNED.get();
        final List<TestEvent> received = new CopyOnWriteArrayList<>();
    }

    static class DirectWriteNow extends BuiltAfterRunReturnedProbe {
        private final EventStore eventStore;
        private final CloudEventConverter<TestEvent> converter;

        DirectWriteNow(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            this.eventStore = eventStore;
            this.converter = converter;
        }

        @PostConstruct
        void write() {
            eventStore.write(UUID.randomUUID().toString(), Flux.fromIterable(converter.toCloudEvents(List.of(new Written("written-by-post-construct"))))).block();
        }

        @Subscription(id = "lateDirectWriteNow", startAt = StartPosition.NOW)
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    static class DirectWriteDefault extends BuiltAfterRunReturnedProbe {
        private final EventStore eventStore;
        private final CloudEventConverter<TestEvent> converter;

        DirectWriteDefault(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            this.eventStore = eventStore;
            this.converter = converter;
        }

        @PostConstruct
        void write() {
            eventStore.write(UUID.randomUUID().toString(), Flux.fromIterable(converter.toCloudEvents(List.of(new Written("written-by-post-construct"))))).block();
        }

        @Subscription(id = "lateDirectWriteDefault", startAt = StartPosition.DEFAULT)
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    static class ApplicationServiceWrite extends BuiltAfterRunReturnedProbe {
        private final ApplicationService<TestEvent> applicationService;

        ApplicationServiceWrite(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void write() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Written("written-by-post-construct"))).block();
        }

        @Subscription(id = "lateApplicationServiceWrite", startAt = StartPosition.NOW)
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    // A synchronous subscription has no replay, so a late bean that declares one is refused before this method runs
    static class SynchronousWrite extends BuiltAfterRunReturnedProbe {
        static final AtomicBoolean POST_CONSTRUCT_RAN = new AtomicBoolean();
        final String streamId = UUID.randomUUID().toString();
        private final ApplicationService<TestEvent> applicationService;

        SynchronousWrite(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void write() {
            POST_CONSTRUCT_RAN.set(true);
            applicationService.execute(streamId,__ -> List.of(new Written("written-by-post-construct"))).block();
        }

        @SynchronousSubscription(id = "lateSynchronousWrite")
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    // The product's declared type is this interface, so the startup scan sees no annotation on it
    interface Product {
    }

    static class FactoryProduct extends BuiltAfterRunReturnedProbe implements Product {
        @Subscription(id = "lateFactoryProduct", startAt = StartPosition.NOW)
        Mono<Void> on(Written event) {
            received.add(event);
            return Mono.empty();
        }
    }

    // A plain FactoryBean caches its product, so every getBean returns the instance the test holds. It also builds the product only when something asks for it
    static class ProductFactory extends BuiltAfterRunReturnedProbe implements FactoryBean<Product> {
        private final EventStore eventStore;
        private final CloudEventConverter<TestEvent> converter;

        ProductFactory(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            this.eventStore = eventStore;
            this.converter = converter;
        }

        @PostConstruct
        void write() {
            writeEvent("written-by-the-factorys-post-construct");
        }

        @Override
        public Product getObject() {
            writeEvent("written-by-get-object");
            return new FactoryProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return Product.class;
        }

        private void writeEvent(String name) {
            eventStore.write(UUID.randomUUID().toString(), Flux.fromIterable(converter.toCloudEvents(List.of(new Written(name))))).block();
        }
    }

    static class EagerFactoryProduct extends BuiltAfterRunReturnedProbe implements Product {
        @Subscription(id = "eagerFactoryProduct", startAt = StartPosition.NOW)
        void on(Written event) {
            received.add(event);
        }
    }

    // Built while the context starts, and builds its product only when something first asks for it
    static class EagerProductFactory extends BuiltAfterRunReturnedProbe implements FactoryBean<Product> {
        private final EventStore eventStore;
        private final CloudEventConverter<TestEvent> converter;
        volatile @Nullable String writeInGetObject;

        EagerProductFactory(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            this.eventStore = eventStore;
            this.converter = converter;
        }

        @Override
        public Product getObject() {
            String name = writeInGetObject;
            if (name != null) {
                eventStore.write(UUID.randomUUID().toString(), Flux.fromIterable(converter.toCloudEvents(List.of(new Written(name))))).block();
            }
            return new EagerFactoryProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return Product.class;
        }
    }

    static class PrototypeRace {
        final AtomicBoolean firstClaimed = new AtomicBoolean();
        final CountDownLatch firstHasWritten = new CountDownLatch(1);
        final CountDownLatch secondInPostConstruct = new CountDownLatch(1);
        final CountDownLatch firstHasFinished = new CountDownLatch(1);
    }

    // The first creation writes from its @PostConstruct method and finishes only once the second creation has read
    // its own start boundary and reached its @PostConstruct method, which then waits for the first to finish
    static class RacingPrototype extends BuiltAfterRunReturnedProbe {
        private final PrototypeRace race;
        private final EventStore eventStore;
        private final CloudEventConverter<TestEvent> converter;
        boolean first;

        RacingPrototype(PrototypeRace race, EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            this.race = race;
            this.eventStore = eventStore;
            this.converter = converter;
        }

        @PostConstruct
        void write() throws InterruptedException {
            first = race.firstClaimed.compareAndSet(false, true);
            if (first) {
                eventStore.write(UUID.randomUUID().toString(), Flux.fromIterable(converter.toCloudEvents(List.of(new Written("written-by-the-first-creation"))))).block();
                race.firstHasWritten.countDown();
                race.secondInPostConstruct.await(WAIT_SECONDS, TimeUnit.SECONDS);
            } else {
                race.secondInPostConstruct.countDown();
                race.firstHasFinished.await(WAIT_SECONDS, TimeUnit.SECONDS);
            }
        }

        @Subscription(id = "racingPrototype", startAt = StartPosition.NOW)
        void on(Written event) {
            received.add(event);
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
    static class DirectWriteNowApplication {
        @Bean
        @Lazy
        Object directWriteNow(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            return new DirectWriteNow(eventStore, converter);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class DirectWriteDefaultApplication {
        @Bean
        @Lazy
        Object directWriteDefault(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            return new DirectWriteDefault(eventStore, converter);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class ApplicationServiceWriteApplication {
        @Bean
        @Lazy
        Object applicationServiceWrite(ApplicationService<TestEvent> applicationService) {
            return new ApplicationServiceWrite(applicationService);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class SynchronousWriteApplication {
        @Bean
        @Lazy
        Object synchronousWrite(ApplicationService<TestEvent> applicationService) {
            return new SynchronousWrite(applicationService);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class FactoryApplication {
        @Bean
        @Lazy
        ProductFactory lateProductFactory(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            return new ProductFactory(eventStore, converter);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class EagerFactoryApplication {
        @Bean
        EagerProductFactory eagerProductFactory(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            return new EagerProductFactory(eventStore, converter);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class PrototypeApplication {
        @Bean
        PrototypeRace prototypeRace() {
            return new PrototypeRace();
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        Object racingPrototype(PrototypeRace race, EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            return new RacingPrototype(race, eventStore, converter);
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
