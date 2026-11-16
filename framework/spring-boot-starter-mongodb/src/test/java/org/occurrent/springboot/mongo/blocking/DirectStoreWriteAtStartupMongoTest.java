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

import jakarta.annotation.PostConstruct;
import org.aopalliance.intercept.MethodInterceptor;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A bean built while the context starts, before the startup scan registers anything, writes straight to the event
 * store from its {@code @PostConstruct} method. A subscription that starts live at startup begins where the event
 * store was when it was created, so it gets that write.
 */
@DisplayName("A write straight to the event store while the context starts")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class DirectStoreWriteAtStartupMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:direct-store-write-at-startup");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void reaches_a_default_projection_with_no_checkpoint() {
        try (ConfigurableApplicationContext context = boot(DefaultProjectionApplication.class, "default-projection")) {
            WrittenCounts counts = context.getBean(WrittenCounts.class);

            awaitUntil(() -> counts.findById("count").isPresent());
            assertThat(counts.findById("count")).as("what the projection counted").contains(new WrittenCount(1));
        }
    }

    @Test
    void through_an_event_store_wrapped_in_a_proxy_with_a_single_target_reaches_a_now_subscription() {
        try (ConfigurableApplicationContext context = boot(ProxiedEventStoreApplication.class, "proxied-event-store")) {
            assertThat(AopUtils.isAopProxy(context.getBean(SpringMongoEventStore.class))).as("the event store bean is a proxy").isTrue();
            NowReceiver receiver = context.getBean(NowReceiver.class);

            awaitUntil(() -> !receiver.received.isEmpty());
            assertThat(receiver.received).extracting(TestEvent::name).containsExactly("written-at-startup");
        }
    }

    private static ConfigurableApplicationContext boot(Class<?> application, String database) {
        return SpringApplication.run(application,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    // The wait only gives a delivery the time to arrive. What was delivered is asserted by the caller, so a
    // delivery that never comes fails on that assertion and not on the wait.
    private static void awaitUntil(Callable<Boolean> delivered) {
        try {
            await().atMost(ofSeconds(15)).pollInterval(ofMillis(100)).until(delivered);
        } catch (ConditionTimeoutException ignored) {
            // reported by the assertion that follows
        }
    }

    static class StartupDirectWriter {
        private final EventStore eventStore;
        private final CloudEventConverter<TestEvent> converter;

        StartupDirectWriter(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            this.eventStore = eventStore;
            this.converter = converter;
        }

        @PostConstruct
        void write() {
            eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Written("written-at-startup"))));
        }
    }

    static class WrittenCounter {
        @Projection(id = "defaultWrittenCounter", storeName = "writtenCounts")
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
    }

    static class NowReceiver {
        final List<TestEvent> received = new CopyOnWriteArrayList<>();

        @Subscription(id = "nowReceiverOfAProxiedStore", startAt = StartPosition.NOW)
        void on(Written event) {
            received.add(event);
        }
    }

    // Ordered, so it runs before the Occurrent post-processor, which then only ever sees the proxy
    static class EventStoreProxier implements BeanPostProcessor, Ordered {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!(bean instanceof SpringMongoEventStore)) {
                return bean;
            }
            ProxyFactory proxyFactory = new ProxyFactory(bean);
            proxyFactory.setProxyTargetClass(true);
            proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
            return proxyFactory.getProxy();
        }

        @Override
        public int getOrder() {
            return 0;
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
    static class DefaultProjectionApplication {

        @Bean
        StartupDirectWriter startupDirectWriter(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            return new StartupDirectWriter(eventStore, converter);
        }

        @Bean
        WrittenCounts writtenCounts() {
            return new WrittenCounts();
        }

        @Bean
        WrittenCounter writtenCounter() {
            return new WrittenCounter();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @Import(EventConverters.class)
    static class ProxiedEventStoreApplication {

        @Bean
        static EventStoreProxier eventStoreProxier() {
            return new EventStoreProxier();
        }

        @Bean
        StartupDirectWriter startupDirectWriter(EventStore eventStore, CloudEventConverter<TestEvent> converter) {
            return new StartupDirectWriter(eventStore, converter);
        }

        @Bean
        NowReceiver nowReceiver() {
            return new NowReceiver();
        }
    }

    record WrittenCount(int count) {
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
