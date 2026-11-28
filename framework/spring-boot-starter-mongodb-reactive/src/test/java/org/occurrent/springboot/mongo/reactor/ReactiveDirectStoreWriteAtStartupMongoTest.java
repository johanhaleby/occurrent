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
import org.aopalliance.intercept.MethodInterceptor;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
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
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The reactive counterpart of {@code DirectStoreWriteAtStartupMongoTest}: a bean built while the context starts, before
 * the startup scan registers anything, writes straight to the event store from its {@code @PostConstruct} method. A
 * subscription that starts at {@code NOW} begins where the event store was when it was created, and finds that store
 * behind a proxy with a single target, so it gets that write.
 */
@DisplayName("A reactive write straight to the event store while the context starts")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveDirectStoreWriteAtStartupMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-direct-store-write-at-startup");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void through_an_event_store_behind_a_proxy_with_a_single_target_reaches_a_now_subscription() {
        try (ConfigurableApplicationContext context = boot(ProxiedEventStoreApplication.class, "proxied-event-store")) {
            EventStore eventStoreBean = context.getBean(EventStore.class);
            assertThat(AopUtils.isAopProxy(eventStoreBean)).as("the event store bean is a proxy").isTrue();
            NowReceiver receiver = context.getBean(NowReceiver.class);

            awaitUntil(() -> !receiver.received.isEmpty());
            assertThat(receiver.received).extracting(Written::name).containsExactly("written-at-startup");
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
        private final CloudEventConverter<Written> converter;

        StartupDirectWriter(EventStore eventStore, CloudEventConverter<Written> converter) {
            this.eventStore = eventStore;
            this.converter = converter;
        }

        @PostConstruct
        void write() {
            eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvent(new Written("written-at-startup"))).block();
        }
    }

    static class NowReceiver {
        final List<Written> received = new CopyOnWriteArrayList<>();

        @Subscription(id = "reactiveNowReceiverOfAProxiedStore", startAt = StartPosition.NOW)
        Mono<Void> on(Written event) {
            return Mono.fromRunnable(() -> received.add(event));
        }
    }

    // A class proxy with a single target. Ordered, so it runs before the Occurrent post-processor, which then only ever sees the proxy.
    static class EventStoreProxier implements BeanPostProcessor, Ordered {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!(bean instanceof ReactorMongoEventStore)) {
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
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class ProxiedEventStoreApplication {

        @Bean
        static EventStoreProxier eventStoreProxier() {
            return new EventStoreProxier();
        }

        @Bean
        StartupDirectWriter startupDirectWriter(EventStore eventStore, CloudEventConverter<Written> converter) {
            return new StartupDirectWriter(eventStore, converter);
        }

        @Bean
        NowReceiver nowReceiver() {
            return new NowReceiver();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EventConverters {

        @Bean
        CloudEventTypeMapper<Written> writtenCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<Written> writtenCloudEventConverter(CloudEventTypeMapper<Written> typeMapper) {
            return new JacksonCloudEventConverter.Builder<Written>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(Written::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }
    }

    record Written(String eventId, Date timestamp, String name) {
        Written(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
