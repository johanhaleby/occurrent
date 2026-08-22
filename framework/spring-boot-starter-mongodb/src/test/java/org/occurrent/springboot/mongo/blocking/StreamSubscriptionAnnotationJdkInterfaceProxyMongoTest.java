/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.blocking;

import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that a {@link StreamSubscription} handler still receives events when the {@code ApplicationContext} hands
 * the registrar a JDK interface proxy at delivery time rather than an instance of the concrete bean class the
 * handler method was captured from at registration time. {@link JdkProxyPostProcessorConfiguration} wraps the
 * subscriber bean in a genuine JDK proxy implementing only {@link Marker}, the same shape a real advisor leaves an
 * interface-proxied bean in under {@code spring.aop.proxy-target-class=false} (mirrors
 * {@code ProjectionAnnotationJdkProxyTest}'s fixture for the sibling #836 defect). Invoking the handler method on
 * that proxy directly throws {@code IllegalArgumentException}, so the registrar has to fall back to the raw bean
 * instead of losing the delivery.
 */
@DisplayName("StreamSubscription handler on a JDK interface proxy")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = StreamSubscriptionAnnotationJdkInterfaceProxyMongoTest.AnnotationApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:stream-subscription-jdk-interface-proxy-test"
        }
)
@Import(StreamSubscriptionAnnotationJdkInterfaceProxyMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class StreamSubscriptionAnnotationJdkInterfaceProxyMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:stream-subscription-jdk-interface-proxy-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private org.springframework.context.ApplicationContext applicationContext;

    @Test
    void the_handler_bean_is_a_JDK_interface_proxy_and_still_receives_the_event() {
        // Confirms the fixture exercises a real JDK interface proxy rather than degrading into an ordinary bean
        // that would pass this test for the wrong reason.
        assertThat(AopUtils.isJdkDynamicProxy(applicationContext.getBean("streamAnnotatedSubscriber"))).isTrue();

        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("stream-1")));

        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(StreamAnnotatedSubscriber.received()).extracting(TestEvent::name).contains("stream-1"));
    }

    // --- inner application and configuration classes ---

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class AnnotationApplication {

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

        @Bean
        StreamAnnotatedSubscriber streamAnnotatedSubscriber() {
            return new StreamAnnotatedSubscriber();
        }

        // Wraps the Marker-implementing subscriber bean in a genuine JDK interface proxy after the registrar's own
        // postProcessBeforeInitialization scan has already run against the raw bean, the same shape a real advisor
        // leaves an interface-proxied bean in under spring.aop.proxy-target-class=false.
        @Bean
        static BeanPostProcessor jdkInterfaceProxyPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof Marker)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setInterfaces(Marker.class);
                    proxyFactory.setProxyTargetClass(false);
                    proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    // Implemented by the subscriber bean and nothing else, so a JDK proxy of it can never carry the handler method:
    // that mismatch is the whole mechanism under test.
    interface Marker {
    }

    static class StreamAnnotatedSubscriber implements Marker {
        private static final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "jdk-interface-proxy-stream-subscriber")
        void on(TestEvent event) {
            received.add(event);
        }

        static List<TestEvent> received() {
            return received;
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
