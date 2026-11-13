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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A catch-up that goes straight to live over a Spring AOP proxy of {@code ReactorMongoSubscriptionModel} calls the
 * four-argument {@code subscribe(..)} through the proxy, so advice on it runs. The model is over a stub that never
 * answers, so no MongoDB is needed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupOverAProxyOfMongoModelTest {

    private static final Function<CloudEvent, Mono<Void>> NO_ACTION = __ -> Mono.empty();
    private static final String NAMED_SUBSCRIBE = "subscribe(String, SubscriptionFilter, StartAt, Function)";

    private final List<SubscriptionModelLifeCycle> started = new CopyOnWriteArrayList<>();

    @AfterEach
    void shut_down_the_models_the_test_started() {
        started.forEach(SubscriptionModelLifeCycle::shutdown);
    }

    @Test
    void calls_the_named_subscribe_through_a_cglib_proxy_of_the_model() {
        // Given
        SubscribesSeen advice = new SubscribesSeen();
        ReactorMongoSubscriptionModel proxy = proxyOf(true, advice);

        // When
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(proxy, new NoReplay());
        started.add(catchup);
        Subscription subscription = catchup
                .subscribe("x", StreamSubscriptionFilter.filter(Filter.all()), StartAt.now(), NO_ACTION);

        // Then
        assertAll(
                () -> assertThat(subscription).as("the subscription returned").isNotNull(),
                () -> assertThat(advice.subscribes).as("the subscribes the advice of the CGLIB proxy saw").containsExactly(NAMED_SUBSCRIBE));
    }

    @Test
    void calls_the_named_subscribe_through_a_jdk_proxy_of_the_model() {
        // Given
        SubscribesSeen advice = new SubscribesSeen();
        CheckpointAwareSubscriptionModel proxy = proxyOf(false, advice);

        // When
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(proxy, new NoReplay());
        started.add(catchup);
        Subscription subscription = catchup
                .subscribe("x", StreamSubscriptionFilter.filter(Filter.all()), StartAt.now(), NO_ACTION);

        // Then
        assertAll(
                () -> assertThat(subscription).as("the subscription returned").isNotNull(),
                () -> assertThat(advice.subscribes).as("the subscribes the advice of the JDK proxy saw").containsExactly(NAMED_SUBSCRIBE));
    }

    // A Spring AOP proxy of the shipped model, as a Spring container builds one
    @SuppressWarnings("unchecked")
    private <T> T proxyOf(boolean proxyTargetClass, MethodInterceptor advice) {
        ProxyFactory factory = new ProxyFactory(new ReactorMongoSubscriptionModel(neverAnswering(), "events", TimeRepresentation.RFC_3339_STRING));
        factory.setProxyTargetClass(proxyTargetClass);
        factory.addAdvice(advice);
        Object proxy = factory.getProxy();
        started.add((SubscriptionModelLifeCycle) proxy);
        return (T) proxy;
    }

    // Answers the two calls a subscribe at now makes with a Publisher that never emits, and refuses every other call
    private static ReactiveMongoOperations neverAnswering() {
        return (ReactiveMongoOperations) Proxy.newProxyInstance(ReactiveMongoOperations.class.getClassLoader(), new Class<?>[]{ReactiveMongoOperations.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "executeCommand" -> Mono.never();
                    case "changeStream" -> Flux.never();
                    case "toString" -> "ReactiveMongoOperations that never answers";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.toString());
                });
    }

    // Records the subscribe(..) calls that pass through the proxy, by their parameter types
    private static final class SubscribesSeen implements MethodInterceptor {
        final List<String> subscribes = new CopyOnWriteArrayList<>();

        @Override
        public @Nullable Object invoke(MethodInvocation invocation) throws Throwable {
            if (invocation.getMethod().getName().equals("subscribe")) {
                String parameters = Arrays.stream(invocation.getMethod().getParameterTypes()).map(Class::getSimpleName).collect(joining(", "));
                subscribes.add("subscribe(" + parameters + ")");
            }
            return invocation.proceed();
        }
    }

    // A subscribe at StartAt.now() goes straight to live, so nothing is replayed
    private static final class NoReplay implements UnscopedPositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("Nothing is replayed"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(0L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }
}
