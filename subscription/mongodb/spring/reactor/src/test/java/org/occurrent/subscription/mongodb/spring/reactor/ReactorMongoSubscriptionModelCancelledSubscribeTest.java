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

package org.occurrent.subscription.mongodb.spring.reactor;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.internal.CancellableRegistration;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A subscribe with whether the call was cancelled, see {@link org.occurrent.subscription.api.reactor.internal.CancellableRegistration},
 * on a model that is shut down. The model is over a mock, as a shut-down model never reaches MongoDB.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelCancelledSubscribeTest {

    @Test
    void throws_when_the_call_was_cancelled_and_the_model_is_shut_down() {
        // Given
        ReactorMongoSubscriptionModel model = new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING);
        model.shutdown();

        // When
        Throwable thrown = catchThrowable(() -> model.subscribe("someSubscription", null, StartAt.now(), __ -> Mono.empty(), () -> true));

        // Then
        assertThat(thrown).as("what the call threw").isInstanceOf(SubscriptionModelShutdownException.class);
    }

    @Test
    void returns_null_without_throwing_when_the_call_was_cancelled_and_the_id_is_taken() {
        // Given
        ReactiveMongoOperations operations = mock(ReactiveMongoOperations.class);
        when(operations.executeCommand(any(Document.class))).thenReturn(Mono.never());
        ReactorMongoSubscriptionModel model = new ReactorMongoSubscriptionModel(operations, "events", TimeRepresentation.RFC_3339_STRING);
        model.subscribe("someSubscription", null, StartAt.now(), __ -> Mono.empty(), () -> false);

        // When
        AtomicReference<Subscription> returned = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> returned.set(model.subscribe("someSubscription", null, StartAt.now(), __ -> Mono.empty(), () -> true)));

        // Then
        assertAll(
                () -> assertThat(thrown).as("what the call threw").isNull(),
                () -> assertThat(returned.get()).as("the subscription returned").isNull());
        model.shutdown();
    }

    @Test
    void stands_in_for_the_named_subscribe_for_the_shipped_model_called_through_itself() {
        // Given
        ReactorMongoSubscriptionModel model = new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING);

        // When
        boolean standsIn = model.standsInForSubscribe(model);

        // Then
        assertThat(standsIn).as("whether the shipped model stands in for its named subscribe").isTrue();
    }

    @Test
    void does_not_stand_in_for_the_named_subscribe_for_a_subclass_that_overrides_nothing() {
        // Given
        ReactorMongoSubscriptionModel subclass = new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING) {
        };

        // When
        boolean standsIn = subclass.standsInForSubscribe(subclass);

        // Then
        assertThat(standsIn).as("whether a subclass overriding nothing stands in for the named subscribe").isFalse();
    }

    @Test
    void does_not_stand_in_for_the_named_subscribe_for_a_reference_other_than_itself() {
        // Given
        ReactorMongoSubscriptionModel model = new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING);

        // When
        boolean standsIn = model.standsInForSubscribe(new Object());

        // Then
        assertThat(standsIn).as("whether the shipped model stands in for a call made through another reference").isFalse();
    }

    @Test
    void does_not_stand_in_for_the_named_subscribe_to_a_cglib_proxy_of_the_shipped_model() {
        // Given
        ProxyFactory factory = new ProxyFactory(new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING));
        factory.setProxyTargetClass(true);
        factory.addAdvice((MethodInterceptor) MethodInvocation::proceed);
        ReactorMongoSubscriptionModel proxy = (ReactorMongoSubscriptionModel) factory.getProxy();

        // When
        boolean standsIn = proxy.standsInForSubscribe(proxy);

        // Then
        assertThat(standsIn).as("whether a CGLIB proxy of the shipped model stands in for its named subscribe").isFalse();
    }

    @Test
    void does_not_stand_in_for_the_named_subscribe_to_a_jdk_proxy_of_the_shipped_model() {
        // Given
        ProxyFactory factory = new ProxyFactory(new ReactorMongoSubscriptionModel(mock(ReactiveMongoOperations.class), "events", TimeRepresentation.RFC_3339_STRING));
        factory.setProxyTargetClass(false);
        factory.addAdvice((MethodInterceptor) MethodInvocation::proceed);
        CancellableRegistration proxy = (CancellableRegistration) factory.getProxy();

        // When
        boolean standsIn = proxy.standsInForSubscribe(proxy);

        // Then
        assertThat(standsIn).as("whether a JDK proxy of the shipped model stands in for its named subscribe").isFalse();
    }
}
