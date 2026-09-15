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

package org.occurrent.springboot.reactor;

import io.cloudevents.CloudEvent;
import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A bean built after startup registers its handlers all or none. The handlers are handed to the registrar in a fixed
 * order here, the valid one first, because the order {@code getDeclaredMethods} returns them in varies between JVM
 * runs, and a refused handler that happens to come first proves nothing about the ones after it. Reactive counterpart
 * of the blocking {@code SubscriptionAnnotationRegistrarTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SubscriptionAnnotationRegistrarTest {

    @Test
    @SuppressWarnings("unchecked")
    void a_handler_asking_for_two_start_positions_leaves_the_valid_handler_before_it_unsubscribed_and_unreserved() throws Exception {
        ApplicationContext context = mock(ApplicationContext.class);
        Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
        when(context.getBean(CloudEventConverter.class)).thenReturn(new NoopCloudEventConverter());
        when(context.getBean(Subscriptions.class)).thenReturn(subscriptions);
        SubscriptionAnnotationRegistrar registrar = new SubscriptionAnnotationRegistrar(context, mock(StartPositionSupport.class));

        TwoStartPositionsSubscriber bean = new TwoStartPositionsSubscriber();
        List<Method> validFirst = List.of(
                TwoStartPositionsSubscriber.class.getDeclaredMethod("valid", TestEvent.class),
                TwoStartPositionsSubscriber.class.getDeclaredMethod("twoStartPositions", TestEvent.class));
        Set<Method> reservedHandlers = ConcurrentHashMap.newKeySet();
        Set<String> claimedIds = ConcurrentHashMap.newKeySet();

        assertThatThrownBy(() -> registrar.registerSubscriptions(bean, validFirst, () -> bean, false,
                reservedHandlers::add,
                id -> {
                    if (!claimedIds.add(id)) {
                        throw new DuplicateSubscriptionIdException(id);
                    }
                },
                reservedHandlers::remove, claimedIds::remove))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not both");

        verify(subscriptions, never()).subscribe(any(String.class), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
        assertThat(reservedHandlers).isEmpty();
        assertThat(claimedIds).isEmpty();
    }

    static class TwoStartPositionsSubscriber {
        @Subscription(id = "valid-beside-two-start-positions")
        void valid(TestEvent event) {
        }

        @Subscription(id = "two-start-positions", startAt = StartPosition.BEGINNING, startAtGlobalPosition = 0)
        void twoStartPositions(TestEvent event) {
        }
    }

    record TestEvent() {
    }

    static class NoopCloudEventConverter implements CloudEventConverter<TestEvent> {
        @Override
        public CloudEvent toCloudEvent(TestEvent domainEvent) {
            return null;
        }

        @Override
        public TestEvent toDomainEvent(CloudEvent cloudEvent) {
            return null;
        }

        @Override
        public String getCloudEventType(Class<? extends TestEvent> type) {
            return type.getSimpleName();
        }
    }
}
