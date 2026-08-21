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

package org.occurrent.springboot.blocking;

import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.slf4j.Logger;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Publishes the {@link CatchupThenPushSubscriptionModel} a {@code @Projection(source = PUSH)} or
 * {@code @Saga(source = PUSH)} registrar builds internally as a named Spring bean, so a
 * {@code RabbitMqCloudEventBridge} or {@code KafkaCloudEventBridge} the application wires separately (in a
 * different, deliberately decoupled starter module, per ADR 133 decision 1) can look it up and pass
 * {@code model::isReadyForLiveDelivery} to the bridge builder's {@code readinessSource(...)}. Without this, the
 * model these registrars build is kept only in a private list, reachable by nothing outside the registrar itself.
 * <p>
 * Shared between {@link ProjectionAnnotationRegistrar} and {@link SagaAnnotationRegistrar}, which each build one of
 * these the same way, so the bean-name format and the registration failure modes cannot drift apart between the two.
 * Mirrors {@code SagaAnnotationRegistrar#registerSagaInstancesSingleton}: a singleton registered directly on the bean
 * factory rather than a bean definition, since a {@code @Projection}/{@code @Saga} factory method only runs once its
 * collaborators are wired, which is after the context has already refreshed.
 */
final class CatchupThenPushSubscriptionModelPublisher {

    private CatchupThenPushSubscriptionModelPublisher() {
    }

    /**
     * Publish {@code model} as {@code "catchupThenPushSubscriptionModel-" + id}, or log and skip if
     * {@code applicationContext} is not a {@link ConfigurableApplicationContext}.
     *
     * @throws IllegalStateException if a bean with that name already exists
     */
    static void publish(ApplicationContext applicationContext, String id, CatchupThenPushSubscriptionModel model, Logger log) {
        String beanName = beanName(id);
        if (!(applicationContext instanceof ConfigurableApplicationContext configurableContext)) {
            // Every Spring Boot context is configurable, so this is only reachable from an exotic harness. The
            // catch-up itself keeps running fine either way. Only the ability to reach this exact object by bean
            // name for readinessSource(...) wiring is lost, and a hand-held CatchupThenPushSubscriptionModel
            // reference still works for a caller not going through Spring at all.
            log.warn("Cannot publish '{}' because the application context is not a ConfigurableApplicationContext. " +
                    "Wire the CatchupThenPushSubscriptionModel this registered by hand instead.", beanName);
            return;
        }
        ConfigurableListableBeanFactory beanFactory = configurableContext.getBeanFactory();
        if (beanFactory.containsBean(beanName)) {
            // registerSingleton would throw from inside afterSingletonsInstantiated, which fails startup with a
            // message that says nothing about push catch-up. The name is documented API, so a collision means two
            // different things claim it. Say which id and which name rather than letting Spring report a bare
            // duplicate-singleton error.
            throw new IllegalStateException("Cannot publish the CatchupThenPushSubscriptionModel for '%s' as '%s' because a bean with that name already exists. Occurrent publishes each push catch-up's wrapper under 'catchupThenPushSubscriptionModel-<id>', so rename your bean or the id.".formatted(id, beanName));
        }
        beanFactory.registerSingleton(beanName, model);
    }

    /** The bean name the {@link CatchupThenPushSubscriptionModel} for {@code id} is published under. */
    static String beanName(String id) {
        return "catchupThenPushSubscriptionModel-" + id;
    }
}
