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

package org.occurrent.springboot.common;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Resolves the asynchronous {@code Subscribable} bean the subscription DSLs bind to, on either stack.
 * <p>
 * Both starters contribute a register-only {@code Subscribable} alongside the durable/competing asynchronous model:
 * the synchronous subscription model always, and on the blocking stack a push-registered one an application may add
 * per consumer. A register-only implementation is a {@code Subscribable} too, so asking Spring for "the"
 * {@code Subscribable} by type stops being unambiguous the moment an application supplies its own asynchronous
 * model: the starter's own asynchronous model correctly steps aside for it ({@code @ConditionalOnMissingBean}),
 * which leaves the application's bean and the register-only one as the two remaining {@code Subscribable}s, and only
 * the starter's own (now absent) bean was ever marked {@code @Primary}.
 * <p>
 * Narrowing to non-register-only candidates first, the same way the annotation registrars narrow a push feed bean in
 * {@link SubscriptionAnnotations#resolveFeedBean}, resolves this with no configuration from the application: a
 * user-declared asynchronous model is picked up automatically, without needing {@code @Primary} itself.
 *
 * @see SubscriptionAnnotations#resolveFeedBean
 */
public final class AsynchronousSubscribables {

    private AsynchronousSubscribables() {
    }

    /**
     * @param applicationContext     the Spring context to resolve beans from
     * @param subscribableType       the stack's {@code Subscribable} interface
     * @param registeringMarkerType  the stack's {@code RegisteringSubscribable} base class, excluded from the
     *                               candidates since it never stands in for the asynchronous model (see class javadoc)
     * @return the single non-register-only {@code Subscribable} bean
     * @throws NoSuchBeanDefinitionException if there is none
     * @throws org.springframework.beans.factory.NoUniqueBeanDefinitionException if there are several and none is
     *                                                                            {@code @Primary}, thrown by the
     *                                                                            container's own by-type resolution
     */
    public static <S> S resolve(ApplicationContext applicationContext, Class<S> subscribableType, Class<?> registeringMarkerType) {
        // isTypeMatch decides from bean-definition metadata, not by instantiating the bean, so a candidate this
        // starter does not end up choosing (or one declared @Lazy or prototype-scoped) is never eagerly created just
        // to be filtered out here.
        List<String> candidates = new ArrayList<>();
        for (String name : applicationContext.getBeanNamesForType(subscribableType)) {
            if (!applicationContext.isTypeMatch(name, registeringMarkerType)) {
                candidates.add(name);
            }
        }
        if (candidates.size() == 1) {
            return applicationContext.getBean(candidates.get(0), subscribableType);
        }
        if (candidates.isEmpty()) {
            throw new NoSuchBeanDefinitionException(subscribableType,
                    "an asynchronous bean (excluding " + registeringMarkerType.getSimpleName() + " implementations)");
        }
        // Several non-register-only candidates: let the container apply its own @Primary resolution over the full
        // Subscribable set before giving up, so an application that has deliberately marked one asynchronous model
        // @Primary keeps working the way it always could.
        return applicationContext.getBean(subscribableType);
    }
}
