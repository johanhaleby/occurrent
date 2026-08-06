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

import org.jspecify.annotations.NonNull;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches unless subscriptions are {@link SubscriptionMode#DISABLED}. Applies to every subscription bean, so
 * {@link SubscriptionMode#MANUAL} still creates them all, just stopped.
 */
public class OnSubscriptionsNotDisabledCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
        return subscriptionMode(context) != SubscriptionMode.DISABLED;
    }

    static SubscriptionMode subscriptionMode(ConditionContext context) {
        Binder binder = Binder.get(context.getEnvironment());
        SubscriptionMode mode = binder.bind("occurrent.subscription.mode", Bindable.of(SubscriptionMode.class)).orElse(null);
        Boolean enabled = binder.bind("occurrent.subscription.enabled", Bindable.of(Boolean.class)).orElse(null);
        return SubscriptionMode.resolve(mode, enabled);
    }
}
