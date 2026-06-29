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

import org.jspecify.annotations.NonNull;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Set;

import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

class OnStreamEventStoreCapabilityCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
        Set<EventStoreCapability> capabilities = Binder.get(context.getEnvironment())
                .bind("occurrent.event-store.capabilities", Bindable.setOf(EventStoreCapability.class))
                .orElse(Set.of(STREAM));
        return capabilities.contains(STREAM);
    }
}
