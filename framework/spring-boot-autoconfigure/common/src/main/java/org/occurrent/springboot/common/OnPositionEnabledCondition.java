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

package org.occurrent.springboot.common;

import org.jspecify.annotations.NonNull;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Set;

import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Matches when the auto-configured event store writes a global position. That is the case when DCB is enabled (DCB
 * always writes position) or when STREAM is enabled with {@code occurrent.event-store.stream.position} on (the
 * default). Use it to gate beans that only make sense when the store writes position.
 */
public class OnPositionEnabledCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
        Binder binder = Binder.get(context.getEnvironment());
        Set<EventStoreCapability> capabilities = binder
                .bind("occurrent.event-store.capabilities", Bindable.setOf(EventStoreCapability.class))
                .orElse(Set.of(STREAM));
        if (capabilities.contains(DCB)) {
            return true;
        }
        boolean streamPositionEnabled = binder
                .bind("occurrent.event-store.stream.position", Bindable.of(Boolean.class))
                .orElse(Boolean.TRUE);
        return capabilities.contains(STREAM) && streamPositionEnabled;
    }
}
