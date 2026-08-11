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

import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;

import java.util.Collection;
import java.util.List;

/**
 * Thrown when an application has several {@link CompetingConsumerStrategy} beans and none of them is {@code @Primary},
 * which leaves Occurrent without a lease to fence a checkpoint write or the saga timer poller with.
 */
public final class AmbiguousCompetingConsumerStrategyException extends IllegalStateException {

    private final List<String> beanNames;

    // No cause, deliberately. The Spring exception that reports the same ambiguity would then be the root cause of
    // this one, and a reader who looks only at the root cause would miss the remedy this message names.
    AmbiguousCompetingConsumerStrategyException(Collection<String> beanNames) {
        super(message(beanNames));
        this.beanNames = List.copyOf(beanNames);
    }

    private static String message(Collection<String> beanNames) {
        String found = beanNames.isEmpty() ? "" : " The beans found are %s.".formatted(beanNames);
        return ("Found more than one %s bean and none of them is marked @Primary, so Occurrent cannot tell which lease " +
                "to fence checkpoint writes and the saga timer poller with.%s Mark the one to use with @Primary, or " +
                "leave only that one in the application context.")
                .formatted(CompetingConsumerStrategy.class.getSimpleName(), found);
    }

    /**
     * The names of the {@link CompetingConsumerStrategy} beans that were found.
     *
     * @return The bean names, in the order the application context reported them.
     */
    public List<String> getBeanNames() {
        return beanNames;
    }
}
