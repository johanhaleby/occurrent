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

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Collection;
import java.util.List;

/**
 * The one place that decides which {@link CompetingConsumerStrategy} bean Occurrent fences with, so the check made at
 * startup and the resolution made on the first checkpoint write can never disagree.
 * <p>
 * Public so a store starter's own auto-configuration (outside this module) resolves the strategy the same way, and
 * fails with the same message, rather than letting a plain injection point report the ambiguity in Spring's words
 * without the remedy. {@link CompetingConsumerCheckpointWriteVersionSource} is public for the same reason.
 */
public final class CompetingConsumerStrategies {

    private CompetingConsumerStrategies() {
    }

    /**
     * The single strategy bean to fence with, or {@code null} when the application has none.
     *
     * @param provider Resolves the {@link CompetingConsumerStrategy} beans to choose between.
     * @return The one strategy to use, or {@code null} if the application declares none.
     * @throws AmbiguousCompetingConsumerStrategyException if several are declared and none of them is {@code @Primary}
     */
    public static @Nullable CompetingConsumerStrategy resolveUnique(ObjectProvider<CompetingConsumerStrategy> provider) {
        CompetingConsumerStrategy resolved = provider.getIfUnique();
        if (resolved != null) {
            return resolved;
        }
        // getIfUnique() answers null both for no bean at all and for several with no @Primary, and disabled safety is
        // not an acceptable reading of the second. getIfAvailable() tells them apart, since it throws for that case
        // and names the beans it found. A @Fallback bean, which is how a store starter declares its default, stops
        // being a candidate for either call once the application declares one of its own.
        try {
            return provider.getIfAvailable();
        } catch (NoUniqueBeanDefinitionException e) {
            Collection<String> beanNames = e.getBeanNamesFound();
            throw new AmbiguousCompetingConsumerStrategyException(beanNames == null ? List.of() : beanNames);
        }
    }
}
