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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.springframework.beans.factory.BeanCurrentlyInCreationException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The rules for the lazy resolution every wiring site relies on, isolated from Spring context wiring
 * (which {@code ProjectionAnnotationFencingWiringTest}, {@code SagaAnnotationFencingWiringTest} and
 * {@code CompetingConsumerFencingWiringTest} in the starter module each characterize for their own site).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerCheckpointWriteVersionSourceTest {

    private static final String SUBSCRIPTION_ID = "sub-1";

    @Test
    @SuppressWarnings("unchecked")
    void asks_the_provider_and_delegates_to_the_strategy_it_finds() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(5L));
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);
        when(provider.getIfUnique()).thenReturn(strategy);

        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, true);

        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEqualTo(OptionalLong.of(5L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void no_strategy_bean_answers_empty() {
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);
        when(provider.getIfUnique()).thenReturn(null);
        when(provider.getIfAvailable()).thenReturn(null);

        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, true);

        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void several_strategy_beans_refuse_the_write_rather_than_answering_empty() {
        // getIfUnique() answers null for an ambiguous match and getIfAvailable() throws for it, which is how this class
        // tells "no strategy at all" apart from "several and no @Primary". Modeled here as the provider reproducing
        // that contract, so the test is about reading the two answers correctly rather than re-testing Spring's own
        // resolution. The startup check refuses the same configuration before a write can reach this.
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);
        when(provider.getIfUnique()).thenReturn(null);
        when(provider.getIfAvailable()).thenThrow(new NoUniqueBeanDefinitionException(CompetingConsumerStrategy.class,
                List.of("occurrentCompetingConsumerStrategy", "myOwnStrategy")));

        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, true);

        assertThatThrownBy(() -> source.writeVersion(SUBSCRIPTION_ID))
                .isInstanceOf(AmbiguousCompetingConsumerStrategyException.class)
                .hasMessageContaining("myOwnStrategy")
                .hasMessageContaining("@Primary");
    }

    @Test
    @SuppressWarnings("unchecked")
    void a_resolution_that_throws_answers_empty_and_is_retried_on_the_next_write() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(9L));
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);
        // The strategy bean is still being built (see the class javadoc, a listener depending on a subscription
        // model would otherwise close a construction cycle), so the first resolution throws.
        when(provider.getIfUnique())
                .thenThrow(new BeanCurrentlyInCreationException("occurrentCompetingConsumerStrategy"))
                .thenReturn(strategy);

        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, true);

        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEmpty();
        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEqualTo(OptionalLong.of(9L));
        verify(provider, times(2)).getIfUnique();
    }

    @Test
    @SuppressWarnings("unchecked")
    void a_first_attempt_that_finds_nothing_is_retried_rather_than_disabling_the_fence_for_good() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(3L));
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);
        when(provider.getIfUnique()).thenReturn(null, strategy);
        when(provider.getIfAvailable()).thenReturn(null);

        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, true);

        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEmpty();
        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEqualTo(OptionalLong.of(3L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void a_strategy_once_found_is_remembered_rather_than_resolved_again() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(1L), OptionalLong.of(2L));
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);
        when(provider.getIfUnique()).thenReturn(strategy);

        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, true);

        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEqualTo(OptionalLong.of(1L));
        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEqualTo(OptionalLong.of(2L));
        // Both writes asked the strategy directly, but only the first write resolved it from the provider.
        verify(provider, times(1)).getIfUnique();
        verify(strategy, times(2)).fencingToken(SUBSCRIPTION_ID);
    }

    @Test
    @SuppressWarnings("unchecked")
    void never_asks_the_strategy_before_the_first_write() {
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);

        new CompetingConsumerCheckpointWriteVersionSource(provider, true);

        verify(provider, never()).getIfUnique();
    }

    @Test
    @SuppressWarnings("unchecked")
    void fencing_turned_off_answers_empty_without_asking_for_a_strategy() {
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);

        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, false);

        assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEmpty();
        verify(provider, never()).getIfUnique();
        verify(provider, never()).getIfAvailable();
    }
}
