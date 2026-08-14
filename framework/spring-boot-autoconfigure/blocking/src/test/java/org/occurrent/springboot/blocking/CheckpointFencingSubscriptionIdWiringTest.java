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
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link CheckpointStorage#evaluatesWriteConditions()} answering true is not the end of what
 * {@link CheckpointFencingConfigurationCheck} asks. It also asks
 * {@link CheckpointStorage#evaluatesWriteConditionsFor(String)} for every subscription id an annotation on the
 * classpath declares, before {@link OccurrentBlockingAnnotationBeanPostProcessor} registers anything. Neither
 * fixture projection's factory method is ever invoked in the tests below, since the fencing check throws first, so
 * this needs none of the reader, converter or store beans {@link ProjectionAnnotationFencingWiringTest} wires for a
 * projection that actually catches up.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CheckpointFencingSubscriptionIdWiringTest {

    private static final String SUBSCRIPTION_ID_A = "proj-fenced-a";
    private static final String SUBSCRIPTION_ID_B = "proj-fenced-b";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(TwoFencedProjectionsConfiguration.class);

    @Test
    void a_storage_that_evaluates_conditions_globally_but_refuses_one_declared_id_fails_startup_naming_the_storage_and_the_id() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.evaluatesWriteConditions()).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(SUBSCRIPTION_ID_A)).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(SUBSCRIPTION_ID_B)).thenReturn(false);

        runner.withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).getFailure()
                            .isInstanceOf(CheckpointStorageCannotFenceSubscriptionException.class)
                            .hasMessageContaining("CheckpointStorage$MockitoMock")
                            .hasMessageContaining(SUBSCRIPTION_ID_B)
                            .hasMessageNotContaining(SUBSCRIPTION_ID_A);
                    // Neither projection's factory method was ever invoked, since the check throws before either is
                    // registered, so a checkpoint write for the id that IS supported never happens either.
                    verify(checkpointStorage, never()).save(any(), any(), any());
                });
    }

    @Test
    void every_declared_id_the_storage_refuses_is_collected_into_one_exception() {
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        CheckpointStorage checkpointStorage = mock(CheckpointStorage.class);
        when(checkpointStorage.evaluatesWriteConditions()).thenReturn(true);
        when(checkpointStorage.evaluatesWriteConditionsFor(any())).thenReturn(false);

        runner.withBean(CompetingConsumerStrategy.class, () -> strategy)
                .withBean(CheckpointStorage.class, () -> checkpointStorage)
                .run(context -> {
                    assertThat(context).getFailure()
                            .isInstanceOf(CheckpointStorageCannotFenceSubscriptionException.class)
                            .hasMessageContaining(SUBSCRIPTION_ID_A)
                            .hasMessageContaining(SUBSCRIPTION_ID_B);
                    verify(checkpointStorage, never()).save(any(), any(), any());
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class TwoFencedProjectionsConfiguration {
        @Bean
        TwoFencedProjections twoFencedProjections() {
            return new TwoFencedProjections();
        }
    }

    static class TwoFencedProjections {
        @Projection(id = SUBSCRIPTION_ID_A, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projectionA() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }

        @Projection(id = SUBSCRIPTION_ID_B, source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projectionB() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    record TestEvent(String eventId) {
    }
}
