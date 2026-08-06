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

package org.occurrent.springboot.mongo.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Saga;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression coverage for #563: a non-push {@code @Saga} resolved its {@code Subscribable} with a bare
 * {@code getBean(Subscribable.class)}, the same shape #541 fixed for the subscription DSLs (see
 * {@link OccurrentMongoAsynchronousSubscribableResolutionTest}). An application-declared asynchronous subscription
 * model, with no {@code @Primary}, failed the context the same way once a non-push saga registered: the starter's own
 * asynchronous model steps aside for it, leaving the application's bean and the starter's register-only
 * {@code SynchronousSubscriptionModel} as the two {@code Subscribable} beans in the context, with neither marked
 * {@code @Primary}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationAsynchronousSubscribableResolutionTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentMongoAutoConfiguration.class))
            .withBean(MongoDatabaseFactory.class, () -> mock(MongoDatabaseFactory.class))
            .withBean(MongoTemplate.class, () -> mock(MongoTemplate.class))
            .withPropertyValues(
                    "occurrent.event-store.enabled=false",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test",
                    // Sidesteps needing a CompetingConsumerStrategy bean, which is irrelevant to what this test characterizes.
                    "occurrent.saga.competing-consumer.enabled=false"
            );

    @Test
    void an_application_supplied_asynchronous_subscription_model_without_primary_still_starts_and_a_non_push_saga_binds_to_it() {
        Subscription subscription = mock(Subscription.class);
        SubscriptionModel own = mock(SubscriptionModel.class);
        when(own.subscribe(any(), any(), any(), any())).thenReturn(subscription);

        contextRunner.withBean(SubscriptionModel.class, () -> own)
                .withUserConfiguration(SagaConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    // Proves the saga bound to the application's own bean, not (say) silently discarding the
                    // registration or resolving to the register-only SynchronousSubscriptionModel that also
                    // satisfies Subscribable.
                    verify(own).subscribe(eq("saga-resolution-test"), any(), any(StartAt.class), any());
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class SagaConfiguration {
        @Bean
        SagaStateStore<TestState> sagaStateStore() {
            return SagaStateStore.inMemory();
        }

        @Bean
        CommandDispatcher<TestCommand> commandDispatcher() {
            return command -> {
            };
        }

        @Bean
        SagaHolder sagaHolder() {
            return new SagaHolder();
        }
    }

    static class SagaHolder {
        @Saga(id = "saga-resolution-test")
        org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> saga() {
            return org.occurrent.dsl.saga.Saga.<TestEvent, TestState, TestCommand>builder(new TestState())
                    .correlateAll(event -> "k")
                    .startsOn(TestEvent.class)
                    .build();
        }
    }

    record TestState() {
    }

    record TestEvent(String eventId) {
    }

    record TestCommand() {
    }
}
