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

import kotlin.Unit;
import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression coverage for #541: an application-declared asynchronous subscription model, with no {@code @Primary},
 * used to fail the context. The starter's own asynchronous model correctly steps aside for it
 * ({@code @ConditionalOnMissingBean(value = SubscriptionModel.class, ignored = RegisteringSubscribable.class)} on
 * {@link OccurrentMongoAutoConfiguration#occurrentCompetingDurableSubscriptionModel}), which leaves this bean and the
 * starter's register-only {@code SynchronousSubscriptionModel} as the two {@code Subscribable} beans in the context,
 * and only the starter's own (now absent) asynchronous model was ever marked {@code @Primary}. The subscription DSL
 * asked for "the" {@code Subscribable} by type, which Spring could no longer resolve, so the context failed to start.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class OccurrentMongoAsynchronousSubscribableResolutionTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentMongoAutoConfiguration.class))
            .withBean(MongoDatabaseFactory.class, () -> mock(MongoDatabaseFactory.class))
            .withBean(MongoTemplate.class, () -> mock(MongoTemplate.class))
            .withPropertyValues(
                    "occurrent.event-store.enabled=false",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test"
            );

    @Test
    void an_application_supplied_asynchronous_subscription_model_without_primary_still_starts_and_the_dsl_binds_to_it() {
        SubscriptionHandle subscription = mock(SubscriptionHandle.class);
        SubscriptionModel own = mock(SubscriptionModel.class);
        when(own.subscribe(any(), any(), any(), any())).thenReturn(subscription);

        contextRunner.withBean(SubscriptionModel.class, () -> own).run(context -> {
            assertThat(context).hasNotFailed();

            @SuppressWarnings("unchecked")
            Subscriptions<TestEvent> subscriptions = (Subscriptions<TestEvent>) context.getBean(Subscriptions.class);

            Function2<EventMetadata, TestEvent, Unit> consumer = (metadata, event) -> Unit.INSTANCE;
            subscriptions.subscribe("test-subscription", AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.subscriptionModelDefault(), false, consumer);

            // Proves the DSL bound to the application's own bean, not (say) silently discarding the call or
            // resolving to the register-only SynchronousSubscriptionModel that also satisfies Subscribable.
            // StartAt.Default has no equals() (each subscriptionModelDefault() call allocates a new instance), so
            // that argument is matched by type rather than by value.
            verify(own).subscribe(eq("test-subscription"), any(), any(StartAt.class), any());
        });
    }

    record TestEvent(String eventId) {
    }
}
