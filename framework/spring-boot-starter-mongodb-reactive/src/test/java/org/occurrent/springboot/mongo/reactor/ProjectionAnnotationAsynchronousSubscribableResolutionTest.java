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

package org.occurrent.springboot.mongo.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression coverage for #563: the non-push, non-synchronous {@code @Projection} branch (the default projection
 * shape) resolved its {@code Subscribable} with a bare {@code getBean(Subscribable.class)}, the same shape #541 fixed
 * for the subscription DSLs (see {@link OccurrentReactiveMongoAsynchronousSubscribableResolutionTest}). An
 * application-declared asynchronous subscription model, with no {@code @Primary}, failed the context the same way
 * once such a projection registered: the starter's own asynchronous model steps aside for it, leaving the
 * application's bean and the starter's register-only {@code SynchronousSubscriptionModel} as the two
 * {@code Subscribable} beans in the context, with neither marked {@code @Primary}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationAsynchronousSubscribableResolutionTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentReactiveMongoAutoConfiguration.class))
            .withBean(ReactiveMongoDatabaseFactory.class, () -> mock(ReactiveMongoDatabaseFactory.class))
            .withBean(ReactiveMongoTemplate.class, () -> mock(ReactiveMongoTemplate.class))
            .withPropertyValues(
                    "occurrent.event-store.enabled=false",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test"
            );

    @Test
    void an_application_supplied_asynchronous_subscription_model_without_primary_still_starts_and_a_default_projection_binds_to_it() {
        SubscriptionHandle subscription = mock(SubscriptionHandle.class);
        when(subscription.waitUntilStarted()).thenReturn(Mono.empty());
        SubscriptionModel own = mock(SubscriptionModel.class);
        when(own.subscribe(any(), any(), any(), any())).thenReturn(subscription);

        contextRunner.withBean(SubscriptionModel.class, () -> own)
                .withUserConfiguration(ProjectionConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    // Proves the projection bound to the application's own bean, not (say) silently discarding the
                    // registration or resolving to the register-only SynchronousSubscriptionModel that also
                    // satisfies Subscribable.
                    verify(own).subscribe(eq("projection-resolution-test"), any(), any(StartAt.class), any());
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class ProjectionConfiguration {
        @Bean
        ViewStateRepository<Integer, String> store() {
            ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
            return ViewStateRepository.create(map::get, map::put);
        }

        @Bean
        ProjectionHolder projectionHolder() {
            return new ProjectionHolder();
        }
    }

    static class ProjectionHolder {
        @Projection(id = "projection-resolution-test")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    record TestEvent(String eventId) {
    }
}
