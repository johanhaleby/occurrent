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

import org.junit.jupiter.api.Test;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerStatus;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerStatus.State;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.mongodb.blocking.ccs.LeaseRefreshFailure;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.springframework.boot.health.autoconfigure.registry.HealthContributorRegistryAutoConfiguration;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.health.registry.HealthContributorRegistry;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * The {@code competingConsumers} and {@code competingConsumerLeases} health contributors, without MongoDB. A subscription
 * is DOWN exactly when its competing consumer status is failing, so one that waits for a lease another node holds is UP.
 * A lease strategy is DOWN exactly while its latest lease refresh failed. What makes a status failing, and when a
 * refresh counts as failed, is tested where they are decided, so the models and strategies here answer what the test
 * gives them.
 */
class OccurrentMongoAutoConfigurationCompetingConsumerHealthTest {

    private static final Instant SINCE = Instant.parse("2026-11-19T10:15:30Z");

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(OccurrentMongoAutoConfiguration.HealthConfiguration.class)
            .withConfiguration(AutoConfigurations.of(HealthContributorRegistryAutoConfiguration.class));

    @Test
    void both_contributors_are_registered_under_their_names_when_spring_boot_health_is_present() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            HealthContributorRegistry registry = context.getBean(HealthContributorRegistry.class);
            assertThat(registry.getContributor("competingConsumers")).isSameAs(context.getBean("competingConsumersHealthContributor"));
            assertThat(registry.getContributor("competingConsumerLeases")).isSameAs(context.getBean("competingConsumerLeasesHealthContributor"));
        });
    }

    @Test
    void neither_contributor_is_configured_without_spring_boot_health() {
        new ApplicationContextRunner()
                .withUserConfiguration(OccurrentMongoAutoConfiguration.HealthConfiguration.class)
                .withClassLoader(new FilteredClassLoader(CompositeHealthContributor.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean("competingConsumersHealthContributor");
                    assertThat(context).doesNotHaveBean("competingConsumerLeasesHealthContributor");
                });
    }

    @Test
    void the_competingConsumers_contributor_is_absent_when_its_property_is_false_and_the_leases_one_stays() {
        contextRunner.withPropertyValues("management.health.competingconsumers.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean("competingConsumersHealthContributor");
            assertThat(context).hasBean("competingConsumerLeasesHealthContributor");
        });
    }

    @Test
    void the_competingConsumerLeases_contributor_is_absent_when_its_property_is_false_and_the_competingConsumers_one_stays() {
        contextRunner.withPropertyValues("management.health.competingconsumerleases.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean("competingConsumerLeasesHealthContributor");
            assertThat(context).hasBean("competingConsumersHealthContributor");
        });
    }

    @Test
    void an_application_bean_under_any_of_the_three_names_replaces_the_competingConsumers_contributor() {
        for (String name : List.of("competingConsumers", "competingConsumersHealthIndicator", "competingConsumersHealthContributor")) {
            HealthIndicator applicationIndicator = () -> Health.up().build();

            contextRunner.withBean(name, HealthIndicator.class, () -> applicationIndicator).run(context -> {
                assertThat(context).as(name).hasNotFailed();
                assertThat(context.getBeansOfType(CompetingConsumersHealthContributor.class)).as(name).isEmpty();
                assertThat(context.getBean(HealthContributorRegistry.class).getContributor("competingConsumers")).as(name).isSameAs(applicationIndicator);
            });
        }
    }

    @Test
    void an_application_bean_under_any_of_the_three_names_replaces_the_competingConsumerLeases_contributor() {
        for (String name : List.of("competingConsumerLeases", "competingConsumerLeasesHealthIndicator", "competingConsumerLeasesHealthContributor")) {
            HealthIndicator applicationIndicator = () -> Health.up().build();

            contextRunner.withBean(name, HealthIndicator.class, () -> applicationIndicator).run(context -> {
                assertThat(context).as(name).hasNotFailed();
                assertThat(context.getBeansOfType(CompetingConsumerLeasesHealthContributor.class)).as(name).isEmpty();
                assertThat(context.getBean(HealthContributorRegistry.class).getContributor("competingConsumerLeases")).as(name).isSameAs(applicationIndicator);
            });
        }
    }

    @Test
    void the_competingConsumers_contributor_lists_nothing_when_there_is_no_competing_consumer_model() {
        contextRunner.run(context -> assertThat(competingConsumers(context).stream()).isEmpty());
    }

    @Test
    void a_failing_subscription_is_DOWN_with_its_state_subscriber_failure_and_since_and_one_that_stands_by_is_UP() {
        CompetingConsumerSubscriptionModel model = mock(CompetingConsumerSubscriptionModel.class);
        CompetingConsumerStatus givenUp = new CompetingConsumerStatus("refused", "node", State.GIVEN_UP,
                new CompetingConsumerStatus.Failure(new IllegalStateException("start refused"), SINCE));
        CompetingConsumerStatus standby = new CompetingConsumerStatus("waiting", "node", State.STANDBY, null);
        when(model.competingConsumerStatuses()).thenReturn(List.of(givenUp, standby));
        when(model.competingConsumerStatus("refused")).thenReturn(Optional.of(givenUp));
        when(model.competingConsumerStatus("waiting")).thenReturn(Optional.of(standby));

        contextRunner.withBean("subscriptionModel", SubscriptionModel.class, () -> model).run(context -> {
            assertThat(competingConsumers(context).stream()).extracting(CompositeHealthContributor.Entry::name).containsExactly("refused", "waiting");
            Health down = indicator(competingConsumers(context), "refused").health();
            assertThat(down.getStatus()).isEqualTo(Status.DOWN);
            assertThat(down.getDetails()).containsEntry("state", "GIVEN_UP").containsEntry("subscriberId", "node")
                    .containsEntry("failure", "java.lang.IllegalStateException: start refused").containsEntry("since", SINCE.toString());
            Health up = indicator(competingConsumers(context), "waiting").health();
            assertThat(up.getStatus()).isEqualTo(Status.UP);
            assertThat(up.getDetails()).containsEntry("state", "STANDBY").doesNotContainKeys("failure", "since");
        });
    }

    @Test
    void a_subscription_is_DOWN_while_its_status_is_failing_and_UP_once_it_is_not_which_the_indicator_asks_for_on_every_call() {
        CompetingConsumerSubscriptionModel model = mock(CompetingConsumerSubscriptionModel.class);
        CompetingConsumerStatus failing = new CompetingConsumerStatus("s", "node", State.STANDBY,
                new CompetingConsumerStatus.Failure(new IllegalStateException("pause fails"), SINCE));
        CompetingConsumerStatus recovered = new CompetingConsumerStatus("s", "node", State.STANDBY, null);
        when(model.competingConsumerStatuses()).thenReturn(List.of(failing));
        when(model.competingConsumerStatus("s")).thenReturn(Optional.of(failing), Optional.of(recovered));

        contextRunner.withBean("subscriptionModel", SubscriptionModel.class, () -> model).run(context -> {
            HealthIndicator indicator = indicator(competingConsumers(context), "s");
            assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
            assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
        });
    }

    @Test
    void a_user_pause_and_a_stopped_model_are_UP() {
        CompetingConsumerSubscriptionModel model = mock(CompetingConsumerSubscriptionModel.class);
        CompetingConsumerStatus paused = new CompetingConsumerStatus("paused", "node", State.PAUSED, null);
        CompetingConsumerStatus stopped = new CompetingConsumerStatus("stopped", "node", State.STOPPED, null);
        when(model.competingConsumerStatuses()).thenReturn(List.of(paused, stopped));
        when(model.competingConsumerStatus("paused")).thenReturn(Optional.of(paused));
        when(model.competingConsumerStatus("stopped")).thenReturn(Optional.of(stopped));

        contextRunner.withBean("subscriptionModel", SubscriptionModel.class, () -> model).run(context -> {
            assertThat(indicator(competingConsumers(context), "paused").health().getStatus()).isEqualTo(Status.UP);
            assertThat(indicator(competingConsumers(context), "stopped").health().getStatus()).isEqualTo(Status.UP);
        });
    }

    @Test
    void a_competing_consumer_model_that_only_a_wrapper_bean_exposes_is_found() {
        CompetingConsumerSubscriptionModel model = mock(CompetingConsumerSubscriptionModel.class);
        CompetingConsumerStatus status = new CompetingConsumerStatus("s", "node", State.LEASE_HELD, null);
        when(model.competingConsumerStatuses()).thenReturn(List.of(status));
        when(model.competingConsumerStatus("s")).thenReturn(Optional.of(status));
        SubscriptionModel wrapper = mock(SubscriptionModel.class, withSettings().extraInterfaces(SubscriptionModelWrapper.class));
        when(((SubscriptionModelWrapper) wrapper).getWrappedSubscriptionModel()).thenReturn(model);

        contextRunner.withBean("wrapping", SubscriptionModel.class, () -> wrapper)
                .run(context -> assertThat(competingConsumers(context).stream()).extracting(CompositeHealthContributor.Entry::name).containsExactly("s"));
    }

    @Test
    void a_competing_consumer_model_under_two_beans_is_listed_once() {
        CompetingConsumerSubscriptionModel model = mock(CompetingConsumerSubscriptionModel.class);
        CompetingConsumerStatus status = new CompetingConsumerStatus("s", "node", State.LEASE_HELD, null);
        when(model.competingConsumerStatuses()).thenReturn(List.of(status));
        when(model.competingConsumerStatus("s")).thenReturn(Optional.of(status));
        SubscriptionModel wrapper = mock(SubscriptionModel.class, withSettings().extraInterfaces(SubscriptionModelWrapper.class));
        when(((SubscriptionModelWrapper) wrapper).getWrappedSubscriptionModel()).thenReturn(model);

        contextRunner.withBean("wrapping", SubscriptionModel.class, () -> wrapper)
                .withBean("wrapped", SubscriptionModel.class, () -> model)
                .run(context -> assertThat(competingConsumers(context).stream()).extracting(CompositeHealthContributor.Entry::name).containsExactly("s"));
    }

    @Test
    void a_subscription_id_two_models_share_is_DOWN_with_the_details_of_the_model_that_fails_whichever_model_comes_first() {
        CompetingConsumerSubscriptionModel failingModel = modelReporting(new CompetingConsumerStatus("shared", "failing-node", State.GIVEN_UP,
                new CompetingConsumerStatus.Failure(new IllegalStateException("start refused"), SINCE)));
        CompetingConsumerSubscriptionModel workingModel = modelReporting(new CompetingConsumerStatus("shared", "working-node", State.LEASE_HELD, null));

        for (boolean failingFirst : new boolean[]{true, false}) {
            ApplicationContextRunner runner = failingFirst
                    ? contextRunner.withBean("first", SubscriptionModel.class, () -> failingModel).withBean("second", SubscriptionModel.class, () -> workingModel)
                    : contextRunner.withBean("first", SubscriptionModel.class, () -> workingModel).withBean("second", SubscriptionModel.class, () -> failingModel);

            runner.run(context -> {
                assertThat(competingConsumers(context).stream()).as("failing model first: " + failingFirst).extracting(CompositeHealthContributor.Entry::name).containsExactly("shared");
                Health health = indicator(competingConsumers(context), "shared").health();
                assertThat(health.getStatus()).as("status, failing model first: " + failingFirst).isEqualTo(Status.DOWN);
                assertThat(health.getDetails()).as("details, failing model first: " + failingFirst).containsEntry("state", "GIVEN_UP").containsEntry("subscriberId", "failing-node")
                        .containsEntry("failure", "java.lang.IllegalStateException: start refused").containsEntry("since", SINCE.toString());
            });
        }
    }

    @Test
    void a_subscription_id_two_models_share_is_UP_when_neither_model_fails() {
        CompetingConsumerSubscriptionModel one = modelReporting(new CompetingConsumerStatus("shared", "node-1", State.LEASE_HELD, null));
        CompetingConsumerSubscriptionModel other = modelReporting(new CompetingConsumerStatus("shared", "node-2", State.STANDBY, null));

        contextRunner.withBean("first", SubscriptionModel.class, () -> one).withBean("second", SubscriptionModel.class, () -> other).run(context -> {
            assertThat(competingConsumers(context).stream()).extracting(CompositeHealthContributor.Entry::name).containsExactly("shared");
            assertThat(indicator(competingConsumers(context), "shared").health().getStatus()).isEqualTo(Status.UP);
        });
    }

    @Test
    void a_subscription_id_only_the_second_model_knows_is_listed_and_a_model_that_forgot_the_id_does_not_hide_the_one_that_fails() {
        CompetingConsumerStatus failing = new CompetingConsumerStatus("shared", "node-2", State.PAUSED,
                new CompetingConsumerStatus.Failure(new IllegalStateException("refused from its position"), SINCE));
        CompetingConsumerSubscriptionModel forgetting = mock(CompetingConsumerSubscriptionModel.class);
        when(forgetting.competingConsumerStatuses()).thenReturn(List.of(new CompetingConsumerStatus("shared", "node-1", State.LEASE_HELD, null), new CompetingConsumerStatus("only-first", "node-1", State.LEASE_HELD, null)));
        when(forgetting.competingConsumerStatus("shared")).thenReturn(Optional.empty());
        when(forgetting.competingConsumerStatus("only-first")).thenReturn(Optional.of(new CompetingConsumerStatus("only-first", "node-1", State.LEASE_HELD, null)));
        CompetingConsumerSubscriptionModel failingModel = modelReporting(failing);

        contextRunner.withBean("first", SubscriptionModel.class, () -> forgetting).withBean("second", SubscriptionModel.class, () -> failingModel).run(context -> {
            assertThat(competingConsumers(context).stream()).extracting(CompositeHealthContributor.Entry::name).containsExactly("shared", "only-first");
            assertThat(indicator(competingConsumers(context), "shared").health().getStatus()).isEqualTo(Status.DOWN);
            assertThat(indicator(competingConsumers(context), "only-first").health().getStatus()).isEqualTo(Status.UP);
        });
    }

    private static CompetingConsumerSubscriptionModel modelReporting(CompetingConsumerStatus status) {
        CompetingConsumerSubscriptionModel model = mock(CompetingConsumerSubscriptionModel.class);
        when(model.competingConsumerStatuses()).thenReturn(List.of(status));
        when(model.competingConsumerStatus(status.subscriptionId())).thenReturn(Optional.of(status));
        return model;
    }

    @Test
    void a_lease_strategy_whose_refresh_fails_is_DOWN_with_the_failure_and_since_and_one_that_works_is_UP() {
        SpringMongoLeaseCompetingConsumerStrategy failing = mock(SpringMongoLeaseCompetingConsumerStrategy.class);
        when(failing.leaseRefreshFailure()).thenReturn(Optional.of(new LeaseRefreshFailure(new IllegalStateException("MongoDB refuses writes"), SINCE)));
        SpringMongoLeaseCompetingConsumerStrategy working = mock(SpringMongoLeaseCompetingConsumerStrategy.class);
        when(working.leaseRefreshFailure()).thenReturn(Optional.empty());

        contextRunner.withBean("failingStrategy", SpringMongoLeaseCompetingConsumerStrategy.class, () -> failing)
                .withBean("workingStrategy", SpringMongoLeaseCompetingConsumerStrategy.class, () -> working)
                .run(context -> {
                    CompositeHealthContributor leases = competingConsumerLeases(context);
                    assertThat(leases.stream()).extracting(CompositeHealthContributor.Entry::name).containsExactlyInAnyOrder("failingStrategy", "workingStrategy");
                    Health down = indicator(leases, "failingStrategy").health();
                    assertThat(down.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(down.getDetails()).containsEntry("failure", "java.lang.IllegalStateException: MongoDB refuses writes").containsEntry("since", SINCE.toString());
                    assertThat(indicator(leases, "workingStrategy").health().getStatus()).isEqualTo(Status.UP);
                });
    }

    @Test
    void a_lease_strategy_is_UP_again_once_its_refresh_works() {
        SpringMongoLeaseCompetingConsumerStrategy strategy = mock(SpringMongoLeaseCompetingConsumerStrategy.class);
        when(strategy.leaseRefreshFailure()).thenReturn(Optional.of(new LeaseRefreshFailure(new IllegalStateException("MongoDB refuses writes"), SINCE)), Optional.empty());

        contextRunner.withBean("strategy", SpringMongoLeaseCompetingConsumerStrategy.class, () -> strategy).run(context -> {
            HealthIndicator indicator = indicator(competingConsumerLeases(context), "strategy");
            assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
            assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
        });
    }

    private static CompositeHealthContributor competingConsumers(AssertableApplicationContext context) {
        return context.getBean("competingConsumersHealthContributor", CompositeHealthContributor.class);
    }

    private static CompositeHealthContributor competingConsumerLeases(AssertableApplicationContext context) {
        return context.getBean("competingConsumerLeasesHealthContributor", CompositeHealthContributor.class);
    }

    private static HealthIndicator indicator(CompositeHealthContributor contributor, String name) {
        HealthContributor found = contributor.getContributor(name);
        assertThat(found).as("a health indicator named " + name).isInstanceOf(HealthIndicator.class);
        return (HealthIndicator) found;
    }
}
