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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerStatus;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerStatus.State;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.mongodb.blocking.ccs.LeaseRefreshFailure;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.springframework.boot.health.autoconfigure.registry.HealthContributorRegistryAutoConfiguration;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.health.registry.HealthContributorRegistry;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
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
            .withUserConfiguration(OccurrentMongoHealthConfiguration.class)
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
                .withUserConfiguration(OccurrentMongoHealthConfiguration.class)
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
    void an_application_bean_declared_next_to_EnableOccurrent_under_any_of_the_three_names_replaces_the_competingConsumers_contributor() {
        for (Class<?> application : List.of(ApplicationWithCompetingConsumers.class, ApplicationWithCompetingConsumersHealthIndicator.class, ApplicationWithCompetingConsumersHealthContributor.class)) {
            enabledOccurrentRunner(application).run(context -> {
                assertThat(context).as(application.getSimpleName()).hasNotFailed();
                assertThat(context.getBeansOfType(CompetingConsumersHealthContributor.class)).as(application.getSimpleName()).isEmpty();
                assertThat(context.getBean(HealthContributorRegistry.class).getContributor("competingConsumers")).as(application.getSimpleName()).isSameAs(context.getBean(ApplicationHealth.class).indicator);
            });
        }
    }

    @Test
    void an_application_bean_declared_next_to_EnableOccurrent_under_any_of_the_three_names_replaces_the_competingConsumerLeases_contributor() {
        for (Class<?> application : List.of(ApplicationWithCompetingConsumerLeases.class, ApplicationWithCompetingConsumerLeasesHealthIndicator.class, ApplicationWithCompetingConsumerLeasesHealthContributor.class)) {
            enabledOccurrentRunner(application).run(context -> {
                assertThat(context).as(application.getSimpleName()).hasNotFailed();
                assertThat(context.getBeansOfType(CompetingConsumerLeasesHealthContributor.class)).as(application.getSimpleName()).isEmpty();
                assertThat(context.getBean(HealthContributorRegistry.class).getContributor("competingConsumerLeases")).as(application.getSimpleName()).isSameAs(context.getBean(ApplicationHealth.class).indicator);
            });
        }
    }

    // The application class is annotated with @EnableOccurrent, so the starter is imported with a plain @Import, with
    // no application bean registered before it, which withBean would do
    private static ApplicationContextRunner enabledOccurrentRunner(Class<?> application) {
        return new ApplicationContextRunner()
                .withUserConfiguration(application)
                .withConfiguration(AutoConfigurations.of(HealthContributorRegistryAutoConfiguration.class))
                .withBean(MongoDatabaseFactory.class, () -> mock(MongoDatabaseFactory.class))
                .withBean(MongoTemplate.class, () -> mock(MongoTemplate.class))
                .withPropertyValues(
                        "occurrent.event-store.enabled=false",
                        "occurrent.subscription.enabled=false",
                        "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test",
                        "occurrent.application-service.enable-default-retry-strategy=false");
    }

    static final class ApplicationHealth {
        final HealthIndicator indicator = () -> Health.up().withDetail("from", "application").build();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class ApplicationWithCompetingConsumers {
        @Bean ApplicationHealth applicationHealth() { return new ApplicationHealth(); }
        @Bean HealthIndicator competingConsumers(ApplicationHealth health) { return health.indicator; }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class ApplicationWithCompetingConsumersHealthIndicator {
        @Bean ApplicationHealth applicationHealth() { return new ApplicationHealth(); }
        @Bean HealthIndicator competingConsumersHealthIndicator(ApplicationHealth health) { return health.indicator; }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class ApplicationWithCompetingConsumersHealthContributor {
        @Bean ApplicationHealth applicationHealth() { return new ApplicationHealth(); }
        @Bean HealthIndicator competingConsumersHealthContributor(ApplicationHealth health) { return health.indicator; }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class ApplicationWithCompetingConsumerLeases {
        @Bean ApplicationHealth applicationHealth() { return new ApplicationHealth(); }
        @Bean HealthIndicator competingConsumerLeases(ApplicationHealth health) { return health.indicator; }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class ApplicationWithCompetingConsumerLeasesHealthIndicator {
        @Bean ApplicationHealth applicationHealth() { return new ApplicationHealth(); }
        @Bean HealthIndicator competingConsumerLeasesHealthIndicator(ApplicationHealth health) { return health.indicator; }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class ApplicationWithCompetingConsumerLeasesHealthContributor {
        @Bean ApplicationHealth applicationHealth() { return new ApplicationHealth(); }
        @Bean HealthIndicator competingConsumerLeasesHealthContributor(ApplicationHealth health) { return health.indicator; }
    }

    @Test
    void a_health_call_creates_no_subscription_model_bean_and_lists_none_that_is_not_created() {
        AtomicBoolean created = new AtomicBoolean();

        contextRunner.withBean("lazyModel", SubscriptionModel.class, () -> {
            created.set(true);
            return mock(CompetingConsumerSubscriptionModel.class);
        }, definition -> definition.setLazyInit(true)).run(context -> {
            assertThat(competingConsumers(context).stream()).isEmpty();
            assertThat(competingConsumers(context).getContributor("anything")).isNull();
            assertThat(created).as("the lazy subscription model bean was created by a health call").isFalse();
        });
    }

    @Test
    void a_health_call_creates_no_lease_strategy_bean_and_lists_none_that_is_not_created() {
        AtomicBoolean created = new AtomicBoolean();

        contextRunner.withBean("lazyStrategy", SpringMongoLeaseCompetingConsumerStrategy.class, () -> {
            created.set(true);
            return mock(SpringMongoLeaseCompetingConsumerStrategy.class);
        }, definition -> definition.setLazyInit(true)).run(context -> {
            assertThat(competingConsumerLeases(context).stream()).isEmpty();
            assertThat(competingConsumerLeases(context).getContributor("lazyStrategy")).isNull();
            assertThat(created).as("the lazy lease strategy bean was created by a health call").isFalse();
        });
    }

    @Test
    void a_health_call_does_not_ask_a_FactoryBean_for_the_subscription_model_it_makes() {
        AtomicInteger getObjectCalls = new AtomicInteger();
        CountingFactoryBean<CompetingConsumerSubscriptionModel> factory = new CountingFactoryBean<>(CompetingConsumerSubscriptionModel.class, getObjectCalls, () -> mock(CompetingConsumerSubscriptionModel.class));

        contextRunner.withBean("modelFactory", CountingFactoryBean.class, () -> factory).run(context -> {
            ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
            assertThat(beanFactory.containsSingleton("modelFactory")).as("the factory exists, so the bean factory says it is a singleton").isTrue();
            assertThat(beanFactory.getBeanNamesForType(SubscriptionModel.class, false, false)).as("names of subscription models, the factory among them").contains("modelFactory");
            assertThat(getObjectCalls).as("calls to getObject before the health call").hasValue(0);

            List<CompositeHealthContributor.Entry> listed = competingConsumers(context).stream().toList();
            HealthContributor looked = competingConsumers(context).getContributor("modelFactory");

            assertThat(getObjectCalls).as("calls to getObject made by a health call").hasValue(0);
            assertThat(listed).as("listed").isEmpty();
            assertThat(looked).as("contributor for the factory").isNull();
        });
    }

    @Test
    void a_health_call_does_not_ask_a_FactoryBean_for_the_lease_strategy_it_makes() {
        AtomicInteger getObjectCalls = new AtomicInteger();
        CountingFactoryBean<SpringMongoLeaseCompetingConsumerStrategy> factory = new CountingFactoryBean<>(SpringMongoLeaseCompetingConsumerStrategy.class, getObjectCalls, () -> mock(SpringMongoLeaseCompetingConsumerStrategy.class));

        contextRunner.withBean("strategyFactory", CountingFactoryBean.class, () -> factory).run(context -> {
            ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
            assertThat(beanFactory.containsSingleton("strategyFactory")).as("the factory exists, so the bean factory says it is a singleton").isTrue();
            assertThat(beanFactory.getBeanNamesForType(SpringMongoLeaseCompetingConsumerStrategy.class, false, false)).as("names of lease strategies, the factory among them").contains("strategyFactory");
            assertThat(getObjectCalls).as("calls to getObject before the health call").hasValue(0);

            List<CompositeHealthContributor.Entry> listed = competingConsumerLeases(context).stream().toList();
            HealthContributor looked = competingConsumerLeases(context).getContributor("strategyFactory");

            assertThat(getObjectCalls).as("calls to getObject made by a health call").hasValue(0);
            assertThat(listed).as("listed").isEmpty();
            assertThat(looked).as("contributor for the factory").isNull();
        });
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

    @Test
    void an_indicator_obtained_while_one_model_knew_the_id_is_DOWN_with_the_details_of_a_model_that_subscribed_the_id_and_failed_afterwards() {
        CompetingConsumerSubscriptionModel first = modelReporting(new CompetingConsumerStatus("shared", "node-1", State.LEASE_HELD, null));
        CompetingConsumerSubscriptionModel second = mock(CompetingConsumerSubscriptionModel.class);
        when(second.competingConsumerStatuses()).thenReturn(List.of());
        when(second.competingConsumerStatus("shared")).thenReturn(Optional.empty());

        contextRunner.withBean("first", SubscriptionModel.class, () -> first).withBean("second", SubscriptionModel.class, () -> second).run(context -> {
            HealthIndicator held = indicator(competingConsumers(context), "shared");
            assertThat(held.health().getStatus()).as("status while only the first model knows the id").isEqualTo(Status.UP);

            // When the second model subscribes the same id and its status fails
            CompetingConsumerStatus failing = new CompetingConsumerStatus("shared", "node-2", State.LEASE_HELD,
                    new CompetingConsumerStatus.Failure(new IllegalStateException("pause fails"), SINCE));
            when(second.competingConsumerStatuses()).thenReturn(List.of(failing));
            when(second.competingConsumerStatus("shared")).thenReturn(Optional.of(failing));

            // Then the indicator obtained earlier reports it
            Health health = held.health();
            assertThat(health.getStatus()).as("status of the indicator obtained earlier").isEqualTo(Status.DOWN);
            assertThat(health.getDetails()).containsEntry("state", "LEASE_HELD").containsEntry("subscriberId", "node-2")
                    .containsEntry("failure", "java.lang.IllegalStateException: pause fails").containsEntry("since", SINCE.toString());
        });
    }

    @Test
    void a_subscription_that_fails_while_a_subscribe_is_still_making_it_is_DOWN_before_the_subscribe_returns() throws Exception {
        // Given a real model whose subscribe is held while it registers the subscription
        RefusingModel wrapped = new RefusingModel();
        CountDownLatch registering = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        when(strategy.registerCompetingConsumer(anyString(), anyString())).thenAnswer(invocation -> {
            registering.countDown();
            release.await(30, SECONDS);
            return true;
        });
        when(strategy.hasLock(anyString(), anyString())).thenReturn(true);
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        Thread subscribing = new Thread(() -> model.subscribe("node", "making", null, StartAt.subscriptionModelDefault(), __ -> {
        }));
        subscribing.start();

        try {
            assertThat(registering.await(10, SECONDS)).as("the subscribe is registering the subscription").isTrue();

            // When the wrapped model reports a refusal of the start while the subscribe is making the subscription
            wrapped.reportRefusal("making");

            // Then the subscription is DOWN, before the subscribe has returned
            contextRunner.withBean("subscriptionModel", SubscriptionModel.class, () -> model).run(context -> {
                assertThat(competingConsumers(context).stream()).extracting(CompositeHealthContributor.Entry::name).containsExactly("making");
                Health health = indicator(competingConsumers(context), "making").health();
                assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                assertThat(health.getDetails()).containsEntry("subscriberId", "node").extractingByKey("failure").asString().contains(UnsupportedStartAtException.class.getName());
            });
        } finally {
            release.countDown();
            subscribing.join(30_000);
            model.shutdown();
        }
        assertThat(subscribing.isAlive()).as("the subscribe is alive").isFalse();
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

    private static final class RefusingModel extends InMemorySubscriptionModel implements StartRefusalReportingSubscriptions {
        private final List<StartRefusalListener> listeners = new CopyOnWriteArrayList<>();

        void reportRefusal(String subscriptionId) {
            listeners.forEach(listener -> listener.handleStartRefusal(subscriptionId, new UnsupportedStartAtException(StartAt.now(), "Refused the start of " + subscriptionId)));
        }

        @Override
        public void addStartRefusalListener(StartRefusalListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeStartRefusalListener(StartRefusalListener listener) {
            listeners.remove(listener);
        }
    }

    private static final class CountingFactoryBean<T> implements FactoryBean<T> {
        private final Class<T> type;
        private final AtomicInteger getObjectCalls;
        private final Supplier<T> product;

        private CountingFactoryBean(Class<T> type, AtomicInteger getObjectCalls, Supplier<T> product) {
            this.type = type;
            this.getObjectCalls = getObjectCalls;
            this.product = product;
        }

        @Override
        public T getObject() {
            getObjectCalls.incrementAndGet();
            return product.get();
        }

        @Override
        public Class<?> getObjectType() {
            return type;
        }
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
