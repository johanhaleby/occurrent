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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.SagaInstancesRegistry;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.StartupWorkaround;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Characterizes {@link SagaAnnotationRegistrar} against a plain {@link ApplicationContext} that is not a
 * {@link ConfigurableApplicationContext} -- the one branch neither {@code SagaAnnotationMongoTest} nor
 * {@code SagaInstancesRegistryMongoTest} can reach, because every Spring Boot context (including
 * {@code ApplicationContextRunner}'s) is configurable. Only a hand-built mock context exercises it.
 * <p>
 * The saga must still register and run: {@link SagaInstancesRegistry} still gets populated, only the named
 * {@code sagaInstances-<id>} singleton publication is skipped, with a warning logged instead of a startup failure. This
 * mirrors {@link SagaAnnotationValidationTest}'s style of driving {@link SagaAnnotationRegistrar} directly with mocked
 * collaborators rather than booting a context, since a real Spring container cannot produce a non-configurable one.
 */
@DisplayName("SagaAnnotationRegistrar against a non-configurable ApplicationContext")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationRegistrarTest {

    @Test
    void registers_the_saga_and_populates_the_registry_but_skips_the_named_singleton() throws Exception {
        SubscriptionHandle subscription = mock(SubscriptionHandle.class);
        Subscribable subscribable = mock(Subscribable.class);
        when(subscribable.subscribe(any(), any(), any(), any())).thenReturn(subscription);

        SagaStateStore<TestState> stateStore = SagaStateStore.inMemory();
        CommandDispatcher<TestCommand> dispatcher = command -> {
        };

        OccurrentProperties properties = new OccurrentProperties();
        // Sidesteps needing a CompetingConsumerStrategy bean, which is irrelevant to what this test characterizes.
        properties.getSaga().getCompetingConsumer().setEnabled(false);

        SagaInstancesRegistryImpl registry = new SagaInstancesRegistryImpl();
        @SuppressWarnings("unchecked")
        ObjectProvider<SagaInstancesRegistryImpl> registryProvider = mock(ObjectProvider.class);
        when(registryProvider.getIfAvailable()).thenReturn(registry);

        // A mock of the plain ApplicationContext interface, not ConfigurableApplicationContext: this is what makes
        // registerSagaInstancesSingleton take its "cannot publish" branch instead of registering the bean.
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBean(CloudEventConverter.class)).thenReturn(converter());
        // The non-push branch resolves through AsynchronousSubscribables rather than a bare getBean(Subscribable.class)
        // (see #563), so the mock has to answer the same by-name lookup that resolution performs.
        when(applicationContext.getBeanNamesForType(Subscribable.class)).thenReturn(new String[]{"subscribable"});
        when(applicationContext.isTypeMatch("subscribable", org.occurrent.subscription.api.blocking.RegisteringSubscribable.class)).thenReturn(false);
        when(applicationContext.getBean("subscribable", Subscribable.class)).thenReturn(subscribable);
        when(applicationContext.getBeanNamesForType(SagaStateStore.class)).thenReturn(new String[]{"sagaStateStore"});
        when(applicationContext.getBean("sagaStateStore")).thenReturn(stateStore);
        when(applicationContext.getBeanNamesForType(CommandDispatcher.class)).thenReturn(new String[]{"dispatcher"});
        when(applicationContext.getBean("dispatcher")).thenReturn(dispatcher);
        // No store starter contributes a startup workaround here, so the mock hands back an empty provider.
        @SuppressWarnings("unchecked")
        ObjectProvider<StartupWorkaround> startupWorkaroundProvider = mock(ObjectProvider.class);
        when(applicationContext.getBeanProvider(StartupWorkaround.class)).thenReturn(startupWorkaroundProvider);
        when(applicationContext.getBean("springApplicationAdminRegistrar")).thenThrow(new NoSuchBeanDefinitionException("springApplicationAdminRegistrar"));
        when(applicationContext.getBean(OccurrentProperties.class)).thenReturn(properties);
        when(applicationContext.getBeanProvider(SagaInstancesRegistryImpl.class)).thenReturn(registryProvider);
        // The registrar builds its checkpoint write version source over this provider, which a real context never
        // answers null for. Empty, since nothing here fences a checkpoint write.
        @SuppressWarnings("unchecked")
        ObjectProvider<CompetingConsumerStrategy> strategyProvider = mock(ObjectProvider.class);
        when(applicationContext.getBeanProvider(CompetingConsumerStrategy.class)).thenReturn(strategyProvider);

        StartPositionSupport startPositionSupport = new StartPositionSupport(applicationContext);
        SagaAnnotationRegistrar registrar = new SagaAnnotationRegistrar(applicationContext, startPositionSupport, new HashSet<>());

        Method factoryMethod = SagaHolder.class.getDeclaredMethod("saga");
        org.occurrent.annotation.Saga annotation = factoryMethod.getAnnotation(org.occurrent.annotation.Saga.class);

        try {
            registrar.processSagaAnnotation(new SagaHolder(), factoryMethod, annotation);

            assertThat(registry.sagaIds()).containsExactly("saga-on-plain-context");
            assertThat(registry.get("saga-on-plain-context")).isNotNull();
            assertThat(applicationContext).isNotInstanceOf(ConfigurableApplicationContext.class);
        } finally {
            registrar.close();
        }
    }

    private static CloudEventConverter<TestEvent> converter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(TestEvent domainEvent) {
                return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("TestEvent").build();
            }

            @Override
            public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                return new TestEvent();
            }

            @Override
            public String getCloudEventType(Class<? extends TestEvent> type) {
                return type.getSimpleName();
            }
        };
    }

    static class SagaHolder {
        @org.occurrent.annotation.Saga(id = "saga-on-plain-context")
        org.occurrent.dsl.saga.Saga<TestEvent, TestState, TestCommand> saga() {
            return org.occurrent.dsl.saga.Saga.<TestEvent, TestState, TestCommand>builder(new TestState())
                    .correlateAll(event -> "k")
                    .startsOn(TestEvent.class)
                    .build();
        }
    }

    record TestState() {
    }

    record TestEvent() {
    }

    record TestCommand() {
    }
}
