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

package org.occurrent.springboot.reactor;

import io.cloudevents.CloudEvent;
import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.springboot.blocking.OccurrentBlockingAnnotationConfiguration;
import org.occurrent.springboot.blocking.OccurrentBlockingBeanNames;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.internal.StartupGap;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A context with both the blocking and the reactor annotation post-processor has two {@link StartupGap}s. The blocking
 * post-processor registers first, so its scan ends first and opens its own gate, while the reactor gate is still closed
 * for as long as the reactor scan runs. A bean that scan is the first to build, and whose class declares a synchronous
 * handler for the blocking stack, must register that handler instead of being refused, since the reactor gate still
 * refuses every write. Once both gates admit writes, such a bean is refused.
 * <p>
 * The reactor scan builds the bean because the reactor stack's synchronous subscription DSL, which only it asks for,
 * is {@code @Lazy} and depends on it. Both the DSL and the bean are declared by an interface or a type that shows no
 * handler, so the blocking scan, which finishes first, never builds them.
 * <p>
 * The stand-in for the blocking application service asks every gap the way the stores' {@code StartupWriteGate} does,
 * and refuses the write with the same exception. Container-free, since both stacks' annotation configurations are
 * store-neutral.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DualStackLateSynchronousHandlerTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentBlockingAnnotationConfiguration.class, OccurrentReactiveAnnotationConfiguration.class))
            .withUserConfiguration(BothStacksConfiguration.class);

    @Test
    void a_bean_the_second_stacks_scan_is_first_to_build_registers_its_synchronous_handler_for_the_first_stack_instead_of_being_refused() {
        runner.run(context -> {
            assertThat(startupFailureOf(context)).as("[why the context failed to start]").isEqualTo("it started");
            assertThat(context.getBeanProvider(StartupGap.class).orderedStream().map(gap -> gap.getClass().getSimpleName()))
                    .as("[post-processors in the order their scans run]")
                    .containsExactly("OccurrentBlockingAnnotationBeanPostProcessor", "OccurrentReactiveAnnotationBeanPostProcessor");
            Trace trace = context.getBean(Trace.class);
            assertThat(trace.gatesOpenWhenBuilt)
                    .as("[blocking gate open, reactor gate open, when the second scan built the bean]")
                    .containsExactly(List.of(true, false));

            context.getBean(Writer.class).write(new TestEvent("written after startup"));

            assertThat(trace.reached)
                    .as("[events the first stack's synchronous handler received]")
                    .containsExactly(new TestEvent("written after startup"));
        });
    }

    @Test
    void a_bean_with_a_synchronous_handler_built_once_both_gates_admit_writes_is_refused() {
        runner.run(context -> {
            assertThat(startupFailureOf(context)).as("[why the context failed to start]").isEqualTo("it started");
            assertThat(context.getBeanProvider(StartupGap.class).orderedStream().map(StartupGap::admitsWrite))
                    .as("[gates admitting writes]")
                    .containsExactly(true, true);

            assertThatThrownBy(() -> context.getBean("afterStartup"))
                    .rootCause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("is created after the startup scan of the Spring Boot starter registered the synchronous handlers");
        });
    }

    private static String startupFailureOf(AssertableApplicationContext context) {
        Throwable failure = context.getStartupFailure();
        return failure == null ? "it started" : NestedExceptionUtils.getMostSpecificCause(failure).toString();
    }

    record TestEvent(String name) {
    }

    static class Trace {
        final List<TestEvent> reached = new CopyOnWriteArrayList<>();
        final List<Function2<EventMetadata, TestEvent, ?>> blockingHandlers = new CopyOnWriteArrayList<>();
        final List<List<Boolean>> gatesOpenWhenBuilt = new CopyOnWriteArrayList<>();
    }

    // Asks every gap before it writes, as the application service does, then delivers to the blocking synchronous handlers
    static class Writer {
        private final Trace trace;
        private final ObjectProvider<StartupGap> startupGaps;

        Writer(Trace trace, ObjectProvider<StartupGap> startupGaps) {
            this.trace = trace;
            this.startupGaps = startupGaps;
        }

        void write(TestEvent event) {
            if (!startupGaps.orderedStream().allMatch(StartupGap::admitsWrite)) {
                throw WriteRefusedException.beforeStartupCompleted();
            }
            trace.blockingHandlers.forEach(handler -> handler.invoke(null, event));
        }
    }

    interface Contract {
    }

    // Declares a synchronous handler for the blocking stack. Only built by what depends on it, since its bean is declared as Contract.
    static class BlockingSynchronousHandler implements Contract {
        BlockingSynchronousHandler(Trace trace, ObjectProvider<StartupGap> startupGaps) {
            trace.gatesOpenWhenBuilt.add(startupGaps.orderedStream().map(StartupGap::admitsWrite).toList());
            this.trace = trace;
        }

        private final Trace trace;

        @SynchronousSubscription(id = "blocking-synchronous-handler")
        void on(TestEvent event) {
            trace.reached.add(event);
        }
    }

    static class AfterStartupHandler implements Contract {
        @SynchronousSubscription(id = "after-startup-handler")
        void on(TestEvent event) {
        }
    }

    static class ReactorSynchronousHandler {
        @SynchronousSubscription(id = "reactor-synchronous-handler")
        Mono<Void> on(TestEvent event) {
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class BothStacksConfiguration {

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return null;
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return null;
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }

        @Bean
        Trace trace() {
            return new Trace();
        }

        @Bean
        Writer writer(Trace trace, ObjectProvider<StartupGap> startupGaps) {
            return new Writer(trace, startupGaps);
        }

        @Bean
        @Lazy
        Contract blockingSynchronousHandler(Trace trace, ObjectProvider<StartupGap> startupGaps) {
            return new BlockingSynchronousHandler(trace, startupGaps);
        }

        @Bean
        @Lazy
        Contract afterStartup() {
            return new AfterStartupHandler();
        }

        @Bean
        ReactorSynchronousHandler reactorSynchronousHandler() {
            return new ReactorSynchronousHandler();
        }

        // Only the reactor scan asks for this one, since it registers the reactor stack's handler there. Taking the
        // lazy bean as a parameter is what makes that scan the first to build it.
        @Bean(OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @Lazy
        @SuppressWarnings("unchecked")
        org.occurrent.dsl.subscription.reactor.Subscriptions<TestEvent> synchronousSubscriptions(@org.springframework.beans.factory.annotation.Qualifier("blockingSynchronousHandler") Contract dependsOn) {
            org.occurrent.dsl.subscription.reactor.Subscriptions<TestEvent> subscriptions = mock(org.occurrent.dsl.subscription.reactor.Subscriptions.class);
            org.occurrent.subscription.api.reactor.Subscription started = mock(org.occurrent.subscription.api.reactor.Subscription.class);
            when(started.waitUntilStarted()).thenReturn(Mono.empty());
            when(subscriptions.subscribe(anyString(), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class))).thenReturn(started);
            return subscriptions;
        }

        @Bean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @SuppressWarnings("unchecked")
        org.occurrent.dsl.subscription.blocking.Subscriptions<TestEvent> blockingSynchronousSubscriptions(Trace trace) {
            org.occurrent.dsl.subscription.blocking.Subscriptions<TestEvent> subscriptions = mock(org.occurrent.dsl.subscription.blocking.Subscriptions.class);
            doAnswer(invocation -> {
                if (invocation.getArgument(0).equals("blocking-synchronous-handler")) {
                    trace.blockingHandlers.add(invocation.getArgument(4));
                }
                return null;
            }).when(subscriptions).subscribe(anyString(), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            return subscriptions;
        }

        @Bean
        @org.springframework.context.annotation.Primary
        @SuppressWarnings("unchecked")
        org.occurrent.dsl.subscription.blocking.Subscriptions<TestEvent> subscriptions() {
            return mock(org.occurrent.dsl.subscription.blocking.Subscriptions.class);
        }
    }
}
