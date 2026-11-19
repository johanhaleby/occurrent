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
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
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
import org.springframework.context.annotation.Primary;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A context with both the blocking and the reactor annotation post-processor has two {@link StartupGap}s, and a write
 * through either application service is refused while one of them is closed. The blocking post-processor registers
 * first in this context, so its scan is the first to finish. Its {@code startupMode = WAIT_UNTIL_STARTED} replay
 * writes at startup, to a synchronous handler only the reactor post-processor's scan registers, which has not run yet
 * when the blocking scan ends. The replay may therefore start only once the reactor scan has opened its gate too.
 * <p>
 * The stand-in for the application service asks every gap the way the stores' {@code StartupWriteGate} does, and
 * refuses the write with the same exception. Container-free, since both stacks' annotation configurations are
 * store-neutral.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DualStackStartupReplayTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentBlockingAnnotationConfiguration.class, OccurrentReactiveAnnotationConfiguration.class))
            .withUserConfiguration(BothStacksConfiguration.class);

    @Test
    void a_replay_of_the_stack_that_scans_first_writes_to_a_handler_the_other_stacks_scan_registers() {
        runner.run(context -> {
            assertThat(startupFailureOf(context)).as("[why the context failed to start]").isEqualTo("it started");
            // The replay sits on the stack whose scan finishes first, otherwise the ordering is not what is tested
            assertThat(context.getBeanProvider(StartupGap.class).orderedStream().map(gap -> gap.getClass().getSimpleName()))
                    .as("[post-processors in the order their scans run]")
                    .startsWith("OccurrentBlockingAnnotationBeanPostProcessor");
            assertThat(context.getBean(Writes.class).reached)
                    .as("[synchronous handlers the replay's write reached]")
                    .containsExactly("reactor-synchronous-handler");
        });
    }

    private static String startupFailureOf(AssertableApplicationContext context) {
        Throwable failure = context.getStartupFailure();
        return failure == null ? "it started" : NestedExceptionUtils.getMostSpecificCause(failure).toString();
    }

    record TestEvent() {
    }

    static class Writes {
        final List<String> reached = new CopyOnWriteArrayList<>();
        final List<Function2<EventMetadata, TestEvent, Mono<Void>>> synchronousHandlers = new CopyOnWriteArrayList<>();
    }

    // Asks every gap before it writes, as the application service does, then delivers to the synchronous handlers
    static class Writer {
        private final Writes writes;
        private final ObjectProvider<StartupGap> startupGaps;

        Writer(Writes writes, ObjectProvider<StartupGap> startupGaps) {
            this.writes = writes;
            this.startupGaps = startupGaps;
        }

        void write() {
            if (!startupGaps.orderedStream().allMatch(StartupGap::admitsWrite)) {
                throw WriteRefusedException.beforeStartupCompleted();
            }
            writes.synchronousHandlers.forEach(handler -> handler.invoke(null, new TestEvent()).block());
        }
    }

    static class BlockingReplay {
        private final Writer writer;

        BlockingReplay(Writer writer) {
            this.writer = writer;
        }

        @Subscription(id = "blocking-replay", startupMode = StartupMode.WAIT_UNTIL_STARTED)
        void on(TestEvent event) {
            writer.write();
        }
    }

    static class ReactorSynchronousHandler {
        private final Writes writes;

        ReactorSynchronousHandler(Writes writes) {
            this.writes = writes;
        }

        @SynchronousSubscription(id = "reactor-synchronous-handler")
        Mono<Void> on(TestEvent event) {
            writes.reached.add("reactor-synchronous-handler");
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
        Writes writes() {
            return new Writes();
        }

        @Bean
        Writer writer(Writes writes, ObjectProvider<StartupGap> startupGaps) {
            return new Writer(writes, startupGaps);
        }

        // Replays its history inside subscribe, the way a WAIT_UNTIL_STARTED subscription does
        @Bean
        @Primary
        @SuppressWarnings("unchecked")
        org.occurrent.dsl.subscription.blocking.Subscriptions<TestEvent> subscriptions() {
            org.occurrent.dsl.subscription.blocking.Subscriptions<TestEvent> subscriptions = mock(org.occurrent.dsl.subscription.blocking.Subscriptions.class);
            doAnswer(invocation -> {
                Function2<EventMetadata, TestEvent, ?> handler = invocation.getArgument(4);
                handler.invoke(null, new TestEvent());
                return null;
            }).when(subscriptions).subscribe(eq("blocking-replay"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            return subscriptions;
        }

        @Bean(OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @SuppressWarnings("unchecked")
        org.occurrent.dsl.subscription.reactor.Subscriptions<TestEvent> synchronousSubscriptions(Writes writes) {
            org.occurrent.dsl.subscription.reactor.Subscriptions<TestEvent> subscriptions = mock(org.occurrent.dsl.subscription.reactor.Subscriptions.class);
            org.occurrent.subscription.api.reactor.Subscription started = mock(org.occurrent.subscription.api.reactor.Subscription.class);
            when(started.waitUntilStarted()).thenReturn(Mono.empty());
            // This stack also registers the blocking stack's replay, which nothing is delivered to
            doAnswer(invocation -> {
                if (invocation.getArgument(0).equals("reactor-synchronous-handler")) {
                    writes.synchronousHandlers.add(invocation.getArgument(3));
                }
                return started;
            }).when(subscriptions).subscribe(anyString(), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
            return subscriptions;
        }

        // Each post-processor scans every bean, so the blocking one registers the reactor stack's handler here, where
        // nothing is delivered from
        @Bean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @SuppressWarnings("unchecked")
        org.occurrent.dsl.subscription.blocking.Subscriptions<TestEvent> blockingSynchronousSubscriptions() {
            return mock(org.occurrent.dsl.subscription.blocking.Subscriptions.class);
        }

        @Bean
        BlockingReplay blockingReplay(Writer writer) {
            return new BlockingReplay(writer);
        }

        @Bean
        ReactorSynchronousHandler reactorSynchronousHandler(Writes writes) {
            return new ReactorSynchronousHandler(writes);
        }
    }
}
