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
import kotlin.jvm.functions.Function2;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Scope;
import org.springframework.core.NestedExceptionUtils;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * A bean the container has not built when the startup scan runs is read through a prediction rather than through its
 * class, because building it there would defeat {@code @Lazy} and {@code spring.main.lazy-initialization} alike, and
 * that prediction is the factory method's declared return type. An annotation only the concrete class declares is
 * invisible to it. These tests cover what happens instead. The container builds the bean later, building it is what
 * makes its class knowable, and the annotation registers then.
 * <p>
 * Container-free, like {@link SubscriptionAnnotationGuardTest} next to it. A mocked {@link Subscriptions} is all a
 * {@code @Subscription} needs to register, so nothing here needs Docker.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class LateBeanAnnotationRegistrationTest {

    private static final AtomicInteger INSTANTIATIONS = new AtomicInteger();
    private static final AtomicInteger PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger INTERFACE_PROJECTION_FACTORY_INVOCATIONS = new AtomicInteger();
    private static final AtomicInteger FAILING_PROJECTION_FACTORY_CALLS = new AtomicInteger();
    private static final java.util.concurrent.atomic.AtomicBoolean DRAIN_TRIGGERED = new java.util.concurrent.atomic.AtomicBoolean();
    private static final AtomicInteger LATE_FEED_READS = new AtomicInteger();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new);

    @BeforeEach
    void resetInstantiations() {
        INSTANTIATIONS.set(0);
    }

    // The case #981 describes, @Bean declared to return an interface, @Lazy so the bean does not exist when the
    // scan runs, and the handler on the concrete class the interface does not declare. The scan sees Marker and
    // nothing else, so nothing registers at startup, and the bean is left uncreated, which is what @Lazy asked for.
    // Asking for the bean is what builds it, and that is when the subscription registers.
    @Test
    void a_subscription_only_the_concrete_class_declares_registers_when_the_lazy_bean_is_created() {
        runner.withUserConfiguration(LazyInterfaceReturningConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(INSTANTIATIONS).describedAs("@Lazy is still honored by the scan").hasValue(0);
            verify(subscriptions, never()).subscribe(eq("lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));

            context.getBean("hiddenSubscriber");

            verify(subscriptions).subscribe(eq("lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // Building the bean a second time cannot register the subscription a second time. The id is the durable
    // checkpoint key, so a duplicate registration is never a harmless repeat.
    @Test
    void a_lazy_beans_subscription_registers_once_however_often_the_bean_is_asked_for() {
        runner.withUserConfiguration(LazyInterfaceReturningConfiguration.class).run(context -> {
            context.getBean("hiddenSubscriber");
            context.getBean("hiddenSubscriber");

            verify(context.getBean(Subscriptions.class), times(1))
                    .subscribe(eq("lazy-hidden-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // A SmartFactoryBean whose product is not eager exists at scan time while its product does not, and
    // getObjectType() answers with the interface for the same reason a factory method's return type does. The
    // product's own creation is what reveals the handler.
    @Test
    void a_subscription_only_a_factory_bean_product_declares_registers_when_the_product_is_created() {
        runner.withUserConfiguration(LazyFactoryBeanConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            assertThat(INSTANTIATIONS).describedAs("the scan does not force the product") .hasValue(0);
            verify(subscriptions, never()).subscribe(eq("factory-product-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));

            context.getBean("hiddenProductFactory");

            verify(subscriptions).subscribe(eq("factory-product-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // Registering a handler the prediction did see builds the bean, and building it records its class, so the scan
    // runs a second time over what it can now read. Without that second pass the handler the interface declares
    // would register and the one only the class declares would not, which is the worse half of the same defect. An
    // application would see one of its two subscriptions running and have no reason to suspect the other.
    @Test
    void a_handler_the_predicted_interface_hides_registers_alongside_one_it_declares() {
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, PartiallyVisibleConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            verify(subscriptions).subscribe(eq("declared-on-the-interface"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            verify(subscriptions).subscribe(eq("declared-on-the-class"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // Reserving a handler is one atomic add rather than a check followed by an add. A second instance that finds
    // the handler free would go on to claim the id, which the first instance already holds, and fail its own
    // bean's creation as a duplicate of itself. Sequential requests hand back both instances and register once, so
    // asking again must not turn into a failure.
    @Test
    void asking_for_a_second_prototype_instance_does_not_fail_as_a_duplicate() {
        runner.withUserConfiguration(PrototypeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            context.getBean("hiddenPrototypeSubscriber");

            assertThatCode(() -> context.getBean("hiddenPrototypeSubscriber")).doesNotThrowAnyException();
        });
    }

    // A prototype passes through the same creation callback once per instance, and a subscription id is the durable
    // checkpoint key, so the second instance must not register it again. The one instance that did register stays
    // the handler's target too, because resolving a prototype by name per delivery would build a fresh bean for
    // every event, which is neither what the startup scan does for a prototype it can see nor anything a handler
    // could rely on.
    @Test
    void a_prototypes_hidden_subscription_registers_once_and_keeps_the_instance_that_registered_it() {
        runner.withUserConfiguration(PrototypeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, ?>> handler = ArgumentCaptor.forClass(Function2.class);
            context.getBean("hiddenPrototypeSubscriber");
            context.getBean("hiddenPrototypeSubscriber");

            verify(context.getBean(Subscriptions.class), times(1))
                    .subscribe(eq("prototype-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), handler.capture());
            assertThat(INSTANTIATIONS).hasValue(2);

            handler.getValue().invoke(null, new TestEvent());

            assertThat(INSTANTIATIONS).describedAs("a delivery builds no further instance").hasValue(2);
        });
    }

    // The bean a late handler is invoked on is resolved by name, per delivery, rather than held on to while the bean
    // is still being created. A BeanPostProcessor registered after the Occurrent one wraps the instance the creation
    // callback hands over, so holding on to that instance would invoke the handler on an inner layer and lose the
    // outer layer's advice, which is exactly the loss that moved registration to afterSingletonsInstantiated.
    @Test
    void a_late_handler_is_invoked_through_the_bean_the_context_publishes() {
        runner.withUserConfiguration(LateWrappingProxyConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, ?>> handler = ArgumentCaptor.forClass(Function2.class);
            context.getBean("wrappedSubscriber");
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("late-wrapped-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), handler.capture());

            LateWrappingProxyConfiguration.ADVICE_CALLS.clear();
            handler.getValue().invoke(null, new TestEvent());

            assertThat(LateWrappingProxyConfiguration.ADVICE_CALLS).containsExactly("on");
        });
    }

    // A handler registered from the creation callback can be delivered to before that callback returns, because
    // startupMode = WAIT_UNTIL_STARTED replays history inside subscribe. The singleton is not published yet at that
    // point, so a handler target that always asks the context by name would throw BeanCurrentlyInCreationException
    // and the bean could never finish being built.
    @Test
    void a_late_handler_replaying_history_inside_its_own_registration_is_delivered_to() {
        runner.withUserConfiguration(ReplayDuringRegistrationConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            context.getBean("replayingSubscriber");

            assertThat(ReplayingSubscriber.DELIVERED).containsExactly("replayed");
        });
    }

    // The startup ordering used to make this impossible, since every subscription id was collected before the first
    // projection checked one out. A subscription registering after startup arrives after all of them, so it has to
    // refuse an id one of them already holds rather than write to the same durable checkpoint key.
    @Test
    void a_late_subscription_reusing_a_projections_id_is_refused() {
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, LateIdClashConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThatThrownBy(() -> context.getBean("clashingSubscriber"))
                    .rootCause()
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("clashing-id");
        });
    }

    // The startup path binds a handler to the instance it resolved, so a prototype whose declared type already
    // exposes the annotation keeps the single instance it registered with. Asking the context by name per delivery
    // would build a fresh prototype for every event instead.
    @Test
    void a_prototype_registered_at_startup_keeps_the_instance_it_registered_with() {
        runner.withUserConfiguration(VisiblePrototypeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, ?>> handler = ArgumentCaptor.forClass(Function2.class);
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("visible-prototype-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), handler.capture());
            int afterStartup = INSTANTIATIONS.get();

            handler.getValue().invoke(null, new TestEvent());

            assertThat(INSTANTIATIONS).describedAs("a delivery builds no further instance").hasValue(afterStartup);
        });
    }

    // Registering a handler builds its bean, and building it, or invoking the descriptor factory it declares, can
    // build another bean whose class the scan has never read. Each pass therefore reveals the next, so the scan
    // has to repeat until a pass registers nothing rather than run a fixed number of times. This chain needs three
    // passes. The first registers chainHead, whose construction reveals chainSecond, and the second registers
    // chainSecond's projection, whose factory reveals chainThird.
    @Test
    void a_handler_revealed_by_a_bean_that_a_later_pass_built_registers_too() {
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, ChainedDiscoveryConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
            verify(subscriptions).subscribe(eq("chain-visible"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            verify(subscriptions).subscribe(eq("chain-third"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // A registration that throws fails the bean's creation, and Spring caches nothing for a creation that failed,
    // so the next request builds the bean again. The handler has to be registered on that second attempt, which it
    // is not if the first attempt recorded it as registered before doing the work.
    @Test
    void a_late_handler_whose_first_registration_threw_registers_on_the_retry() {
        runner.withUserConfiguration(FailingFirstRegistrationConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThatThrownBy(() -> context.getBean("retriedSubscriber")).isNotNull();

            context.getBean("retriedSubscriber");

            verify(context.getBean(Subscriptions.class), times(2))
                    .subscribe(eq("retried-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // The register step reads the bean's class again, and by then the bean exists, so that class can declare a
    // handler the collecting pass never saw. Registering such a handler straight away would take its id without
    // ever checking it, which is how a second method could quietly share a durable checkpoint key with the first.
    @Test
    void a_second_handler_on_the_concrete_class_reusing_the_interfaces_id_is_refused() {
        runner.withUserConfiguration(SameIdAcrossMethodsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("shared-across-methods");
        });
    }

    // The id that gets claimed is the one the registrar read from the method it registered, not one resolved
    // earlier against a predicted type. An overriding method may declare a different id than the method it
    // overrides, and claiming the wrong one lets something else take the real id.
    @Test
    void an_overriding_methods_own_id_is_the_one_that_gets_claimed() {
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, OverriddenIdConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("class-declared-id");
        });
    }

    // A descriptor registrar claims its id before the rest of its work, so a failure part way through would leave
    // the id held by a registration that never happened and the bean's next creation attempt would be refused as a
    // duplicate of itself.
    @Test
    void a_late_projection_whose_first_registration_threw_registers_on_the_retry() {
        FAILING_PROJECTION_FACTORY_CALLS.set(0);
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, FailingFirstProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThatThrownBy(() -> context.getBean("failingProjectionHolder")).isNotNull();

            context.getBean("failingProjectionHolder");

            assertThat(FAILING_PROJECTION_FACTORY_CALLS).hasValue(2);
        });
    }

    // Waiting for a replay inside the creation callback would deliver to the handler on an object the context has
    // not published yet. A late registration therefore never waits, whatever startupMode says, and the replay runs
    // once creation has finished instead.
    @Test
    void a_late_registration_does_not_wait_for_its_replay() {
        runner.withUserConfiguration(ReplayingStartupModeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            context.getBean("waitingSubscriber");

            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("late-waiting-handler"), any(AgnosticSubscriptionFilter.class), any(), eq(false), any(Function2.class));
        });
    }

    // A CGLIB proxy added by a later BeanPostProcessor cannot override a final handler, so selecting the
    // method succeeds while invoking it reaches the inherited method directly and every layer's advice is skipped.
    // Registration cannot see that proxy, because it does not exist yet, so the guard runs again on the object the
    // handler is actually invoked on.
    @Test
    void a_final_late_handler_wrapped_in_a_cglib_proxy_afterwards_is_refused_on_delivery() {
        runner.withUserConfiguration(LateFinalHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Function2<EventMetadata, TestEvent, ?>> handler = ArgumentCaptor.forClass(Function2.class);
            context.getBean("finalHandlerSubscriber");
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("late-final-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), handler.capture());

            assertThatThrownBy(() -> handler.getValue().invoke(null, new TestEvent()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining("is final");
        });
    }

    // A bean can declare several handlers, and the second one failing must not leave the first subscribed. The
    // bean's creation fails either way, so a handler registered before the failure would be delivering to an
    // instance nobody can reach, and the context stays up because a lazily built bean failing does not close it.
    @Test
    void a_bean_whose_second_handler_cannot_register_leaves_the_first_one_unsubscribed() {
        runner.withUserConfiguration(SecondHandlerFailsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThatThrownBy(() -> context.getBean("twoHandlerSubscriber")).isNotNull();

            verify(context.getBean(Subscriptions.class), never())
                    .subscribe(eq("first-of-two"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    // Claiming an id is one atomic add rather than a check under a lock, so two threads building two lazy beans
    // that declare the same id cannot both win. The lock this replaced was held across the registrar's own
    // collaborator lookups, which is what could deadlock against a thread building one of those collaborators.
    @Test
    @Timeout(60)
    void two_threads_claiming_the_same_id_at_once_leave_exactly_one_winner() throws Exception {
        runner.withUserConfiguration(SameIdOnTwoBeansConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            CyclicBarrier bothReady = new CyclicBarrier(2);
            ExecutorService threads = Executors.newFixedThreadPool(2);
            try {
                List<Future<Throwable>> attempts = new ArrayList<>();
                for (String beanName : List.of("firstClaimant", "secondClaimant")) {
                    attempts.add(threads.submit(() -> {
                        bothReady.await(30, TimeUnit.SECONDS);
                        try {
                            context.getBean(beanName);
                            return null;
                        } catch (Throwable e) {
                            return e;
                        }
                    }));
                }
                List<Throwable> outcomes = new ArrayList<>();
                for (Future<Throwable> attempt : attempts) {
                    outcomes.add(attempt.get(60, TimeUnit.SECONDS));
                }

                assertThat(outcomes).describedAs("exactly one claimant wins").filteredOn(java.util.Objects::isNull).hasSize(1);
                assertThat(outcomes).filteredOn(java.util.Objects::nonNull).allSatisfy(failure ->
                        assertThat(NestedExceptionUtils.getMostSpecificCause(failure)).isInstanceOf(DuplicateSubscriptionIdException.class));
                verify(context.getBean(Subscriptions.class), times(1))
                        .subscribe(eq("contended-id"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            } finally {
                threads.shutdownNow();
            }
        });
    }

    // A bean built while another late registration is in flight registers the same way one built on its own does.
    // Nothing is deferred to another thread and nothing waits on a lock, so the two are independent.
    @Test
    @Timeout(60)
    void two_late_beans_built_at_once_both_register() throws Exception {
        runner.withUserConfiguration(TwoLateBeansConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            CyclicBarrier bothReady = new CyclicBarrier(2);
            ExecutorService threads = Executors.newFixedThreadPool(2);
            try {
                List<Future<?>> builds = new ArrayList<>();
                for (String beanName : List.of("concurrentOne", "concurrentTwo")) {
                    builds.add(threads.submit(() -> {
                        bothReady.await(30, TimeUnit.SECONDS);
                        return context.getBean(beanName);
                    }));
                }
                for (Future<?> build : builds) {
                    build.get(60, TimeUnit.SECONDS);
                }

                Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);
                verify(subscriptions).subscribe(eq("concurrent-one"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
                verify(subscriptions).subscribe(eq("concurrent-two"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            } finally {
                threads.shutdownNow();
            }
        });
    }

    // The catch-up drain polls until the queue is empty rather than iterating it and clearing, so a feed added
    // while the drain is running is still caught up. Iterating and then clearing drops such an entry, and a
    // dropped entry there is a projection that never replays its history.
    @Test
    @Timeout(60)
    void a_feed_registered_while_the_catch_up_drain_runs_is_still_caught_up() {
        DRAIN_TRIGGERED.set(false);
        LATE_FEED_READS.set(0);
        runner.withUserConfiguration(DrainDuringCatchUpConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(DRAIN_TRIGGERED).describedAs("the first feed's catch-up built the lazy projection").isTrue();
            assertThat(LATE_FEED_READS).describedAs("the feed added during the drain was caught up too").hasValueGreaterThan(0);
        });
    }

    // A registration refused because its id belongs to something else must not hand that id away on its way out.
    // It reserved a handler and acquired no id, so releasing the id would free the owner's claim and let the next
    // bean register alongside it on the same durable checkpoint key.
    @Test
    void a_refused_late_subscription_does_not_release_the_id_it_was_refused() {
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, TwoClashingLateBeansConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThatThrownBy(() -> context.getBean("firstClashingSubscriber"))
                    .rootCause().isInstanceOf(DuplicateSubscriptionIdException.class);

            // The projection still owns the id, so the second bean is refused for the same reason rather than
            // finding it free.
            assertThatThrownBy(() -> context.getBean("secondClashingSubscriber"))
                    .rootCause().isInstanceOf(DuplicateSubscriptionIdException.class);
        });
    }

    // A refused late descriptor must not release the id either, and which claim is whose cannot be read off the
    // exception type. A descriptor registrar claims the id itself and then calls the subscription model, which
    // raises the same exception for a programmatic subscription already using that id, so the coordinator reads
    // who holds the id before the attempt runs rather than inferring it afterwards.
    @Test
    void a_refused_late_projection_does_not_release_the_id_it_was_refused() {
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, TwoClashingLateProjectionsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();

            assertThatThrownBy(() -> context.getBean("firstClashingProjection"))
                    .rootCause().isInstanceOf(DuplicateSubscriptionIdException.class);

            // The subscription still owns the id, so the second is refused for the same reason rather than
            // finding it free.
            assertThatThrownBy(() -> context.getBean("secondClashingProjection"))
                    .rootCause().isInstanceOf(DuplicateSubscriptionIdException.class);
        });
    }

    // Registration is keyed by the method, so a method declaring two descriptor annotations would have the second
    // one skipped in silence once the first marked the key. It was always a mistake, and it used to be caught by
    // the second registrar rejecting the return type, so it is refused here rather than dropped.
    @Test
    void a_method_with_two_descriptor_annotations_is_refused_rather_than_half_registered() {
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, MixedDescriptorConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("more than one of @Projection, @Snapshot and @Saga");
        });
    }

    interface Marker {
    }

    static class HiddenSubscriber implements Marker {
        HiddenSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "lazy-hidden-handler")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LazyInterfaceReturningConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        // Declared to return Marker, so applicationContext.getType predicts Marker however the body is written.
        @Bean
        @Lazy
        Marker hiddenSubscriber() {
            return new HiddenSubscriber();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LazyFactoryBeanConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        HiddenProductFactory hiddenProductFactory() {
            return new HiddenProductFactory();
        }
    }

    // isEagerInit() is false by SmartFactoryBean's default, so the container does not build the product during
    // startup and neither may the scan.
    static class HiddenProductFactory implements SmartFactoryBean<Marker> {
        @Override
        public Marker getObject() {
            return new FactoryProductSubscriber();
        }

        @Override
        public Class<?> getObjectType() {
            return Marker.class;
        }
    }

    static class FactoryProductSubscriber implements Marker {
        FactoryProductSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "factory-product-handler")
        void on(TestEvent event) {
        }
    }

    interface DeclaringMarker {
        @Subscription(id = "declared-on-the-interface")
        void onTheInterface(TestEvent event);
    }

    static class PartiallyVisibleSubscriber implements DeclaringMarker {
        @Override
        public void onTheInterface(TestEvent event) {
        }

        @Subscription(id = "declared-on-the-class")
        void onTheClass(TestEvent event) {
        }

        @Projection(id = "declared-on-the-class-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> hiddenProjection() {
            PROJECTION_FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    // The collaborators a source = PUSH projection needs, shared by the two configurations below. Each of them runs
    // in a context of its own, so each gets its own DomainEventFeed, which matters because a feed feeds exactly one
    // projection.
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ProjectionCollaboratorsConfiguration {
        // Returns a CloudEvent rather than null, unlike the stub the subscription-only fixtures use, because the
        // @Projection registration path converts events rather than only deriving a type filter from them.
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
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

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        // An empty domain-feed reader is all a source = PUSH projection needs to register without Docker.
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader emptyReader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return Stream.empty();
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(emptyReader, converter, event -> "k");
        }

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }

    }

    @Configuration(proxyBeanMethods = false)
    static class PartiallyVisibleConfiguration {
        @Bean
        @Lazy
        DeclaringMarker partiallyVisibleSubscriber() {
            return new PartiallyVisibleSubscriber();
        }
    }

    interface OverridingMarker {
        @Projection(id = "overridden-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> interfaceProjection();
    }

    static class OverridingSubscriber implements OverridingMarker {
        // Overrides a @Projection the interface declares, so the startup scan sees the interface's Method for it and
        // the rescan sees the overriding one. Registering it twice would refuse the id as a duplicate.
        @Override
        public org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> interfaceProjection() {
            INTERFACE_PROJECTION_FACTORY_INVOCATIONS.incrementAndGet();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OverriddenProjectionConfiguration {
        @Bean
        @Lazy
        OverridingMarker overridingSubscriber() {
            return new OverridingSubscriber();
        }
    }

    static class PrototypeSubscriber implements Marker {
        PrototypeSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "prototype-handler")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PrototypeConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        // Declared to return Marker for the same reason the @Lazy fixtures are, so the handler is one only the
        // concrete class declares and the startup scan cannot see it.
        @Bean
        @Scope("prototype")
        Marker hiddenPrototypeSubscriber() {
            return new PrototypeSubscriber();
        }
    }

    public static class WrappedSubscriber implements Marker {
        @Subscription(id = "late-wrapped-handler")
        public void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LateWrappingProxyConfiguration {
        static final List<String> ADVICE_CALLS = new ArrayList<>();

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        Marker wrappedSubscriber() {
            return new WrappedSubscriber();
        }

        // Declared in a user configuration, so it is registered after the post processor the runner contributes and
        // therefore wraps the bean after the post processor's own creation callback has already seen it.
        @Bean
        static BeanPostProcessor lateWrappingPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof WrappedSubscriber)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setProxyTargetClass(true);
                    proxyFactory.addAdvice((MethodInterceptor) invocation -> {
                        ADVICE_CALLS.add(invocation.getMethod().getName());
                        return invocation.proceed();
                    });
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    // The second scan pass, and the case only it reaches. A @Projection is collected as a (bean, method, annotation)
    // triple during the pass that reads the bean's type and registered afterwards, so unlike a subscription, whose
    // registration re-reads the type after the bean exists, it never revisits the type. Registering the handler the
    // interface does declare is what builds the bean, and only a second pass reads the class that build revealed.
    // Without it an application would see one of its two annotations working and have no reason to suspect the other.
    @Test
    void a_projection_only_the_concrete_class_declares_registers_alongside_a_subscription_the_interface_declares() {
        PROJECTION_FACTORY_INVOCATIONS.set(0);
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, PartiallyVisibleConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("declared-on-the-interface"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            assertThat(PROJECTION_FACTORY_INVOCATIONS).describedAs("the @Projection only the class declares").hasValue(1);
        });
    }

    // A @Projection the interface declares and the class overrides is a different Method on each pass, so a handler
    // key that included the declaring class would let the rescan register the same id a second time.
    @Test
    void a_projection_the_class_overrides_from_its_interface_registers_once() {
        INTERFACE_PROJECTION_FACTORY_INVOCATIONS.set(0);
        runner.withUserConfiguration(ProjectionCollaboratorsConfiguration.class, OverriddenProjectionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(INTERFACE_PROJECTION_FACTORY_INVOCATIONS).hasValue(1);
        });
    }

    public static class ReplayingSubscriber implements Marker {
        static final List<String> DELIVERED = new ArrayList<>();

        @Subscription(id = "replay-during-registration")
        public void on(TestEvent event) {
            DELIVERED.add("replayed");
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ReplayDuringRegistrationConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        // Delivers to the handler from inside subscribe, the way a WAIT_UNTIL_STARTED history replay does, so the
        // delivery arrives while the bean this handler belongs to is still being built.
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
            doAnswer(invocation -> {
                Function2<EventMetadata, TestEvent, ?> handler = invocation.getArgument(4);
                handler.invoke(null, new TestEvent());
                return null;
            }).when(subscriptions).subscribe(eq("replay-during-registration"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            return subscriptions;
        }

        @Bean
        @Lazy
        Marker replayingSubscriber() {
            ReplayingSubscriber.DELIVERED.clear();
            return new ReplayingSubscriber();
        }
    }

    static class ClashingSubscriber implements Marker {
        @Subscription(id = "clashing-id")
        void on(TestEvent event) {
        }
    }

    static class ClashingProjectionHolder {
        @Projection(id = "clashing-id", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LateIdClashConfiguration {
        @Bean
        ClashingProjectionHolder clashingProjectionHolder() {
            return new ClashingProjectionHolder();
        }

        @Bean
        @Lazy
        Marker clashingSubscriber() {
            return new ClashingSubscriber();
        }
    }

    // Declared as the concrete class, so the startup scan sees the handler and registers it there rather than from
    // the creation callback.
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class VisiblePrototypeConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Scope("prototype")
        VisiblePrototypeSubscriber visiblePrototypeSubscriber() {
            return new VisiblePrototypeSubscriber();
        }
    }

    static class VisiblePrototypeSubscriber {
        VisiblePrototypeSubscriber() {
            INSTANTIATIONS.incrementAndGet();
        }

        @Subscription(id = "visible-prototype-handler")
        void on(TestEvent event) {
        }
    }

    interface ChainMarker {
    }

    // Declared as the concrete class, so the first pass sees this handler. Building it builds the second bean,
    // whose class the first pass had no way to read.
    static class ChainHead {
        ChainHead(ObjectProvider<ChainMarker> second) {
            second.getObject();
        }

        @Subscription(id = "chain-visible")
        void on(TestEvent event) {
        }
    }

    // Found by the second pass, and its @Projection factory is what builds the third bean. Building it from the
    // factory rather than from this constructor is the point, because the third bean's class is recorded after the
    // second pass has already collected, so only a third pass can read it.
    static class ChainSecond implements ChainMarker {
        private final ObjectProvider<ChainTailMarker> third;

        ChainSecond(ObjectProvider<ChainTailMarker> third) {
            this.third = third;
        }

        @Projection(id = "chain-second-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            third.getObject();
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    interface ChainTailMarker {
    }

    static class ChainThird implements ChainTailMarker {
        @Subscription(id = "chain-third")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ChainedDiscoveryConfiguration {
        @Bean
        @Lazy
        ChainHead chainHead(ObjectProvider<ChainMarker> second) {
            return new ChainHead(second);
        }

        @Bean
        @Lazy
        ChainMarker chainSecond(ObjectProvider<ChainTailMarker> third) {
            return new ChainSecond(third);
        }

        @Bean
        @Lazy
        ChainTailMarker chainThird() {
            return new ChainThird();
        }
    }

    static class RetriedSubscriber implements Marker {
        @Subscription(id = "retried-handler")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FailingFirstRegistrationConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        // Refuses the first registration and accepts the second, so the retry Spring performs after a failed bean
        // creation is what this fixture exercises.
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            Subscriptions<TestEvent> subscriptions = mock(Subscriptions.class);
            doThrow(new IllegalStateException("refused once"))
                    .doAnswer(invocation -> null)
                    .when(subscriptions).subscribe(eq("retried-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
            return subscriptions;
        }

        @Bean
        @Lazy
        Marker retriedSubscriber() {
            return new RetriedSubscriber();
        }
    }

    interface SameIdMarker {
        @Subscription(id = "shared-across-methods")
        void onTheInterface(TestEvent event);
    }

    static class SameIdSubscriber implements SameIdMarker {
        @Override
        public void onTheInterface(TestEvent event) {
        }

        // Only the concrete class declares the second handler, so the collecting pass sees it a pass later than
        // the first, and has to refuse the id it reuses.
        @Subscription(id = "shared-across-methods")
        void onTheClass(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SameIdAcrossMethodsConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        SameIdMarker sameIdSubscriber() {
            return new SameIdSubscriber();
        }
    }

    interface DifferentIdMarker {
        @Subscription(id = "interface-declared-id")
        void handler(TestEvent event);
    }

    static class DifferentIdSubscriber implements DifferentIdMarker {
        @Override
        @Subscription(id = "class-declared-id")
        public void handler(TestEvent event) {
        }
    }

    static class ClassIdProjectionHolder {
        @Projection(id = "class-declared-id", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OverriddenIdConfiguration {
        @Bean
        @Lazy
        DifferentIdMarker differentIdSubscriber() {
            return new DifferentIdSubscriber();
        }

        @Bean
        ClassIdProjectionHolder classIdProjectionHolder() {
            return new ClassIdProjectionHolder();
        }
    }

    static class FailingFirstProjectionHolder implements Marker {
        // Throws the first time and succeeds the second, so the retry Spring performs after a failed bean creation
        // is what this fixture exercises.
        @Projection(id = "failing-first-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            if (FAILING_PROJECTION_FACTORY_CALLS.incrementAndGet() == 1) {
                throw new IllegalStateException("refused once");
            }
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FailingFirstProjectionConfiguration {
        @Bean
        @Lazy
        Marker failingProjectionHolder() {
            return new FailingFirstProjectionHolder();
        }
    }

    static class WaitingSubscriber implements Marker {
        @Subscription(id = "late-waiting-handler", startAt = org.occurrent.annotation.StartPosition.BEGINNING,
                startupMode = org.occurrent.annotation.StartupMode.WAIT_UNTIL_STARTED)
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ReplayingStartupModeConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        Marker waitingSubscriber() {
            return new WaitingSubscriber();
        }
    }

    public static class LateFinalHandlerSubscriber implements Marker {
        @Subscription(id = "late-final-handler")
        public final void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class LateFinalHandlerConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        Marker finalHandlerSubscriber() {
            return new LateFinalHandlerSubscriber();
        }

        // Wraps the bean only after this post processor's own callback has registered the handler, so the CGLIB
        // proxy does not exist when the registration guards run.
        @Bean
        static BeanPostProcessor lateCglibPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof LateFinalHandlerSubscriber)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setProxyTargetClass(true);
                    proxyFactory.addAdvice((MethodInterceptor) MethodInvocation::proceed);
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    static class TwoHandlerSubscriber implements Marker {
        @Subscription(id = "first-of-two")
        void first(TestEvent event) {
        }

        // Static, so resolveHandlerInvocation refuses it. The refusal has to happen before the handler above
        // subscribes, not after.
        @Subscription(id = "second-of-two")
        static void second(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SecondHandlerFailsConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        Marker twoHandlerSubscriber() {
            return new TwoHandlerSubscriber();
        }
    }

    static class FirstClaimant implements Marker {
        @Subscription(id = "contended-id")
        void on(TestEvent event) {
        }
    }

    static class SecondClaimant implements Marker {
        @Subscription(id = "contended-id")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SameIdOnTwoBeansConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        Marker firstClaimant() {
            return new FirstClaimant();
        }

        @Bean
        @Lazy
        Marker secondClaimant() {
            return new SecondClaimant();
        }
    }

    static class ConcurrentOne implements Marker {
        @Subscription(id = "concurrent-one")
        void on(TestEvent event) {
        }
    }

    static class ConcurrentTwo implements Marker {
        @Subscription(id = "concurrent-two")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class TwoLateBeansConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @Lazy
        Marker concurrentOne() {
            return new ConcurrentOne();
        }

        @Bean
        @Lazy
        Marker concurrentTwo() {
            return new ConcurrentTwo();
        }
    }

    static class DrainTriggeringProjectionHolder {
        @Projection(id = "drain-trigger-projection", source = Source.PUSH, subscriptionModelName = "triggerFeed")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    static class DrainAddedProjectionHolder implements Marker {
        @Projection(id = "drain-added-projection", source = Source.PUSH, subscriptionModelName = "lateFeed")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class DrainDuringCatchUpConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
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

        @Bean
        ViewStateRepository<Integer, String> viewStateRepository() {
            Map<String, Integer> store = new ConcurrentHashMap<>();
            return ViewStateRepository.create(store::get, store::put);
        }

        // Reading this feed is what the catch-up drain does, and it builds the lazy projection holder from inside
        // that read, so a second feed is queued while the drain is still running.
        @Bean
        DomainEventFeed<TestEvent> triggerFeed(CloudEventConverter<TestEvent> converter, ApplicationContext context) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    if (DRAIN_TRIGGERED.compareAndSet(false, true)) {
                        context.getBean("drainAddedProjectionHolder");
                    }
                    return Stream.empty();
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(reader, converter, event -> "k");
        }

        @Bean
        DomainEventFeed<TestEvent> lateFeed(CloudEventConverter<TestEvent> converter) {
            PositionOrderedReader reader = new PositionOrderedReader() {
                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    LATE_FEED_READS.incrementAndGet();
                    return Stream.empty();
                }

                @Override
                public long currentPosition() {
                    return 0;
                }

                @Override
                public boolean writesPosition() {
                    return true;
                }
            };
            return new DomainEventFeed<>(reader, converter, event -> "k");
        }

        @Bean
        DrainTriggeringProjectionHolder drainTriggeringProjectionHolder() {
            return new DrainTriggeringProjectionHolder();
        }

        @Bean
        @Lazy
        Marker drainAddedProjectionHolder() {
            return new DrainAddedProjectionHolder();
        }
    }

    static class FirstClashingSubscriber implements Marker {
        @Subscription(id = "owned-by-the-projection")
        void on(TestEvent event) {
        }
    }

    static class SecondClashingSubscriber implements Marker {
        @Subscription(id = "owned-by-the-projection")
        void on(TestEvent event) {
        }
    }

    static class OwningProjectionHolder {
        @Projection(id = "owned-by-the-projection", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoClashingLateBeansConfiguration {
        @Bean
        OwningProjectionHolder owningProjectionHolder() {
            return new OwningProjectionHolder();
        }

        @Bean
        @Lazy
        Marker firstClashingSubscriber() {
            return new FirstClashingSubscriber();
        }

        @Bean
        @Lazy
        Marker secondClashingSubscriber() {
            return new SecondClashingSubscriber();
        }
    }

    static class OwningSubscriber {
        @Subscription(id = "owned-by-the-subscription")
        void on(TestEvent event) {
        }
    }

    static class FirstClashingProjection implements Marker {
        @Projection(id = "owned-by-the-subscription", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    static class SecondClashingProjection implements Marker {
        @Projection(id = "owned-by-the-subscription", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoClashingLateProjectionsConfiguration {
        @Bean
        OwningSubscriber owningSubscriber() {
            return new OwningSubscriber();
        }

        @Bean
        @Lazy
        Marker firstClashingProjection() {
            return new FirstClashingProjection();
        }

        @Bean
        @Lazy
        Marker secondClashingProjection() {
            return new SecondClashingProjection();
        }
    }

    static class MixedDescriptorHolder {
        @Projection(id = "mixed-descriptor-projection", source = Source.PUSH)
        @org.occurrent.annotation.Snapshot(id = "mixed-descriptor-snapshot")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> descriptor() {
            return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(TestEvent.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MixedDescriptorConfiguration {
        @Bean
        MixedDescriptorHolder mixedDescriptorHolder() {
            return new MixedDescriptorHolder();
        }
    }

    record TestEvent() {
    }

    static class NoopCloudEventConverter implements CloudEventConverter<TestEvent> {
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
    }
}
