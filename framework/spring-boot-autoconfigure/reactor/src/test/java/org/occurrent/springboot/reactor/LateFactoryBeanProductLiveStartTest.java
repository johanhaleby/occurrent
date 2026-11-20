/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.occurrent.springboot.reactor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.GlobalCheckpoint;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import reactor.core.publisher.Mono;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Each test moves the event store's boundary while a FactoryBean's product is being built, or after its factory was
 * created, and pins the boundary the product's handler registers from. The context starts at boundary 1.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class LateFactoryBeanProductLiveStartTest {

    private static final long AWAIT_SECONDS = 20;
    private static final long GC_DEADLINE_NANOS = 10_000_000_000L;
    private static final long STARTUP_BOUNDARY = 1;

    private static final AtomicLong BOUNDARY = new AtomicLong();
    private static final List<String> NOT_BEFORE = new CopyOnWriteArrayList<>();
    private static final AtomicInteger SCOPE_CONTEXT = new AtomicInteger();
    private static final AtomicInteger GET_OBJECT_CALLS = new AtomicInteger();
    private static final AtomicInteger NESTED_FACTORIES = new AtomicInteger();
    private static final AtomicInteger STALE_ATTEMPTS = new AtomicInteger();
    private static ReentrantFactory reentrantFactory;
    private static final List<WeakReference<StaleFactory>> STALE_FACTORIES = new CopyOnWriteArrayList<>();
    private static final CountDownLatch[] REACHED_GET_OBJECT = latches(3);
    private static final CountDownLatch[] RELEASE_GET_OBJECT = latches(3);

    private final ExecutorService threads = Executors.newFixedThreadPool(3);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withInitializer(context -> context.getBeanFactory().registerScope(ContextScope.NAME, new ContextScope()))
            .withUserConfiguration(ProductFactoryConfiguration.class);

    @BeforeEach
    void reset() {
        BOUNDARY.set(STARTUP_BOUNDARY);
        NOT_BEFORE.clear();
        SCOPE_CONTEXT.set(0);
        GET_OBJECT_CALLS.set(0);
        NESTED_FACTORIES.set(0);
        STALE_ATTEMPTS.set(0);
        reentrantFactory = null;
        STALE_FACTORIES.clear();
        for (int i = 0; i < REACHED_GET_OBJECT.length; i++) {
            REACHED_GET_OBJECT[i] = new CountDownLatch(1);
            RELEASE_GET_OBJECT[i] = new CountDownLatch(1);
        }
    }

    @AfterEach
    void releaseEveryone() {
        for (CountDownLatch release : RELEASE_GET_OBJECT) {
            release.countDown();
        }
        threads.shutdownNow();
    }

    /**
     * The factory is a singleton definition whose isSingleton is false. The first getObject returns a product with no
     * handler and the second one a product with a handler, held while the first product completes and a write moves
     * the boundary.
     */
    @Test
    void a_product_that_is_built_after_another_product_of_its_factory_completed_registers_from_the_boundary_its_factory_read() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            Future<Object> first = threads.submit(() -> context.getBean("twoProductFactory"));
            assertThat(REACHED_GET_OBJECT[1].await(AWAIT_SECONDS, TimeUnit.SECONDS)).describedAs("first getObject was reached").isTrue();
            Future<Object> second = threads.submit(() -> context.getBean("twoProductFactory"));
            assertThat(REACHED_GET_OBJECT[2].await(AWAIT_SECONDS, TimeUnit.SECONDS)).describedAs("second getObject was reached").isTrue();

            RELEASE_GET_OBJECT[1].countDown();
            first.get(AWAIT_SECONDS, TimeUnit.SECONDS);
            BOUNDARY.set(3);
            RELEASE_GET_OBJECT[2].countDown();
            second.get(AWAIT_SECONDS, TimeUnit.SECONDS);

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "two-product-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    /**
     * The factory is created on one thread and its product asked for on another, in a scope both share.
     * The boundary moves inside getObject.
     */
    @Test
    void a_product_of_a_scoped_factory_asked_for_on_another_thread_registers_from_the_boundary_its_factory_read() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            onNewThread(() -> context.getBean("&movingScopedFactory"));
            onNewThread(() -> context.getBean("movingScopedFactory"));

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "moving-scoped-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    /**
     * The scope keys its instances by a context id the test sets, whatever thread asks. Each context creates its own
     * factory on its own thread, and the first product of the second context is asked for on a third thread.
     */
    @Test
    void the_first_product_of_a_factory_in_a_second_scope_context_registers_from_the_boundary_of_that_factory_on_a_thread_that_created_no_factory() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            SCOPE_CONTEXT.set(1);
            BOUNDARY.set(2);
            onNewThread(() -> context.getBean("&contextFactory"));
            SCOPE_CONTEXT.set(2);
            BOUNDARY.set(3);
            onNewThread(() -> context.getBean("&contextFactory"));
            onNewThread(() -> context.getBean("contextFactory"));

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "context-handler")).isEqualTo(GlobalCheckpoint.of(3));
        });
    }

    @Test
    void a_product_of_a_singleton_factory_created_by_an_earlier_call_registers_from_the_boundary_its_factory_read() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            context.getBean("&laterSingletonFactory");
            BOUNDARY.set(3);
            context.getBean("laterSingletonFactory");

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "later-singleton-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    /**
     * The factory is created while the context starts, and the boundary moves inside getObject.
     */
    @Test
    void a_product_of_a_factory_created_before_the_startup_scan_ended_registers_from_the_boundary_of_the_context() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            context.getBean("startupFactory");

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "startup-handler")).isEqualTo(GlobalCheckpoint.of(STARTUP_BOUNDARY));
        });
    }

    /**
     * The outer getObject moves the boundary, then asks for the factory's product again on its own thread. That builds
     * another factory and a product with a handler of its own.
     */
    @Test
    void a_prototype_product_whose_getObject_builds_another_product_of_its_name_registers_from_the_boundary_its_own_factory_read() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            context.getBean("nestedProductFactory");

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "nested-outer-handler")).isEqualTo(GlobalCheckpoint.of(2));
            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "nested-inner-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    /**
     * The outer getObject moves the boundary, then asks for the factory itself on its own thread, which builds another
     * factory and no product.
     */
    @Test
    void a_prototype_product_whose_getObject_builds_another_factory_of_its_name_registers_from_the_boundary_its_own_factory_read() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            context.getBean("nestedFactoryFactory");

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "nested-outer-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    /**
     * A prototype factory whose getObject threw stays reachable, and a second creation on the same thread then registers
     * from the boundary of the failed one. Once it is collected, a third creation registers from its own.
     */
    @Test
    void a_prototype_product_registers_from_the_boundary_of_an_uncollected_failed_factory_and_from_its_own_once_that_is_collected() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            assertThatThrownBy(() -> context.getBean("staleFactory")).hasRootCauseMessage("creation failed");
            assertThat(STALE_FACTORIES).describedAs("the failed creation built one factory").hasSize(1);
            List<Object> failed = new ArrayList<>();
            failed.add(STALE_FACTORIES.get(0).get());

            BOUNDARY.set(3);
            context.getBean("staleFactory");
            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "stale-second-handler")).isEqualTo(GlobalCheckpoint.of(2));

            failed.clear();
            assertThat(uncollectedAfterGc(STALE_FACTORIES.subList(0, 1))).describedAs("the failed factory still reachable after gc").isZero();
            BOUNDARY.set(4);
            context.getBean("staleFactory");
            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "stale-third-handler")).isEqualTo(GlobalCheckpoint.of(4));
        });
    }

    /**
     * The outer getObject moves the boundary, then asks for its own product again on its own thread, which builds a
     * second factory while the first is unclaimed.
     */
    @Test
    void a_prototype_factory_built_while_another_of_its_name_is_unclaimed_reads_no_earlier_than_the_boundary_of_that_one() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            NOT_BEFORE.clear();

            BOUNDARY.set(2);
            context.getBean("nestedProductFactory");

            assertThat(NOT_BEFORE).containsExactly(null, "2");
        });
    }

    @Test
    void each_prototype_factory_after_the_first_on_a_thread_reads_no_earlier_than_the_boundary_of_the_newest_unclaimed_one() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            NOT_BEFORE.clear();

            onNewThread(() -> {
                BOUNDARY.set(2);
                context.getBean("&unclaimedFactory");
                BOUNDARY.set(3);
                context.getBean("&unclaimedFactory");
                BOUNDARY.set(4);
                return context.getBean("&unclaimedFactory");
            });

            assertThat(NOT_BEFORE).containsExactly(null, "2", "3");
        });
    }

    @Test
    void the_first_prototype_factory_on_a_thread_reads_with_no_notBefore_whatever_another_thread_left_unclaimed() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            NOT_BEFORE.clear();

            BOUNDARY.set(2);
            onNewThread(() -> context.getBean("&unclaimedFactory"));
            BOUNDARY.set(3);
            onNewThread(() -> context.getBean("&unclaimedFactory"));

            assertThat(NOT_BEFORE).containsExactly(null, null);
        });
    }

    @Test
    void a_singleton_factory_created_after_startup_reads_with_no_notBefore() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            NOT_BEFORE.clear();

            BOUNDARY.set(2);
            context.getBean("&laterSingletonFactory");

            assertThat(NOT_BEFORE).containsExactly((String) null);
        });
    }

    @Test
    void a_scoped_factory_created_while_another_of_its_name_is_alive_in_another_scope_context_reads_with_no_notBefore() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            NOT_BEFORE.clear();

            SCOPE_CONTEXT.set(1);
            BOUNDARY.set(2);
            onNewThread(() -> context.getBean("&contextFactory"));
            SCOPE_CONTEXT.set(2);
            BOUNDARY.set(3);
            onNewThread(() -> context.getBean("&contextFactory"));

            assertThat(NOT_BEFORE).containsExactly(null, null);
        });
    }

    @Test
    void a_plain_bean_created_after_startup_reads_with_no_notBefore() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            NOT_BEFORE.clear();

            BOUNDARY.set(2);
            context.getBean("plainLateHandler");

            assertThat(NOT_BEFORE).containsExactly((String) null);
            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "plain-late-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    /**
     * The product is a FactoryBean too, so it reaches the post-processor the way its factory did.
     */
    @Test
    void a_product_that_is_a_FactoryBean_of_a_singleton_factory_created_by_an_earlier_call_registers_from_the_boundary_its_factory_read() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            context.getBean("&factoryProductSingletonFactory");
            BOUNDARY.set(3);
            context.getBean("factoryProductSingletonFactory");

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "factory-product-singleton-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    @Test
    void a_product_that_is_a_FactoryBean_of_a_prototype_factory_registers_from_the_boundary_its_factory_read() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            context.getBean("factoryProductPrototypeFactory");

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "factory-product-prototype-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    @Test
    void a_prototype_factory_whose_product_is_a_FactoryBean_leaves_no_claim_for_the_next_factory_of_its_name_on_the_thread() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            NOT_BEFORE.clear();

            BOUNDARY.set(2);
            context.getBean("factoryProductPrototypeFactory");
            BOUNDARY.set(3);
            context.getBean("&factoryProductPrototypeFactory");

            assertThat(NOT_BEFORE).containsExactly(null, null);
        });
    }

    /**
     * One FactoryBean instance is a lazy bean under two names. Its afterPropertiesSet asks for the second one, so the
     * container initializes the same instance again, under the second name, inside its first initialization.
     */
    @Test
    void a_factory_that_the_container_initializes_again_under_another_name_inside_its_own_initialization_is_still_a_factory_when_the_first_initialization_ends() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            BOUNDARY.set(2);
            assertThatCode(() -> context.getBean("reentrantOuterFactory")).describedAs("asking for the product of the outer name").doesNotThrowAnyException();

            assertThat(ConcurrentLateFactoryBeanLiveStartTest.startOfLateRegistration(context, "reentrant-handler")).isEqualTo(GlobalCheckpoint.of(2));
        });
    }

    private static <T> T onNewThread(Callable<T> work) throws Exception {
        FutureTask<T> task = new FutureTask<>(work);
        new Thread(task, "late-factory-test").start();
        return task.get(AWAIT_SECONDS, TimeUnit.SECONDS);
    }

    private static long uncollectedAfterGc(List<? extends WeakReference<?>> references) {
        long deadline = System.nanoTime() + GC_DEADLINE_NANOS;
        long uncollected;
        while ((uncollected = references.stream().filter(reference -> reference.get() != null).count()) > 0 && System.nanoTime() < deadline) {
            System.gc();
            byte[] pressure = new byte[1024 * 1024];
            pressure[0] = 1;
        }
        return uncollected;
    }

    private static CountDownLatch[] latches(int count) {
        CountDownLatch[] latches = new CountDownLatch[count];
        for (int i = 0; i < count; i++) {
            latches[i] = new CountDownLatch(1);
        }
        return latches;
    }

    interface LateMarker {
    }

    static class PlainProduct implements LateMarker {
    }

    static class TwoProductHandler implements LateMarker {
        @Subscription(id = "two-product-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onTwoProduct(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class MovingScopedHandler implements LateMarker {
        @Subscription(id = "moving-scoped-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onMovingScoped(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class ContextHandler implements LateMarker {
        @Subscription(id = "context-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onContext(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class LaterSingletonHandler implements LateMarker {
        @Subscription(id = "later-singleton-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onLaterSingleton(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class StartupHandler implements LateMarker {
        @Subscription(id = "startup-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onStartup(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class NestedOuterHandler implements LateMarker {
        @Subscription(id = "nested-outer-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onNestedOuter(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class NestedInnerHandler implements LateMarker {
        @Subscription(id = "nested-inner-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onNestedInner(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class PlainLateHandler implements LateMarker {
        @Subscription(id = "plain-late-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onPlainLate(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class StaleSecondHandler implements LateMarker {
        @Subscription(id = "stale-second-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onStaleSecond(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class StaleThirdHandler implements LateMarker {
        @Subscription(id = "stale-third-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onStaleThird(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    // A product that implements FactoryBean itself and carries a handler
    static class SingletonFactoryProduct implements LateMarker, FactoryBean<Object> {
        @Subscription(id = "factory-product-singleton-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onFactoryProduct(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }

        @Override
        public Object getObject() {
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    static class ReentrantProduct implements LateMarker {
        @Subscription(id = "reentrant-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onReentrant(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class PrototypeFactoryProduct implements LateMarker, FactoryBean<Object> {
        @Subscription(id = "factory-product-prototype-handler", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT, startupMode = StartupMode.BACKGROUND)
        void onFactoryProduct(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }

        @Override
        public Object getObject() {
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    // Each getObject waits on a latch the test releases
    static class TwoProductFactory implements FactoryBean<LateMarker> {
        @Override
        public LateMarker getObject() throws InterruptedException {
            int call = GET_OBJECT_CALLS.incrementAndGet();
            REACHED_GET_OBJECT[call].countDown();
            if (!RELEASE_GET_OBJECT[call].await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("getObject " + call + " was never released");
            }
            return call == 1 ? new PlainProduct() : new TwoProductHandler();
        }

        @Override
        public Class<?> getObjectType() {
            return LateMarker.class;
        }

        @Override
        public boolean isSingleton() {
            return false;
        }
    }

    // Moves the boundary once getObject has started
    static class MovingFactory implements FactoryBean<LateMarker> {
        private final LateMarker product;
        private final boolean singleton;

        MovingFactory(LateMarker product, boolean singleton) {
            this.product = product;
            this.singleton = singleton;
        }

        @Override
        public LateMarker getObject() {
            BOUNDARY.set(BOUNDARY.get() + 1);
            return product;
        }

        @Override
        public Class<?> getObjectType() {
            return LateMarker.class;
        }

        @Override
        public boolean isSingleton() {
            return singleton;
        }
    }

    static class SimpleFactory implements FactoryBean<LateMarker> {
        private final LateMarker product;
        private final boolean singleton;

        SimpleFactory(LateMarker product, boolean singleton) {
            this.product = product;
            this.singleton = singleton;
        }

        @Override
        public LateMarker getObject() {
            return product;
        }

        @Override
        public Class<?> getObjectType() {
            return LateMarker.class;
        }

        @Override
        public boolean isSingleton() {
            return singleton;
        }
    }

    // The first factory built is the outer one. Its getObject moves the boundary and then asks the container for
    // its own name again, for the product or for the factory.
    static class NestedFactory implements FactoryBean<LateMarker>, BeanFactoryAware, BeanNameAware {
        private final boolean asksForProduct;
        private final boolean outer = NESTED_FACTORIES.getAndIncrement() == 0;
        private BeanFactory beanFactory;
        private String beanName;

        NestedFactory(boolean asksForProduct) {
            this.asksForProduct = asksForProduct;
        }

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public void setBeanName(String beanName) {
            this.beanName = beanName;
        }

        @Override
        public LateMarker getObject() {
            if (!outer) {
                return new NestedInnerHandler();
            }
            BOUNDARY.set(BOUNDARY.get() + 1);
            beanFactory.getBean(asksForProduct ? beanName : BeanFactory.FACTORY_BEAN_PREFIX + beanName);
            return new NestedOuterHandler();
        }

        @Override
        public Class<?> getObjectType() {
            return LateMarker.class;
        }

        @Override
        public boolean isSingleton() {
            return false;
        }
    }

    // Its afterPropertiesSet asks for the product of the second name it is registered under, once. That product is
    // built first and has no handler, and the product of the first name, built after it, has one.
    static class ReentrantFactory implements FactoryBean<LateMarker>, BeanFactoryAware, InitializingBean {
        private BeanFactory beanFactory;
        private boolean askedForInnerName;
        private int products;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public void afterPropertiesSet() {
            if (!askedForInnerName) {
                askedForInnerName = true;
                beanFactory.getBean("reentrantInnerFactory");
            }
        }

        @Override
        public LateMarker getObject() {
            return products++ == 0 ? new PlainProduct() : new ReentrantProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return LateMarker.class;
        }

        @Override
        public boolean isSingleton() {
            return true;
        }
    }

    // Fails its first creation, and its later ones build a product with a handler of its own
    static class StaleFactory implements FactoryBean<LateMarker> {
        private final int attempt = STALE_ATTEMPTS.incrementAndGet();

        StaleFactory() {
            STALE_FACTORIES.add(new WeakReference<>(this));
        }

        @Override
        public LateMarker getObject() {
            if (attempt == 1) {
                throw new IllegalStateException("creation failed");
            }
            return attempt == 2 ? new StaleSecondHandler() : new StaleThirdHandler();
        }

        @Override
        public Class<?> getObjectType() {
            return LateMarker.class;
        }

        @Override
        public boolean isSingleton() {
            return false;
        }
    }

    // One instance per bean name in the context the test has set, whatever thread asks
    static class ContextScope implements org.springframework.beans.factory.config.Scope {
        static final String NAME = "context";

        private final Map<String, Object> instances = new HashMap<>();

        @Override
        public synchronized Object get(String name, ObjectFactory<?> objectFactory) {
            return instances.computeIfAbsent(SCOPE_CONTEXT.get() + "/" + name, key -> objectFactory.getObject());
        }

        @Override
        public synchronized Object remove(String name) {
            return instances.remove(SCOPE_CONTEXT.get() + "/" + name);
        }

        @Override
        public void registerDestructionCallback(String name, Runnable callback) {
        }

        @Override
        public Object resolveContextualObject(String key) {
            return null;
        }

        @Override
        public String getConversationId() {
            return NAME;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ProductFactoryConfiguration {
        @Bean
        CloudEventConverter<LateBeanAnnotationRegistrationTest.TestEvent> testEventCloudEventConverter() {
            return new LateBeanAnnotationRegistrationTest.NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<LateBeanAnnotationRegistrationTest.TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        LiveStartCheckpoints liveStartCheckpoints() {
            return liveStart -> GlobalCheckpoint.of(Long.parseLong(liveStart));
        }

        @Bean
        EventStore eventStore() {
            EventStore store = mock(EventStore.class, withSettings().extraInterfaces(PositionOrderedReader.class, CatchupReads.class));
            when(((CatchupReads) store).openCatchupReadScope(any())).thenAnswer(invocation -> {
                String notBefore = invocation.getArgument(0);
                NOT_BEFORE.add(notBefore);
                return Mono.just(mock(CatchupReadScope.class, scopeCall -> switch (scopeCall.getMethod().getName()) {
                    case "liveStart" -> String.valueOf(BOUNDARY.get());
                    case "close" -> Mono.empty();
                    default -> null;
                }));
            });
            return store;
        }

        @Bean
        @Lazy
        TwoProductFactory twoProductFactory() {
            return new TwoProductFactory();
        }

        @Bean
        @Scope(ContextScope.NAME)
        MovingFactory movingScopedFactory() {
            return new MovingFactory(new MovingScopedHandler(), false);
        }

        @Bean
        @Scope(ContextScope.NAME)
        SimpleFactory contextFactory() {
            return new SimpleFactory(new ContextHandler(), false);
        }

        @Bean
        @Lazy
        SimpleFactory laterSingletonFactory() {
            return new SimpleFactory(new LaterSingletonHandler(), true);
        }

        @Bean
        MovingFactory startupFactory() {
            return new MovingFactory(new StartupHandler(), true);
        }

        @Bean
        @Scope("prototype")
        NestedFactory nestedProductFactory() {
            return new NestedFactory(true);
        }

        @Bean
        @Scope("prototype")
        NestedFactory nestedFactoryFactory() {
            return new NestedFactory(false);
        }

        @Bean
        @Scope("prototype")
        SimpleFactory unclaimedFactory() {
            return new SimpleFactory(new PlainProduct(), false);
        }

        @Bean
        @Lazy
        SimpleFactory factoryProductSingletonFactory() {
            return new SimpleFactory(new SingletonFactoryProduct(), true);
        }

        @Bean
        @Scope("prototype")
        SimpleFactory factoryProductPrototypeFactory() {
            return new SimpleFactory(new PrototypeFactoryProduct(), false);
        }

        @Bean
        @Lazy
        LateMarker plainLateHandler() {
            return new PlainLateHandler();
        }

        @Bean
        @Lazy
        ReentrantFactory reentrantOuterFactory() {
            reentrantFactory = new ReentrantFactory();
            return reentrantFactory;
        }

        @Bean
        @Lazy
        ReentrantFactory reentrantInnerFactory() {
            return reentrantFactory;
        }

        @Bean
        @Scope("prototype")
        StaleFactory staleFactory() {
            return new StaleFactory();
        }
    }
}
