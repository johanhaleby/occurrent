/*
 *
 *  Copyright 2024 Johan Haleby
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

import org.jspecify.annotations.NonNull;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Reactive counterpart of the blocking {@code OccurrentBlockingAnnotationBeanPostProcessor}. It supports the
 * {@link Subscription}, {@link StreamSubscription} and {@link DcbSubscription} annotations for the reactive (Project
 * Reactor) stack. The stack-neutral reflection and event-type resolution is shared with the blocking processor through
 * {@link SubscriptionAnnotations}.
 * <p>
 * The reactive stream (non-DCB) catch-up model replays only by position, so a {@link StreamSubscription} that starts
 * at a specific time ({@code startAtISO8601} or {@code startAtTimeEpochMillis}) fails loud, position replay cannot
 * resolve a wall-clock time to a position. {@code BEGINNING_OF_TIME} replays from position 0 on any STREAM store
 * that writes position, including a combined STREAM and DCB store, and fails loud otherwise. {@code NOW} and
 * {@code DEFAULT} are always supported. DCB subscriptions replay history by position via the reactive DCB catch-up
 * model, matching the blocking behavior. The capability-agnostic {@link Subscription} replays over the unified global
 * position, so {@code BEGINNING} replays from position 0 and {@code startAtGlobalPosition} from a specific position,
 * both delivering events of every capability.
 * <p>
 * This class is a thin coordinator: it owns the Spring lifecycle wiring and the shared {@link #registeredIds} id
 * registry, and delegates the actual annotation processing to the package-private collaborators built in
 * {@link #setApplicationContext}.
 */
class OccurrentReactiveAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, SmartInitializingSingleton, DisposableBean {

    private ApplicationContext applicationContext;

    // Every subscription and projection id must be unique, since it is the durable checkpoint key. Subscription ids are
    // added as their annotations are processed (before singletons finish), projection ids when they register below.
    // Shared as a single instance across every registrar so id uniqueness is enforced across all annotation kinds.
    private final Set<String> registeredIds = new HashSet<>();

    private SubscriptionAnnotationRegistrar subscriptionRegistrar;
    private ProjectionAnnotationRegistrar projectionRegistrar;
    private SnapshotAnnotationRegistrar snapshotRegistrar;

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        StartPositionSupport startPositionSupport = new StartPositionSupport(applicationContext);
        this.subscriptionRegistrar = new SubscriptionAnnotationRegistrar(applicationContext, startPositionSupport);
        this.projectionRegistrar = new ProjectionAnnotationRegistrar(applicationContext, registeredIds, startPositionSupport);
        this.snapshotRegistrar = new SnapshotAnnotationRegistrar(applicationContext, registeredIds, startPositionSupport);
    }

    // Still a BeanPostProcessor only so the static @Bean factory method below registers it ahead of ordinary beans;
    // postProcessBeforeInitialization and postProcessAfterInitialization do no work and use the interface's defaults.

    // @Projection and @Snapshot factory methods, and @Subscription, @StreamSubscription, @DcbSubscription and
    // @SynchronousSubscription handler methods, register after all singletons are instantiated: the factory has to
    // be invoked to obtain the descriptor, and its collaborators (the store, the subscription model) must already be
    // wired. Every handler resolves its invocation through applicationContext.getBean(beanName), which by this point
    // always returns the fully proxied singleton, so advice such as @Transactional applies to every delivery,
    // including a WAIT_UNTIL_STARTED history replay, not just the ones after startup. First collect every
    // subscription id so a projection or snapshot cannot reuse one, then register the subscriptions, then each
    // projection, catch up domain-push feeds, then register each snapshot.
    @Override
    public void afterSingletonsInstantiated() {
        // A presence check, not a resolution: getBeanProvider(...).getIfAvailable() throws NoUniqueBeanDefinitionException
        // the moment two Subscribable beans exist (an application's own asynchronous model plus the register-only
        // SynchronousSubscriptionModel this starter always contributes), which starts failing every context the
        // instant subscriptions are enabled, whether or not any bean here uses an annotation at all. This only needs
        // to know whether at least one candidate exists, so getBeanNamesForType (which never throws on ambiguity) is
        // the right tool; which one is meant is resolved later, per annotation, by the actual registrars.
        // SynchronousSubscriptionModel is itself a Subscribable (it extends RegisteringSubscribable), so it is
        // already covered by the check below and needs no check of its own.
        //
        // A misconfigured @Subscription method (more than one subscription annotation, a @DcbSubscription with no
        // event parameter) has to fail fast whether or not a Subscribable bean exists at all, so subscription
        // scanning and registration run unconditionally below, ahead of this check. Only @Projection and @Snapshot,
        // which cannot register anything without a Subscribable-backed DSL bean, are behind it.
        boolean subscribableExists = applicationContext.getBeanNamesForType(Subscribable.class).length > 0;
        List<Object[]> projectionMethods = new ArrayList<>();
        List<Object[]> snapshotMethods = new ArrayList<>();
        // Iteration order of getBeanDefinitionNames() is deterministic, so a LinkedHashSet keeps registration order
        // reproducible across runs.
        Set<String> subscriptionBeanNames = new LinkedHashSet<>();
        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Class<?> type;
            try {
                type = resolveScanType(beanName);
            } catch (RuntimeException e) {
                continue;
            }
            if (type == null) {
                continue;
            }
            for (Method method : type.getDeclaredMethods()) {
                collectSubscriptionId(beanName, method, subscriptionBeanNames);
                if (!subscribableExists) {
                    continue;
                }
                org.occurrent.annotation.Projection projection = AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Projection.class);
                if (projection != null) {
                    projectionMethods.add(new Object[]{beanName, method, projection});
                }
                org.occurrent.annotation.Snapshot snapshot = AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Snapshot.class);
                if (snapshot != null) {
                    snapshotMethods.add(new Object[]{beanName, method, snapshot});
                }
            }
        }
        for (String beanName : subscriptionBeanNames) {
            subscriptionRegistrar.registerSubscriptions(applicationContext.getBean(beanName), resolveScanType(beanName));
        }
        if (!subscribableExists) {
            return;
        }
        for (Object[] pm : projectionMethods) {
            projectionRegistrar.processProjectionAnnotation(applicationContext.getBean((String) pm[0]), (Method) pm[1], (org.occurrent.annotation.Projection) pm[2]);
        }
        // Catch up each domain-push feed once, after all its projections are registered.
        projectionRegistrar.catchUpCollectedFeeds();
        for (Object[] sm : snapshotMethods) {
            snapshotRegistrar.processSnapshotAnnotation(applicationContext.getBean((String) sm[0]), (Method) sm[1], (org.occurrent.annotation.Snapshot) sm[2]);
        }
    }

    // Stop every catch-up the projection registrar started, so no replay outlives the context that owns the store it
    // is folding into. The blocking twin has had this hook since the push catch-up gained a life cycle. This class
    // implemented no destroy callback at all until the reactor model gained one too.
    @Override
    public void destroy() {
        if (projectionRegistrar != null) {
            projectionRegistrar.close();
        }
    }

    // getType(beanName) predicts the type from the bean definition without forcing creation, which is what lets a
    // @Lazy bean stay uncreated until a registrar actually needs it, but once a bean is already a singleton, getType
    // returns that instance's own class, a JDK dynamic proxy included, and ClassUtils.getUserClass only strips
    // CGLIB's naming convention, not a JDK proxy. Scanning that class finds nothing, since a JDK proxy implements
    // only its interfaces, so an already-created bean's annotation went undetected rather than reaching the
    // resolveHandlerInvocation guard that exists to catch exactly this. SubscriptionAnnotations.ultimateTarget
    // unwraps either proxy kind, through any number of nested layers, given the real instance, so an already-created
    // bean is resolved through it instead, the same unwrap invokeDescriptorFactory already uses for a descriptor
    // bean's own factory method. ultimateTarget only unwraps an Advised proxy though, and Spring's own CGLIB
    // enhancement of a proxyBeanMethods = true @Configuration class is not one, so a subscription-annotated bean
    // that happens to be such a class still needs ClassUtils.getUserClass afterward to strip that generated
    // subclass, the same normalization the getType(beanName) branch below already applies.
    //
    // containsSingleton(beanName) is also true once a FactoryBean itself is created, whether or not its product
    // has been. getBean(beanName) dereferences that factory, so calling it here for every such name would create a
    // product nothing has asked for yet, whatever the factory's own object creation does. isFactoryBean(beanName)
    // keeps a FactoryBean-backed name on the metadata-only path instead, at the cost of missing a product that
    // happens to already be a JDK proxy, a narrower case than the one this method exists to fix.
    //
    // A bean neither branch has created yet, an uncreated @Lazy bean or an uncreated FactoryBean product, stays on
    // the metadata-only getType(beanName) branch below by construction, since forcing it here to read its real class
    // would defeat the laziness the FactoryBean case above is already written to preserve. getType's prediction can
    // fall short of the bean's eventual concrete class, a @Bean factory method declared to return an interface being
    // the common shape, and an annotation the concrete class alone carries then goes undetected, with no rescan once
    // the bean is later created, since afterSingletonsInstantiated runs this whole scan exactly once. #981 tracks a
    // fix that keeps this scan lazy while also closing that gap.
    private Class<?> resolveScanType(String beanName) {
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        if (beanFactory.containsSingleton(beanName) && !beanFactory.isFactoryBean(beanName)) {
            return ClassUtils.getUserClass(SubscriptionAnnotations.ultimateTarget(applicationContext.getBean(beanName)).getClass());
        }
        Class<?> type = applicationContext.getType(beanName);
        return type == null ? null : ClassUtils.getUserClass(type);
    }

    // A bean carrying any of the four annotations goes into subscriptionBeanNames so registerSubscriptions runs for
    // it exactly once, above.
    private void collectSubscriptionId(String beanName, Method method, Set<String> subscriptionBeanNames) {
        StreamSubscription s = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
        if (s != null) {
            registeredIds.add(s.id());
            subscriptionBeanNames.add(beanName);
        }
        Subscription a = AnnotationUtils.findAnnotation(method, Subscription.class);
        if (a != null) {
            registeredIds.add(a.id());
            subscriptionBeanNames.add(beanName);
        }
        DcbSubscription d = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
        if (d != null) {
            registeredIds.add(d.id());
            subscriptionBeanNames.add(beanName);
        }
        SynchronousSubscription sy = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
        if (sy != null) {
            registeredIds.add(sy.id());
            subscriptionBeanNames.add(beanName);
        }
    }
}
