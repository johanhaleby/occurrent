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

package org.occurrent.springboot.blocking;

import org.jspecify.annotations.NonNull;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Implements support for the {@link Subscription}, {@link StreamSubscription} and {@link DcbSubscription} annotations in
 * Spring Boot. The stack-neutral reflection and event-type resolution is shared with the reactive processor through
 * {@link SubscriptionAnnotations}.
 * <p>
 * This class is a thin coordinator: it implements the Spring lifecycle interfaces and orchestrates the per-annotation
 * registrars ({@link SubscriptionAnnotationRegistrar}, {@link ProjectionAnnotationRegistrar},
 * {@link SnapshotAnnotationRegistrar}, {@link SagaAnnotationRegistrar}) built on top of {@link StartPositionSupport}.
 */
class OccurrentBlockingAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, SmartInitializingSingleton, DisposableBean {

    private ApplicationContext applicationContext;
    // One shared duplicate-id registry across every registrar: all subscription ids are collected before projections,
    // snapshots and sagas each check-and-add against it.
    private final Set<String> registeredIds = new HashSet<>();
    private SubscriptionAnnotationRegistrar subscriptionRegistrar;
    private ProjectionAnnotationRegistrar projectionRegistrar;
    private SnapshotAnnotationRegistrar snapshotRegistrar;
    private SagaAnnotationRegistrar sagaRegistrar;

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        // A BeanPostProcessor is fully Aware-initialized before it post-processes any other bean, so building the
        // registrars here (rather than lazily) is safe and keeps the lifecycle methods thin.
        this.applicationContext = applicationContext;
        StartPositionSupport startPositionSupport = new StartPositionSupport(applicationContext);
        this.subscriptionRegistrar = new SubscriptionAnnotationRegistrar(applicationContext, startPositionSupport);
        this.projectionRegistrar = new ProjectionAnnotationRegistrar(applicationContext, startPositionSupport, registeredIds);
        this.snapshotRegistrar = new SnapshotAnnotationRegistrar(applicationContext, startPositionSupport, registeredIds);
        this.sagaRegistrar = new SagaAnnotationRegistrar(applicationContext, startPositionSupport, registeredIds);
    }

    // Still a BeanPostProcessor only so the static @Bean factory method below registers it ahead of ordinary beans;
    // postProcessBeforeInitialization and postProcessAfterInitialization do no work and use the interface's defaults.

    // @Projection factory methods, and @Subscription, @StreamSubscription, @DcbSubscription and
    // @SynchronousSubscription handler methods, register after all singletons are instantiated: the factory has to
    // be invoked to obtain the descriptor, and its collaborators (the store, the subscription model) must already be
    // wired. Every handler resolves its invocation through applicationContext.getBean(beanName), which by this point
    // always returns the fully proxied singleton, so advice such as @Transactional applies to every delivery,
    // including a WAIT_UNTIL_STARTED history replay, not just the ones after startup. First collect every
    // subscription id so a projection cannot reuse one and so the fencing check below can be asked about each one,
    // then register the subscriptions, then the projections.
    //
    // @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription methods register before the
    // fencing check below runs, so one can already write a checkpoint before the check inspects idsToCheck.
    // Pre-existing, not introduced by this reorder. CheckpointStorageCannotFenceSubscriptionException's javadoc
    // covers it.
    @Override
    public void afterSingletonsInstantiated() {
        // Reflects over method signatures only, no store access or checkpoint write, so running it before any
        // registration is safe. Spring creates this bean before CheckpointFencingConfigurationCheck's own bean, so
        // a check that instead waited for its own SmartInitializingSingleton callback would run after a catch-up
        // write had already happened.
        List<Object[]> projectionMethods = new ArrayList<>();
        List<Object[]> snapshotMethods = new ArrayList<>();
        List<Object[]> sagaMethods = new ArrayList<>();
        // Only an id whose own registration path reaches CheckpointStorage, kept out of idsToCheck otherwise even
        // though it stays in registeredIds for duplicate detection.
        // CheckpointStorageCannotFenceSubscriptionException's javadoc says exactly which ids that is.
        Set<String> idsToCheck = new HashSet<>();
        // Iteration order of getBeanDefinitionNames() is deterministic, so a LinkedHashSet keeps registration order
        // reproducible across runs.
        Set<String> subscriptionBeanNames = new LinkedHashSet<>();
        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Class<?> type;
            try {
                type = applicationContext.getType(beanName);
            } catch (RuntimeException e) {
                continue;
            }
            if (type == null) {
                continue;
            }
            for (Method method : ClassUtils.getUserClass(type).getDeclaredMethods()) {
                collectSubscriptionId(beanName, method, idsToCheck, subscriptionBeanNames);
                org.occurrent.annotation.Projection projection = AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Projection.class);
                if (projection != null) {
                    projectionMethods.add(new Object[]{beanName, method, projection});
                    if (projection.mode() != org.occurrent.annotation.Mode.SYNCHRONOUS && checkpointsWhenPush(projection.source(), projection.catchup())
                            && !isDomainEventFeedFed(projection)) {
                        idsToCheck.add(projection.id());
                    }
                }
                org.occurrent.annotation.Snapshot snapshot = AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Snapshot.class);
                if (snapshot != null) {
                    snapshotMethods.add(new Object[]{beanName, method, snapshot});
                    if (snapshot.mode() != org.occurrent.annotation.Mode.SYNCHRONOUS) {
                        idsToCheck.add(snapshot.id());
                    }
                }
                org.occurrent.annotation.Saga saga = AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Saga.class);
                if (saga != null) {
                    sagaMethods.add(new Object[]{beanName, method, saga});
                    if (checkpointsWhenPush(saga.source(), saga.catchup())) {
                        idsToCheck.add(saga.id());
                    }
                }
            }
        }
        for (String beanName : subscriptionBeanNames) {
            // getType(beanName), not bean.getClass(): a JDK dynamic proxy's class implements only interfaces, so
            // scanning it for annotated methods would miss any declared on the concrete class.
            Class<?> userClass = ClassUtils.getUserClass(applicationContext.getType(beanName));
            subscriptionRegistrar.registerSubscriptions(applicationContext.getBean(beanName), userClass);
        }
        CheckpointFencingConfigurationCheck.check(applicationContext, idsToCheck);
        for (Object[] pm : projectionMethods) {
            projectionRegistrar.processProjectionAnnotation(applicationContext.getBean((String) pm[0]), (Method) pm[1], (org.occurrent.annotation.Projection) pm[2]);
        }
        // Catch up each domain-push feed once, after all its projections are registered.
        projectionRegistrar.catchUpCollectedFeeds();
        for (Object[] sm : snapshotMethods) {
            snapshotRegistrar.processSnapshotAnnotation(applicationContext.getBean((String) sm[0]), (Method) sm[1], (org.occurrent.annotation.Snapshot) sm[2]);
        }
        for (Object[] gm : sagaMethods) {
            sagaRegistrar.processSagaAnnotation(applicationContext.getBean((String) gm[0]), (Method) gm[1], (org.occurrent.annotation.Saga) gm[2]);
        }
    }

    // Every id here goes into registeredIds, the shared duplicate-id registry. Only the three that reach
    // CheckpointStorage also go into idsToCheck. @SynchronousSubscription writes no checkpoint at all, so its id is
    // registered for duplicate detection only, never asked about by the fencing check. A bean carrying any of the
    // four annotations goes into subscriptionBeanNames so registerSubscriptions runs for it exactly once, below.
    private void collectSubscriptionId(String beanName, Method method, Set<String> idsToCheck, Set<String> subscriptionBeanNames) {
        StreamSubscription s = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
        if (s != null) {
            registeredIds.add(s.id());
            idsToCheck.add(s.id());
            subscriptionBeanNames.add(beanName);
        }
        Subscription a = AnnotationUtils.findAnnotation(method, Subscription.class);
        if (a != null) {
            registeredIds.add(a.id());
            idsToCheck.add(a.id());
            subscriptionBeanNames.add(beanName);
        }
        DcbSubscription d = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
        if (d != null) {
            registeredIds.add(d.id());
            idsToCheck.add(d.id());
            subscriptionBeanNames.add(beanName);
        }
        SynchronousSubscription sy = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
        if (sy != null) {
            registeredIds.add(sy.id());
            subscriptionBeanNames.add(beanName);
        }
    }

    // True unless source = PUSH and catchup = NONE, the one combination @Projection and @Saga share where the bare
    // push feed is used directly and no CatchupThenPushSubscriptionModel, the one that resolves CheckpointStorage
    // for either annotation, is ever built.
    private static boolean checkpointsWhenPush(org.occurrent.annotation.Source source, org.occurrent.annotation.Catchup catchup) {
        return source != org.occurrent.annotation.Source.PUSH || catchup != org.occurrent.annotation.Catchup.NONE;
    }

    // A DomainEventFeed-fed @Projection(source = PUSH) never resolves CheckpointStorage, whatever catchup says.
    // Resolved read-only, so an id whose feed bean type cannot be determined this way stays in idsToCheck rather
    // than being excluded on a guess.
    private boolean isDomainEventFeedFed(org.occurrent.annotation.Projection projection) {
        if (projection.source() != org.occurrent.annotation.Source.PUSH) {
            return false;
        }
        Class<?> feedType = SubscriptionAnnotations.resolveFeedBeanType(applicationContext, projection.subscriptionModel(),
                projection.subscriptionModelName(), PushSubscriptionModel.class, DomainEventFeed.class);
        return feedType != null && DomainEventFeed.class.isAssignableFrom(feedType);
    }

    @Override
    public void destroy() {
        // In a finally so a saga that fails to close still leaves no projection replay running.
        try {
            sagaRegistrar.close();
        } finally {
            projectionRegistrar.close();
        }
    }
}
