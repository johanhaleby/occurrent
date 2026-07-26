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

package org.occurrent.springboot.mongo.reactor;

import org.jspecify.annotations.NonNull;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
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
class OccurrentReactiveAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, SmartInitializingSingleton {

    /**
     * The bean name of the synchronous {@code Subscriptions} DSL declared by the auto-configuration. Resolved by name
     * (rather than by type) so it does not collide with the asynchronous {@code Subscriptions} bean, which is of the
     * same type.
     */
    static final String SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME = "occurrentSynchronousSubscriptions";

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

    @Override
    public Object postProcessBeforeInitialization(Object bean, @NonNull String beanName) throws BeansException {
        return subscriptionRegistrar.postProcessBeforeInitialization(bean, beanName);
    }

    // @Projection and @Snapshot factory methods are registered after all singletons are instantiated, not in
    // postProcessBeforeInitialization: the factory has to be invoked to obtain the descriptor, and its collaborators
    // (the store, the subscription model) must already be wired. First collect every subscription id so a projection
    // or snapshot cannot reuse one, then register each projection, catch up domain-push feeds, then register each
    // snapshot.
    @Override
    public void afterSingletonsInstantiated() {
        if (applicationContext.getBeanProvider(Subscribable.class).getIfAvailable() == null
                && applicationContext.getBeanProvider(SynchronousSubscriptionModel.class).getIfAvailable() == null) {
            return;
        }
        List<Object[]> projectionMethods = new ArrayList<>();
        List<Object[]> snapshotMethods = new ArrayList<>();
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
                collectSubscriptionId(method);
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
        for (Object[] pm : projectionMethods) {
            projectionRegistrar.processProjectionAnnotation(applicationContext.getBean((String) pm[0]), (Method) pm[1], (org.occurrent.annotation.Projection) pm[2]);
        }
        // Catch up each domain-push feed once, after all its projections are registered.
        projectionRegistrar.catchUpCollectedFeeds();
        for (Object[] sm : snapshotMethods) {
            snapshotRegistrar.processSnapshotAnnotation(applicationContext.getBean((String) sm[0]), (Method) sm[1], (org.occurrent.annotation.Snapshot) sm[2]);
        }
    }

    private void collectSubscriptionId(Method method) {
        StreamSubscription s = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
        if (s != null) registeredIds.add(s.id());
        Subscription a = AnnotationUtils.findAnnotation(method, Subscription.class);
        if (a != null) registeredIds.add(a.id());
        DcbSubscription d = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
        if (d != null) registeredIds.add(d.id());
        SynchronousSubscription sy = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
        if (sy != null) registeredIds.add(sy.id());
    }
}
