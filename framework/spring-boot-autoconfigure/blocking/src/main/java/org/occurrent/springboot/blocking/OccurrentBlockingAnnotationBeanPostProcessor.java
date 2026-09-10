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
import org.springframework.beans.factory.FactoryBean;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

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
    // The class the container actually built for a bean, recorded as the container hands the object over. This is
    // the only source that cannot fall short of the real class, see resolveScanType below for why the prediction
    // this replaces can.
    private final Map<String, Class<?>> userClassByBeanName = new ConcurrentHashMap<>();
    // Every handler already registered, so scanning a bean a second time registers only what is new. Guarded by
    // registrationLock, since a bean created after startup is created on whatever thread asked for it.
    private final Set<String> registeredHandlers = new HashSet<>();
    private final Object registrationLock = new Object();
    private volatile boolean startupScanComplete;
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

    // The container hands over the raw instance here, before any proxy wraps it, so this is where a bean's real
    // class is knowable without predicting it. Nothing registers from this callback, since the bean is still being
    // created and a lookup by name for it would deadlock or hand back the unproxied target, which is the whole
    // reason registration moved to afterSingletonsInstantiated.
    //
    // A FactoryBean is skipped, because the container passes the factory itself under the product's own bean name
    // and the factory's class says nothing about the product's. The product arrives at
    // postProcessAfterInitialization instead, under the same name, and is recorded there.
    @Override
    public Object postProcessBeforeInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        if (!(bean instanceof FactoryBean<?>)) {
            userClassByBeanName.putIfAbsent(beanName, userClassOf(bean));
        }
        return bean;
    }

    // Where a bean created after the startup scan registers. A @Lazy bean, or a FactoryBean product nothing has
    // asked for yet, does not exist when afterSingletonsInstantiated runs, and the startup scan therefore reads it
    // through a prediction that can fall short of its real class (resolveScanType says how). The container creates
    // it later, and creating it is exactly what makes its real class knowable, so that is when it registers.
    //
    // Registering here happens at most once per handler, whatever the scope. A prototype passes through this
    // callback once per instance, and a subscription id is the durable checkpoint key, so a second registration of
    // it is never a harmless repeat.
    //
    // The handler target is a supplier because getBean(beanName) throws BeanCurrentlyInCreationException from here.
    // For a singleton, resolving by name per delivery reaches the object the context publishes, whatever a later
    // BeanPostProcessor wrapped it in. For anything else, resolving by name would build a new instance per delivery,
    // so the handler stays bound to the instance this callback received, the way the startup scan already binds a
    // prototype's handler to the single instance it asked for.
    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        if (bean instanceof FactoryBean<?>) {
            return bean;
        }
        userClassByBeanName.putIfAbsent(beanName, userClassOf(bean));
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        if (startupScanComplete && beanFactory.containsBeanDefinition(beanName)) {
            Supplier<Object> handlerTarget = beanFactory.isSingleton(beanName) ? () -> applicationContext.getBean(beanName) : () -> bean;
            synchronized (registrationLock) {
                scan(new String[]{beanName}, name -> bean, name -> handlerTarget);
            }
        }
        return bean;
    }

    private static Class<?> userClassOf(Object bean) {
        return ClassUtils.getUserClass(SubscriptionAnnotations.ultimateTarget(bean).getClass());
    }

    // @Projection factory methods, and @Subscription, @StreamSubscription, @DcbSubscription and
    // @SynchronousSubscription handler methods, register after all singletons are instantiated: the factory has to
    // be invoked to obtain the descriptor, and its collaborators (the store, the subscription model) must already be
    // wired. Every handler resolves its invocation through applicationContext.getBean(beanName), which by this point
    // always returns the fully proxied singleton, so advice such as @Transactional applies to every delivery,
    // including a WAIT_UNTIL_STARTED history replay, not just the ones after startup.
    //
    // The scan runs twice. Registering a bean creates it, and creating it records its real class, so a second pass
    // sees an annotation the first pass's prediction could not, an interface declaring one handler implemented by a
    // class declaring a second. Everything the first pass registered is skipped by handler key, so the second pass
    // registers only what the first could not see. It terminates because a bean's recorded real class never changes
    // once the container has built it.
    @Override
    public void afterSingletonsInstantiated() {
        synchronized (registrationLock) {
            String[] beanNames = applicationContext.getBeanDefinitionNames();
            scan(beanNames, applicationContext::getBean, name -> () -> applicationContext.getBean(name));
            scan(beanNames, applicationContext::getBean, name -> () -> applicationContext.getBean(name));
            startupScanComplete = true;
        }
    }

    // beanResolver hands back the object to read a descriptor factory from and to check the invocation guards
    // against. targetSupplier hands back the object a handler is invoked on, per delivery. They differ only for a
    // bean created after the startup scan, where the object is in hand but its name cannot be resolved until
    // creation finishes.
    //
    // First collect every subscription id so a projection cannot reuse one and so the fencing check below can be
    // asked about each one, then register the subscriptions, then the projections.
    //
    // @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription methods register before the
    // fencing check below runs, so one can already write a checkpoint before the check inspects idsToCheck.
    // Pre-existing, not introduced by this reorder. CheckpointStorageCannotFenceSubscriptionException's javadoc
    // covers it.
    private void scan(String[] beanNames, Function<String, Object> beanResolver, Function<String, Supplier<Object>> targetSupplier) {
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
        for (String beanName : beanNames) {
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
                if (isAlreadyRegistered(beanName, method)) {
                    continue;
                }
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
            Object bean = beanResolver.apply(beanName);
            subscriptionRegistrar.registerSubscriptions(bean, resolveScanType(beanName), targetSupplier.apply(beanName),
                    method -> markRegistered(beanName, method));
        }
        CheckpointFencingConfigurationCheck.check(applicationContext, idsToCheck);
        for (Object[] pm : projectionMethods) {
            if (markRegistered((String) pm[0], (Method) pm[1])) {
                projectionRegistrar.processProjectionAnnotation(beanResolver.apply((String) pm[0]), (Method) pm[1], (org.occurrent.annotation.Projection) pm[2]);
            }
        }
        // Catch up each domain-push feed once, after all its projections are registered.
        projectionRegistrar.catchUpCollectedFeeds();
        for (Object[] sm : snapshotMethods) {
            if (markRegistered((String) sm[0], (Method) sm[1])) {
                snapshotRegistrar.processSnapshotAnnotation(beanResolver.apply((String) sm[0]), (Method) sm[1], (org.occurrent.annotation.Snapshot) sm[2]);
            }
        }
        for (Object[] gm : sagaMethods) {
            if (markRegistered((String) gm[0], (Method) gm[1])) {
                sagaRegistrar.processSagaAnnotation(beanResolver.apply((String) gm[0]), (Method) gm[1], (org.occurrent.annotation.Saga) gm[2]);
            }
        }
    }


    // A handler is identified by its bean name, method name and parameter types, so the same method found again on
    // a rescan is skipped while a second method on the same bean is not. The declaring class is deliberately left
    // out. A method an interface declares and the bean's class overrides is one handler, and the two passes see a
    // different Method for it, so including the declaring class would register that handler's id twice and the
    // second registration would be refused as a duplicate.
    private boolean isAlreadyRegistered(String beanName, Method method) {
        return registeredHandlers.contains(handlerKey(beanName, method));
    }

    // True the first time a handler is registered, false every time after, so a caller registers it exactly once.
    private boolean markRegistered(String beanName, Method method) {
        return registeredHandlers.add(handlerKey(beanName, method));
    }

    private static String handlerKey(String beanName, Method method) {
        return beanName + '#' + method.getName() + Arrays.toString(method.getParameterTypes());
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
    // would defeat the laziness the FactoryBean case above is already written to preserve, and would defeat
    // spring.main.lazy-initialization for a whole application. That branch is a prediction rather than the class.
    // For a definition backed by a factory method, AbstractAutowireCapableBeanFactory.getTypeForFactoryMethod
    // answers with the method's declared return type, or the common ancestor of the overloads when there are
    // several, and its own
    // comment says why ("Can't clearly figure out exact method due to type converting / autowiring!"). A @Bean method
    // declared to return an interface therefore predicts the interface, whatever concrete class it returns, and no
    // bean-definition API can say more, because the concrete class is decided by running the method.
    //
    // So the real class is not predicted here at all. It is recorded when the container hands the object over, in
    // postProcessBeforeInitialization for an ordinary bean and postProcessAfterInitialization for a FactoryBean
    // product, and that recording is what the first branch below reads. The containsSingleton branch after it is
    // for a bean the container built before this post processor was registered as a BeanPostProcessor, which has no
    // recording of its own. A bean the container has not built has neither, and registers when it is built instead,
    // from postProcessAfterInitialization.
    private Class<?> resolveScanType(String beanName) {
        Class<?> recorded = userClassByBeanName.get(beanName);
        if (recorded != null) {
            return recorded;
        }
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        if (beanFactory.containsSingleton(beanName) && !beanFactory.isFactoryBean(beanName)) {
            return ClassUtils.getUserClass(SubscriptionAnnotations.ultimateTarget(applicationContext.getBean(beanName)).getClass());
        }
        Class<?> type = applicationContext.getType(beanName);
        return type == null ? null : ClassUtils.getUserClass(type);
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
