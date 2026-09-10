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
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.api.reactor.Subscribable;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

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

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        StartPositionSupport startPositionSupport = new StartPositionSupport(applicationContext);
        this.subscriptionRegistrar = new SubscriptionAnnotationRegistrar(applicationContext, startPositionSupport);
        this.projectionRegistrar = new ProjectionAnnotationRegistrar(applicationContext, registeredIds, startPositionSupport);
        this.snapshotRegistrar = new SnapshotAnnotationRegistrar(applicationContext, registeredIds, startPositionSupport);
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
    // The handler target is a supplier rather than an instance, because which object to invoke on depends on when
    // the delivery happens. Asking the context by name is what reaches the published singleton, whatever a
    // BeanPostProcessor later in the chain wrapped it in, but the singleton is not published until this callback
    // returns and asking for it before then throws BeanCurrentlyInCreationException. A subscription registered
    // here with startupMode = WAIT_UNTIL_STARTED replays its history inside this very callback, so that is not a
    // theoretical window. Until the bean has finished being created the handler runs on the instance this callback
    // received, which is already past every ordered BeanPostProcessor and so already has its AOP advice applied,
    // and after that
    // on the published one. A bean of any other scope is never published under its name at all, so it stays on the
    // instance this callback received, the way the startup scan binds a prototype's handler to the single instance
    // it asked for.
    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        if (bean instanceof FactoryBean<?>) {
            return bean;
        }
        userClassByBeanName.putIfAbsent(beanName, userClassOf(bean));
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        if (startupScanComplete && beanFactory.containsBeanDefinition(beanName)) {
            boolean singleton = beanFactory.isSingleton(beanName);
            synchronized (registrationLock) {
                scan(new String[]{beanName}, name -> bean,
                        (name, resolved) -> () -> singleton && !beanFactory.isCurrentlyInCreation(name) ? applicationContext.getBean(name) : resolved);
            }
        }
        return bean;
    }

    private static Class<?> userClassOf(Object bean) {
        return ClassUtils.getUserClass(SubscriptionAnnotations.ultimateTarget(bean).getClass());
    }

    // @Projection and @Snapshot factory methods, and @Subscription, @StreamSubscription, @DcbSubscription and
    // @SynchronousSubscription handler methods, register after all singletons are instantiated: the factory has to
    // be invoked to obtain the descriptor, and its collaborators (the store, the subscription model) must already be
    // wired. Every handler resolves its invocation through applicationContext.getBean(beanName), which by this point
    // always returns the fully proxied singleton, so advice such as @Transactional applies to every delivery,
    // including a WAIT_UNTIL_STARTED history replay, not just the ones after startup. First collect every
    // subscription id so a projection or snapshot cannot reuse one, then register the subscriptions, then each
    // projection, catch up domain-push feeds, then register each snapshot.
    //
    // The scan repeats until a pass registers nothing. Registering a handler builds its bean, and building it
    // records that bean's real class, so a later pass sees an annotation an earlier pass's predicted type did not
    // declare. Registering also builds whatever collaborators the handler asks for, and those beans are recorded
    // after the pass that collected, so one extra pass is not enough. Everything already registered is skipped by
    // handler key, so each pass registers only what the ones before it could not see. It terminates because a pass
    // that registers nothing has built nothing, so nothing new was recorded for the next pass to find, and the
    // handlers a context declares are finite.
    @Override
    public void afterSingletonsInstantiated() {
        synchronized (registrationLock) {
            String[] beanNames = applicationContext.getBeanDefinitionNames();
            while (scan(beanNames, applicationContext::getBean, (name, resolved) -> () -> resolved)) {
                beanNames = applicationContext.getBeanDefinitionNames();
            }
            startupScanComplete = true;
        }
    }

    // beanResolver hands back the object to read a descriptor factory from and to check the invocation guards
    // against. handlerTargets turns that object into the one a handler is invoked on, per delivery. They differ
    // only for a bean created after the startup scan, where the object is in hand but its name cannot be resolved
    // until creation finishes. Answers whether anything registered, which is what the loop above repeats on.
    private boolean scan(String[] beanNames, Function<String, Object> beanResolver, BiFunction<String, Object, Supplier<Object>> handlerTargets) {
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
        // Held aside until each id's own registration succeeds, see claimSubscriptionId.
        Map<String, String> pendingSubscriptionIds = new LinkedHashMap<>();
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
                collectSubscriptionId(beanName, method, subscriptionBeanNames, pendingSubscriptionIds);
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
        boolean registeredAnything = !subscriptionBeanNames.isEmpty() || !projectionMethods.isEmpty() || !snapshotMethods.isEmpty();
        // Marking happens after the registration it stands for, never before. A registration that throws while a
        // bean is being created fails that bean's creation, and Spring caches nothing for a creation that failed,
        // so asking for the bean again creates it again and runs this callback again. A handler marked ahead of
        // its own registration would be skipped on that second attempt, and the bean would then be published with
        // a handler that never registered, which is the loss this whole class exists to close.
        for (String beanName : subscriptionBeanNames) {
            Object bean = beanResolver.apply(beanName);
            subscriptionRegistrar.registerSubscriptions(bean, resolveScanType(beanName), handlerTargets.apply(beanName, bean),
                    method -> !isAlreadyRegistered(beanName, method),
                    method -> {
                        markRegistered(beanName, method);
                        registeredIds.add(pendingSubscriptionIds.get(handlerKey(beanName, method)));
                    });
        }
        if (!subscribableExists) {
            return registeredAnything;
        }
        for (Object[] pm : projectionMethods) {
            projectionRegistrar.processProjectionAnnotation(beanResolver.apply((String) pm[0]), (Method) pm[1], (org.occurrent.annotation.Projection) pm[2]);
            markRegistered((String) pm[0], (Method) pm[1]);
        }
        // Catch up each domain-push feed once, after all its projections are registered.
        projectionRegistrar.catchUpCollectedFeeds();
        for (Object[] sm : snapshotMethods) {
            snapshotRegistrar.processSnapshotAnnotation(beanResolver.apply((String) sm[0]), (Method) sm[1], (org.occurrent.annotation.Snapshot) sm[2]);
            markRegistered((String) sm[0], (Method) sm[1]);
        }
        return registeredAnything;
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

    // A bean with any of the four annotations goes into subscriptionBeanNames so registerSubscriptions runs for
    // it exactly once, above.
    //
    // The id is refused here when something else already holds it, and joins registeredIds only once its own
    // registration has succeeded. Refusing here is what the startup ordering used to give for free, since every
    // subscription id was collected before the first projection or snapshot checked one out. A subscription
    // registering after startup arrives long after all of those, so without this check it would take an id one of
    // them already holds and write to the same durable checkpoint key.
    private void collectSubscriptionId(String beanName, Method method, Set<String> subscriptionBeanNames, Map<String, String> pendingIds) {
        StreamSubscription s = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
        if (s != null) {
            claimSubscriptionId(beanName, method, s.id(), "@StreamSubscription", pendingIds);
            subscriptionBeanNames.add(beanName);
        }
        Subscription a = AnnotationUtils.findAnnotation(method, Subscription.class);
        if (a != null) {
            claimSubscriptionId(beanName, method, a.id(), "@Subscription", pendingIds);
            subscriptionBeanNames.add(beanName);
        }
        DcbSubscription d = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
        if (d != null) {
            claimSubscriptionId(beanName, method, d.id(), "@DcbSubscription", pendingIds);
            subscriptionBeanNames.add(beanName);
        }
        SynchronousSubscription sy = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
        if (sy != null) {
            claimSubscriptionId(beanName, method, sy.id(), "@SynchronousSubscription", pendingIds);
            subscriptionBeanNames.add(beanName);
        }
    }

    private void claimSubscriptionId(String beanName, Method method, String id, String annotationName, Map<String, String> pendingIds) {
        if (registeredIds.contains(id) || pendingIds.containsValue(id)) {
            throw new DuplicateSubscriptionIdException(id, "Duplicate subscription/projection id '%s' (used by %s on %s#%s), each id must be unique because it is the durable checkpoint key.".formatted(
                    id, annotationName, userClassByBeanName.getOrDefault(beanName, method.getDeclaringClass()).getName(), method.getName()));
        }
        pendingIds.put(handlerKey(beanName, method), id);
    }
}
