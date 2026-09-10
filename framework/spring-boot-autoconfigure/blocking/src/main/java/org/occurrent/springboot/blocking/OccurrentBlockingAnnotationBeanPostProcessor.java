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
import org.occurrent.subscription.DuplicateSubscriptionIdException;
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
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
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
    private final Set<String> registeredIds = ConcurrentHashMap.newKeySet();
    // The class the container actually built for a bean, recorded as the container hands the object over. This is
    // the only source that cannot fall short of the real class, see resolveScanType below for why the prediction
    // this replaces can.
    private final Map<String, Class<?>> userClassByBeanName = new ConcurrentHashMap<>();
    // Every handler already registered, so scanning a bean a second time registers only what is new. Concurrent
    // rather than lock-guarded, because a bean created after startup is created on whatever thread asked for it,
    // and claiming an id is an add that answers false when something else already holds it, which is the whole
    // check. Holding a lock across a late registration instead deadlocks, since that registration resolves
    // collaborators by type and a thread creating one of those enters this same callback, so it waits for the lock
    // the first thread holds while the first waits for the bean the second is building.
    //
    // What keeps the late path safe is therefore two things together, and both have to stay true. No lock is held
    // across a late registration, and every collection such a registration appends to is concurrent and drained by
    // polling rather than by iterating and then clearing, in the three registrars as well as here. No test
    // demonstrates either. The interleaving cannot be staged, because parking a thread inside a bean factory
    // serialises other singleton creation at the Spring level, so the two threads the hazard needs never overlap.
    // LateRegistrationConcurrencyContractTest asserts the types instead, which catches the way this realistically
    // regresses rather than the hazard itself.
    private final Set<String> registeredHandlers = ConcurrentHashMap.newKeySet();
    private final Object registrationLock = new Object();
    // A bean another thread finishes while the startup scan is still running is recorded here, because the scan
    // may already have passed its name and the callback that finished it may see startupScanComplete as false and
    // do nothing. The scan drains this once the flag is set. Only another thread's beans are recorded, since the
    // scan handles every bean it builds itself, which is what keeps this from collecting every bean in the context.
    private final Queue<String> builtWhileScanning = new ConcurrentLinkedQueue<>();
    private volatile Thread scanningThread;
    // Bean names this scan has built so a later pass can read their real class, so a build after which the class
    // is still unknown is not attempted for ever.
    private final Set<String> alreadyBuiltToBeScanned = ConcurrentHashMap.newKeySet();
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
            // put rather than putIfAbsent, because a creation that failed is retried and the retry can produce a
            // different class than the attempt that failed. The recording has to describe the instance the
            // container is building now, not the first one it tried.
            userClassByBeanName.put(beanName, userClassOf(bean));
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
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        // Only a FactoryBean's product is recorded here. Every other bean was recorded by the callback above, from
        // the instance before any proxy wrapped it, and overwriting that with what arrives here would record a
        // proxy class for a proxy ultimateTarget cannot unwrap. A product is what reaches this
        // callback without reaching that one, since the container passes the factory itself under this name there.
        //
        // containsBeanDefinition first, because not everything that reaches this callback is a bean. Spring's test
        // support initializes a test instance through it under a name it never defined, and isFactoryBean throws
        // NoSuchBeanDefinitionException for a name with no definition behind it.
        if (beanFactory.containsBeanDefinition(beanName) && beanFactory.isFactoryBean(beanName)) {
            userClassByBeanName.put(beanName, userClassOf(bean));
        }
        Thread scanning = scanningThread;
        if (!startupScanComplete && scanning != null && scanning != Thread.currentThread()) {
            builtWhileScanning.add(beanName);
        }
        // Read again rather than reused, so a bean finishing as the scan ends is registered by whichever of the two
        // sees the flag set. Registering it twice is not possible, since both go through the same handler keys.
        if (startupScanComplete && beanFactory.containsBeanDefinition(beanName)) {
            boolean singleton = beanFactory.isSingleton(beanName);
            // The class of the instance this callback received, never a second lookup by name. Two threads
            // building the same prototype share the recorded entry, so a lookup here can answer with the other
            // one's class and this bean would be scanned for methods its own class does not declare.
            Class<?> userClass = userClassOf(bean);
            scan(new String[]{beanName}, name -> userClass, name -> bean,
                    (name, resolved) -> () -> singleton && !beanFactory.isCurrentlyInCreation(name) ? applicationContext.getBean(name) : resolved, false);
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
            scanningThread = Thread.currentThread();
            String[] beanNames = applicationContext.getBeanDefinitionNames();
            while (scan(beanNames, this::resolveScanType, applicationContext::getBean, (name, resolved) -> () -> resolved, true)) {
                beanNames = applicationContext.getBeanDefinitionNames();
            }
            startupScanComplete = true;
            drainBeansBuiltWhileScanning();
            scanningThread = null;
        }
    }

    // A bean another thread finished during the scan may have been passed over, so each one is scanned again now
    // that the flag is set. A name still being created is left alone, because reaching it means waiting for the
    // thread creating it, which may be waiting for the lock this holds. That thread's own callback reads the flag
    // once more after recording the name here, so it registers the bean itself.
    private void drainBeansBuiltWhileScanning() {
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        String beanName;
        // Poll until empty, never iterate then clear, since an entry added between those two is dropped.
        while ((beanName = builtWhileScanning.poll()) != null) {
            if (beanFactory.containsBeanDefinition(beanName) && !beanFactory.isCurrentlyInCreation(beanName)) {
                scan(new String[]{beanName}, this::resolveScanType, applicationContext::getBean, (name, resolved) -> () -> resolved, false);
            }
        }
    }

    // beanResolver hands back the object to read a descriptor factory from and to check the invocation guards
    // against. handlerTargets turns that object into the one a handler is invoked on, per delivery. They differ
    // only for a bean created after the startup scan, where the object is in hand but its name cannot be resolved
    // until creation finishes. Answers whether anything registered, which is what the loop above repeats on.
    //
    // First collect every subscription id so a projection cannot reuse one and so the fencing check below can be
    // asked about each one, then register the subscriptions, then the projections.
    //
    // @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription methods register before the
    // fencing check below runs, so one can already write a checkpoint before the check inspects idsToCheck.
    // Pre-existing, not introduced by this reorder. CheckpointStorageCannotFenceSubscriptionException's javadoc
    // covers it.
    private boolean scan(String[] beanNames, Function<String, Class<?>> typeResolver, Function<String, Object> beanResolver, BiFunction<String, Object, Supplier<Object>> handlerTargets, boolean mayBlockForReplay) {
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
        // Beans whose class is only predicted and whose prediction shows an annotation. Built at the end of this
        // pass so the next one can read their real class.
        Set<String> beansToBuild = new LinkedHashSet<>();
        for (String beanName : beanNames) {
            Class<?> type;
            try {
                type = typeResolver.apply(beanName);
            } catch (RuntimeException e) {
                continue;
            }
            if (type == null) {
                continue;
            }
            // A predicted type is what the bean definition says, not what the container will build, so an
            // annotation read from it may be one the concrete class overrides with different settings. Nothing is
            // registered from it. Building the bean is what makes its class knowable, so a bean whose prediction
            // shows any annotation at all is built by this pass and collected by the next one, from its own class.
            if (!isConcreteScanType(beanName)) {
                if (declaresAnyOccurrentAnnotation(type) && !alreadyBuiltToBeScanned.contains(beanName)) {
                    beansToBuild.add(beanName);
                }
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
        boolean registeredAnything = !subscriptionBeanNames.isEmpty() || !projectionMethods.isEmpty()
                || !snapshotMethods.isEmpty() || !sagaMethods.isEmpty() || !beansToBuild.isEmpty();
        // Only a handler the loop above collected registers, which is what keeps every id going through
        // claimSubscriptionId. The register step reads the bean's class again, and by then the bean exists, so that
        // class can declare a handler the collecting pass never saw. Registering it here would take its id without
        // checking it. It is left for the next pass instead, which collects it from the now recorded class.
        //
        // Marking happens after the registration it stands for, never before. A registration that throws while a
        // bean is being created fails that bean's creation, and Spring caches nothing for a creation that failed,
        // so asking for the bean again creates it again and runs this callback again. A handler marked ahead of
        // its own registration would be skipped on that second attempt, and the bean would then be published with
        // a handler that never registered, which is the loss this whole class exists to close.
        // A bean built after startup is checked before anything of its is activated. Failing after a subscription
        // is live keeps it running against a bean whose creation then fails, and the context stays up, so the
        // check has to come first here. The startup path keeps the order it had, where subscriptions register
        // ahead of the check, which CheckpointStorageCannotFenceSubscriptionException's javadoc describes and
        // ADR 127 records as open work rather than something this change settles.
        if (!mayBlockForReplay) {
            CheckpointFencingConfigurationCheck.check(applicationContext, idsToCheck);
        }
        for (String beanName : subscriptionBeanNames) {
            Object bean = beanResolver.apply(beanName);
            subscriptionRegistrar.registerSubscriptions(bean, typeResolver.apply(beanName), handlerTargets.apply(beanName, bean), mayBlockForReplay,
                    method -> markRegistered(beanName, method),
                    this::claimSubscriptionId,
                    method -> registeredHandlers.remove(handlerKey(beanName, method)),
                    registeredIds::remove);
        }
        if (mayBlockForReplay) {
            CheckpointFencingConfigurationCheck.check(applicationContext, idsToCheck);
        }
        for (Object[] pm : projectionMethods) {
            if (markRegistered((String) pm[0], (Method) pm[1])) {
                registerDescriptor((String) pm[0], (Method) pm[1], ((org.occurrent.annotation.Projection) pm[2]).id(),
                        () -> projectionRegistrar.processProjectionAnnotation(beanResolver.apply((String) pm[0]), (Method) pm[1], (org.occurrent.annotation.Projection) pm[2]));
            }
        }
        // Catch up each domain-push feed once, after all its projections are registered.
        projectionRegistrar.catchUpCollectedFeeds();
        for (Object[] sm : snapshotMethods) {
            if (markRegistered((String) sm[0], (Method) sm[1])) {
                registerDescriptor((String) sm[0], (Method) sm[1], ((org.occurrent.annotation.Snapshot) sm[2]).id(),
                        () -> snapshotRegistrar.processSnapshotAnnotation(beanResolver.apply((String) sm[0]), (Method) sm[1], (org.occurrent.annotation.Snapshot) sm[2]));
            }
        }
        for (Object[] gm : sagaMethods) {
            if (markRegistered((String) gm[0], (Method) gm[1])) {
                registerDescriptor((String) gm[0], (Method) gm[1], ((org.occurrent.annotation.Saga) gm[2]).id(),
                        () -> sagaRegistrar.processSagaAnnotation(beanResolver.apply((String) gm[0]), (Method) gm[1], (org.occurrent.annotation.Saga) gm[2]));
            }
        }
        for (String beanName : beansToBuild) {
            alreadyBuiltToBeScanned.add(beanName);
            beanResolver.apply(beanName);
        }
        return registeredAnything;
    }

    // Concrete when the container has handed the object over and its class was recorded, and when the bean is
    // already a singleton whose own class resolveScanType reads directly. Everything else is the bean definition's
    // prediction, which cannot be registered from.
    private boolean isConcreteScanType(String beanName) {
        if (userClassByBeanName.containsKey(beanName)) {
            return true;
        }
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        return beanFactory.containsSingleton(beanName) && !beanFactory.isFactoryBean(beanName);
    }

    private static boolean declaresAnyOccurrentAnnotation(Class<?> type) {
        for (Method method : type.getDeclaredMethods()) {
            if (AnnotationUtils.findAnnotation(method, StreamSubscription.class) != null
                    || AnnotationUtils.findAnnotation(method, Subscription.class) != null
                    || AnnotationUtils.findAnnotation(method, DcbSubscription.class) != null
                    || AnnotationUtils.findAnnotation(method, SynchronousSubscription.class) != null
                    || AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Projection.class) != null
                    || AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Snapshot.class) != null
                    || AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Saga.class) != null) {
                return true;
            }
        }
        return false;
    }


    // The descriptor registrars claim their own id inside registeredIds before they do the rest of their work, so a
    // failure part way through would otherwise leave the id claimed by a registration that never happened, and the
    // bean's next creation attempt would be refused as a duplicate of itself. The claim is released here instead.
    // A DuplicateSubscriptionIdException is the one failure that must not release, because the id it names belongs
    // to whoever claimed it first and releasing would hand it away.
    private void registerDescriptor(String beanName, Method method, String id, Runnable registration) {
        try {
            registration.run();
        } catch (DuplicateSubscriptionIdException e) {
            registeredHandlers.remove(handlerKey(beanName, method));
            throw e;
        } catch (RuntimeException | Error e) {
            registeredIds.remove(id);
            registeredHandlers.remove(handlerKey(beanName, method));
            throw e;
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

    // Only the three that reach CheckpointStorage go into idsToCheck. @SynchronousSubscription writes no checkpoint
    // at all, so its id is checked for duplicates only, never asked about by the fencing check. A bean with any
    // of the four annotations goes into subscriptionBeanNames so registerSubscriptions runs for it exactly once.
    //
    // The id is refused here when something else already holds it, and joins registeredIds only once its own
    // registration has succeeded. Refusing here is what the startup ordering used to give for free, since every
    // subscription id was collected before the first projection, snapshot or saga checked one out. A subscription
    // registering after startup arrives long after all of those, so without this check it would take an id one of
    // them already holds and write to the same durable checkpoint key.
    private void collectSubscriptionId(String beanName, Method method, Set<String> idsToCheck, Set<String> subscriptionBeanNames) {
        StreamSubscription s = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
        if (s != null) {
            idsToCheck.add(s.id());
            subscriptionBeanNames.add(beanName);
        }
        Subscription a = AnnotationUtils.findAnnotation(method, Subscription.class);
        if (a != null) {
            idsToCheck.add(a.id());
            subscriptionBeanNames.add(beanName);
        }
        DcbSubscription d = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
        if (d != null) {
            idsToCheck.add(d.id());
            subscriptionBeanNames.add(beanName);
        }
        SynchronousSubscription sy = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
        if (sy != null) {
            subscriptionBeanNames.add(beanName);
        }
    }

    // Claiming writes to registeredIds itself rather than to anything held aside for the length of a scan, so a
    // nested scan, which happens when registering a handler builds another annotated bean, meets a claim that is
    // already there. The claim is released when the work it stands for throws.
    private void claimSubscriptionId(String id) {
        if (!registeredIds.add(id)) {
            throw new DuplicateSubscriptionIdException(id, "Duplicate subscription/projection id '%s', each id must be unique because it is the durable checkpoint key.".formatted(id));
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
