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
    //
    // The instance goes in with the name. The two flag reads in that callback are not ordered against the write
    // here, so a callback can read false and decline, and the drain must then register the bean itself rather than
    // leave it to a second read that has already happened. Holding the instance is what lets it, since a name
    // still in creation cannot be resolved but an instance in hand needs no resolving.
    private final Queue<BuiltWhileScanning> builtWhileScanning = new ConcurrentLinkedQueue<>();
    private volatile Thread scanningThread;
    // Bean names this scan has built so a later pass can read their real class, so a build after which the class
    // is still unknown is not attempted for ever.
    private final Set<String> alreadyBuiltToBeScanned = ConcurrentHashMap.newKeySet();
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
            builtWhileScanning.add(new BuiltWhileScanning(beanName, bean));
        }
        // Read again rather than reused, so a bean finishing as the scan ends is registered by whichever of the two
        // sees the flag set. Registering it twice is not possible, since both go through the same handler keys.
        if (startupScanComplete && beanFactory.containsBeanDefinition(beanName)) {
            boolean singleton = beanFactory.isSingleton(beanName);
            // The class of the instance this callback received, never a second lookup by name. Two threads
            // building the same prototype share the recorded entry, so a lookup here can answer with the other
            // one's class and this bean would be scanned for methods its own class does not declare.
            ScanType userClass = new ScanType(userClassOf(bean), true);
            scan(new String[]{beanName}, name -> userClass, name -> bean,
                    (name, resolved) -> () -> publishedBeanIsResolvable(beanFactory, name, singleton) ? applicationContext.getBean(name) : resolved, false);
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

    // Whether a handler invocation for this name can reach the object the container published, rather than the
    // instance captured on the way through. A registration may block for a replay only where this holds, since a
    // blocking replay delivers before creation has finished and would otherwise reach the captured instance.
    private static boolean publishedBeanIsResolvable(ConfigurableListableBeanFactory beanFactory, String beanName, boolean singleton) {
        return singleton && !beanFactory.isCurrentlyInCreation(beanName);
    }

    // A bean another thread finished during the scan may have been passed over, so each one is scanned again now
    // that the flag is set. Every entry is scanned, including one whose name is still in creation, because the
    // callback that recorded it may have read startupScanComplete as false and declined before the flag was set.
    // Dropping it on the expectation that the callback will come back to it loses the bean, since that read has
    // already happened and there is no third.
    //
    // The instance recorded with the name is what makes this safe. Resolving the name would mean waiting for the
    // thread creating it, which may be waiting for the lock this holds, so nothing here resolves anything. Both
    // sides can reach the same bean, and the handler keys they both go through admit only the first.
    //
    // This runs on the startup thread inside afterSingletonsInstantiated, so a bean whose creating thread has
    // finished with it registers under the startup policy and keeps the WAIT_UNTIL_STARTED guarantee it would have
    // had a moment earlier. A bean still in creation does not, because a blocking replay delivers while this call
    // is on the stack, and what it would reach is the instance captured before the post-processors after this one
    // ran. That is the split #965 removed, where a replay ran on the raw bean and everything after it on the proxy.
    private void drainBeansBuiltWhileScanning() {
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        BuiltWhileScanning built;
        // Poll until empty, never iterate then clear, since an entry added between those two is dropped.
        while ((built = builtWhileScanning.poll()) != null) {
            String beanName = built.beanName();
            Object bean = built.bean();
            if (!beanFactory.containsBeanDefinition(beanName)) {
                continue;
            }
            boolean singleton = beanFactory.isSingleton(beanName);
            ScanType userClass = new ScanType(userClassOf(bean), true);
            // One condition answers both, so a blocking replay cannot deliver anywhere the supplier would not have
            // sent it. Asked once here for the whole registration, and again per delivery below, since creation
            // finishing between the two only turns a replay this declined to block for into deliveries that do
            // resolve the published bean.
            scan(new String[]{beanName}, name -> userClass, name -> bean,
                    (name, resolved) -> () -> publishedBeanIsResolvable(beanFactory, name, singleton) ? applicationContext.getBean(name) : resolved,
                    publishedBeanIsResolvable(beanFactory, beanName, singleton));
        }
    }

    // beanResolver hands back the object to read a descriptor factory from and to check the invocation guards
    // against. handlerTargets turns that object into the one a handler is invoked on, per delivery. They differ
    // only for a bean created after the startup scan, where the object is in hand but its name cannot be resolved
    // until creation finishes. Answers whether anything registered, which is what the loop above repeats on.
    private boolean scan(String[] beanNames, Function<String, ScanType> typeResolver, Function<String, Object> beanResolver, BiFunction<String, Object, Supplier<Object>> handlerTargets, boolean mayBlockForReplay) {
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
        // Beans whose class is only predicted and whose prediction shows an annotation. Built at the end of this
        // pass so the next one can read their real class.
        Set<String> beansToBuild = new LinkedHashSet<>();
        for (String beanName : beanNames) {
            ScanType scanType;
            try {
                scanType = typeResolver.apply(beanName);
            } catch (RuntimeException e) {
                continue;
            }
            if (scanType == null) {
                continue;
            }
            Class<?> type = scanType.type();
            // A predicted type is what the bean definition says, not what the container will build, so an
            // annotation read from it may be one the concrete class overrides with different settings. Nothing is
            // registered from it. Building the bean is what makes its class knowable, so a bean whose prediction
            // shows any annotation at all is built by this pass and collected by the next one, from its own class.
            if (!scanType.concrete()) {
                if (declaresAnyOccurrentAnnotation(type, subscribableExists) && !alreadyBuiltToBeScanned.contains(beanName)) {
                    beansToBuild.add(beanName);
                }
                continue;
            }
            for (Method method : type.getDeclaredMethods()) {
                if (isAlreadyRegistered(beanName, method)) {
                    continue;
                }
                collectSubscriptionId(beanName, method, subscriptionBeanNames);
                if (!subscribableExists) {
                    continue;
                }
                refuseMoreThanOneHandlerAnnotation(type, method);
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
        boolean registeredAnything = !subscriptionBeanNames.isEmpty() || !projectionMethods.isEmpty()
                || !snapshotMethods.isEmpty() || !beansToBuild.isEmpty();
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
        for (String beanName : subscriptionBeanNames) {
            Object bean = beanResolver.apply(beanName);
            // The class of the instance just resolved, not another lookup by name. A non-singleton bean is a new
            // instance here, and a factory that can return different implementations would otherwise have this
            // register one implementation's methods against another's instance.
            subscriptionRegistrar.registerSubscriptions(bean, userClassOf(bean), handlerTargets.apply(beanName, bean), mayBlockForReplay,
                    method -> markRegistered(beanName, method),
                    this::claimSubscriptionId,
                    method -> registeredHandlers.remove(handlerKey(beanName, method)),
                    registeredIds::remove);
        }
        // No early return here. The build step at the end of this method is what lets the loop above finish, so a
        // pass that skips registering must still reach it.
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
        for (String beanName : beansToBuild) {
            alreadyBuiltToBeScanned.add(beanName);
            beanResolver.apply(beanName);
        }
        return registeredAnything;
    }


    // Only what this pass could register counts, so a bean is never built for an annotation the pass would skip
    // anyway. @Projection and @Snapshot need a Subscribable bean to register against, the same condition the
    // collecting loop applies to them.
    private static boolean declaresAnyOccurrentAnnotation(Class<?> type, boolean subscribableExists) {
        for (Method method : type.getDeclaredMethods()) {
            if (AnnotationUtils.findAnnotation(method, StreamSubscription.class) != null
                    || AnnotationUtils.findAnnotation(method, Subscription.class) != null
                    || AnnotationUtils.findAnnotation(method, DcbSubscription.class) != null
                    || AnnotationUtils.findAnnotation(method, SynchronousSubscription.class) != null) {
                return true;
            }
            if (subscribableExists && (AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Projection.class) != null
                    || AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Snapshot.class) != null)) {
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
        // Whether this attempt is the one holding the id, read before it runs, rather than inferred afterwards from
        // the exception. A descriptor registrar adds the id itself and then calls the subscription model, which can
        // throw DuplicateSubscriptionIdException for a programmatic subscription already using that id, so the
        // exception type says nothing about whose claim this is. Releasing on it regardless would hand away an id
        // another registration owns, and never releasing would keep a claim this attempt made and leave the bean
        // refused for ever once the real duplicate is gone.
        boolean heldByAnother = registeredIds.contains(id);
        try {
            registration.run();
        } catch (RuntimeException | Error e) {
            if (!heldByAnother) {
                registeredIds.remove(id);
            }
            registeredHandlers.remove(handlerKey(beanName, method));
            throw e;
        }
    }

    // One method declares at most one handler annotation. The descriptor ones register through different registrars
    // and return different descriptor types, so a method with two of them was always a mistake, and it used to be
    // caught by the second registrar rejecting the return type. A subscription annotation shares nothing with a
    // descriptor one except the key both register under, and that key spans the two families, so a method carrying
    // one of each used to be caught the same way. Registration is keyed by the method now, so the second one would
    // be skipped in silence instead. Refused here rather than dropped.
    //
    // Two subscription annotations are left to the registrar, which names them in a message of its own.
    private static void refuseMoreThanOneHandlerAnnotation(Class<?> userClass, Method method) {
        List<String> declared = new ArrayList<>();
        if (AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Projection.class) != null) {
            declared.add("@Projection");
        }
        if (AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Snapshot.class) != null) {
            declared.add("@Snapshot");
        }
        if (declared.size() > 1) {
            throw new IllegalArgumentException("Method %s#%s is annotated with more than one of @Projection and @Snapshot, use only one.".formatted(userClass.getName(), method.getName()));
        }
        if (declared.isEmpty()) {
            return;
        }
        List<String> subscriptions = new ArrayList<>();
        if (AnnotationUtils.findAnnotation(method, Subscription.class) != null) {
            subscriptions.add("@Subscription");
        }
        if (AnnotationUtils.findAnnotation(method, StreamSubscription.class) != null) {
            subscriptions.add("@StreamSubscription");
        }
        if (AnnotationUtils.findAnnotation(method, DcbSubscription.class) != null) {
            subscriptions.add("@DcbSubscription");
        }
        if (AnnotationUtils.findAnnotation(method, SynchronousSubscription.class) != null) {
            subscriptions.add("@SynchronousSubscription");
        }
        if (!subscriptions.isEmpty()) {
            throw new IllegalArgumentException("Method %s#%s is annotated with %s and %s, which register as different things and cannot share a method, use only one.".formatted(
                    userClass.getName(), method.getName(), String.join(" and ", subscriptions), String.join(" and ", declared)));
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
    // The class and whether it is the one the container built come back together, from a single read of the
    // recording. Asking twice lets another thread record the class in between, so a prediction returned by the
    // first question is answered as concrete by the second, and an annotation the interface declares then registers
    // with settings the real class overrides.
    private ScanType resolveScanType(String beanName) {
        Class<?> recorded = userClassByBeanName.get(beanName);
        if (recorded != null) {
            return new ScanType(recorded, true);
        }
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        if (beanFactory.containsSingleton(beanName) && !beanFactory.isFactoryBean(beanName)) {
            return new ScanType(ClassUtils.getUserClass(SubscriptionAnnotations.ultimateTarget(applicationContext.getBean(beanName)).getClass()), true);
        }
        Class<?> type = applicationContext.getType(beanName);
        return type == null ? null : new ScanType(ClassUtils.getUserClass(type), false);
    }

    // The name and the instance travel together, so the drain never has to resolve a name that is still
    // in creation.
    private record BuiltWhileScanning(String beanName, Object bean) {
    }

    private record ScanType(Class<?> type, boolean concrete) {
    }

    // A bean with any of the four annotations goes into subscriptionBeanNames so registerSubscriptions runs for
    // it exactly once, above.
    //
    // The id is refused here when something else already holds it, and joins registeredIds only once its own
    // registration has succeeded. Refusing here is what the startup ordering used to give for free, since every
    // subscription id was collected before the first projection or snapshot checked one out. A subscription
    // registering after startup arrives long after all of those, so without this check it would take an id one of
    // them already holds and write to the same durable checkpoint key.
    private void collectSubscriptionId(String beanName, Method method, Set<String> subscriptionBeanNames) {
        StreamSubscription s = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
        if (s != null) {
            subscriptionBeanNames.add(beanName);
        }
        Subscription a = AnnotationUtils.findAnnotation(method, Subscription.class);
        if (a != null) {
            subscriptionBeanNames.add(beanName);
        }
        DcbSubscription d = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
        if (d != null) {
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
}
