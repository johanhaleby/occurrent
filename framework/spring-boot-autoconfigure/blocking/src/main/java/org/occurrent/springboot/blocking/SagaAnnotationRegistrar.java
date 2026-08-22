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

import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.SagaInstancesRegistry;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.blocking.RedeliveryDetection;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.springboot.common.AsynchronousSubscribables;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.RegisteringSubscribable;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;

/**
 * Registers {@code @Saga} factory methods: subscribes the saga to its events, materializes per-instance state, dispatches
 * issued commands, and polls the store to fire timeouts. Invoked from the coordinator's
 * {@code afterSingletonsInstantiated}, after snapshots, sharing the one duplicate-id registry. Each created
 * {@link SagaSubscription} owns a timer poller, and a push saga that catches up owns a replay thread, both of which
 * {@link #close()} stops when the context is destroyed. Blocking-stack only.
 */
class SagaAnnotationRegistrar {

    private static final Logger log = LoggerFactory.getLogger(SagaAnnotationRegistrar.class);

    private final ApplicationContext applicationContext;
    private final StartPositionSupport startPositionSupport;
    private final Set<String> registeredIds;
    // Resolves the competing-consumer strategy lazily, on the first checkpoint write a catch-up-then-push saga makes,
    // so this registrar does not force the strategy bean into existence while singletons are still being instantiated
    // (ADR 116). Separate from resolveSagaCompetingConsumerStrategy below, which gates the saga timer poller and is
    // an unrelated, already-eager use of the same strategy type.
    private final CompetingConsumerCheckpointWriteVersionSource writeVersionSource;
    // Registered sagas own a timer poller each, stop them when the context is destroyed so no poller thread leaks.
    // Concurrent because a push saga withheld by manual mode is added when the application starts it, on whichever
    // thread that is, while close() may be reading the list.
    private final List<SagaSubscription> sagaSubscriptions = new CopyOnWriteArrayList<>();
    // Push catch-up models created here, kept so the context can stop their replay threads on the way down. Created
    // during registration on the refresh thread, whether or not manual mode withholds the saga itself, so a plain list
    // is enough where sagaSubscriptions needs a concurrent one.
    private final List<CatchupThenPushSubscriptionModel> pushModels = new ArrayList<>();

    SagaAnnotationRegistrar(ApplicationContext applicationContext, StartPositionSupport startPositionSupport, Set<String> registeredIds) {
        this.applicationContext = applicationContext;
        this.startPositionSupport = startPositionSupport;
        this.registeredIds = registeredIds;
        this.writeVersionSource = new CompetingConsumerCheckpointWriteVersionSource(applicationContext.getBeanProvider(CompetingConsumerStrategy.class),
                () -> CheckpointFencingConfigurationCheck.fenceCheckpoints(applicationContext.getBeanProvider(OccurrentProperties.class)));
    }

    // A @Saga factory returns a Saga descriptor: subscribe to its events, materialize per-instance state into a
    // SagaStateStore, dispatch the commands it issues through a CommandDispatcher, and poll the store to fire timeouts.
    // Registered after other subscriptions so a saga cannot reuse an id. Blocking-stack only.
    @SuppressWarnings("unchecked")
    <E, S, C> void processSagaAnnotation(Object bean, Method method, org.occurrent.annotation.Saga annotation) {
        String id = annotation.id();
        if (!registeredIds.add(id)) {
            throw new DuplicateSubscriptionIdException(id, "Duplicate subscription/projection/snapshot/saga id '%s' (used by @Saga on %s#%s), each id must be unique because it is the durable checkpoint key.".formatted(id, bean.getClass().getName(), method.getName()));
        }
        if (method.getParameterCount() != 0) {
            throw new IllegalArgumentException("@Saga factory method %s#%s must take no parameters and return a Saga.".formatted(bean.getClass().getName(), method.getName()));
        }
        if (annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT && annotation.startAtGlobalPosition() >= 0) {
            throw new IllegalArgumentException("Specify either startAt or startAtGlobalPosition for @Saga '%s', not both.".formatted(id));
        }

        Object descriptor = SubscriptionAnnotations.invokeDescriptorFactory("@Saga", bean, method);
        if (!(descriptor instanceof Saga<?, ?, ?>)) {
            throw new IllegalArgumentException("@Saga '%s' method %s#%s must return a Saga, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor.getClass().getName()));
        }
        Saga<E, S, C> saga = (Saga<E, S, C>) descriptor;

        boolean push = annotation.source() == org.occurrent.annotation.Source.PUSH;
        if (push) {
            rejectStartPositionAttributes(annotation, id);
        } else if (annotation.catchup() != org.occurrent.annotation.Catchup.FROM_EVENT_STORE) {
            // Ignoring it would be the expensive kind of silence: someone reaching for catchup=NONE means "don't read
            // the history", and an event-store saga left on its default start position reads all of it.
            throw new IllegalArgumentException("@Saga '%s' sets catchup, which only applies to source=PUSH, where it decides whether the saga replays the event store before going live. An event-store saga chooses its history with startAt instead (startAt = NOW to skip it).".formatted(id));
        } else if (annotation.redeliveryDetection() != org.occurrent.annotation.RedeliveryDetection.REQUIRED) {
            // The event store's own events always carry the metadata, so BEST_EFFORT would relax a check that never
            // fires. Accepting it would read as protection given up, which is not what happens.
            throw new IllegalArgumentException("@Saga '%s' sets redeliveryDetection, which only applies to source=PUSH. An event-store saga reads events that always carry a streamid with a streamversion, so it can always recognise a redelivery.".formatted(id));
        }

        CloudEventConverter<E> converter = applicationContext.getBean(CloudEventConverter.class);
        // Resolved through AsynchronousSubscribables rather than a bare getBean(Subscribable.class): that also
        // matches the register-only SynchronousSubscriptionModel, which is ambiguous the moment an application
        // supplies its own asynchronous model without marking it @Primary (see AsynchronousSubscribables, and #541).
        Subscribable subscribable = push ? pushFeed(annotation, id)
                : AsynchronousSubscribables.resolve(applicationContext, Subscribable.class, RegisteringSubscribable.class);
        SagaStateStore<S> stateStore = resolveSagaStateStore(annotation, method, id);
        CommandDispatcher<C> commandDispatcher = resolveCommandDispatcher(annotation, id);
        // A push model ignores StartAt, and a replay in front of it always starts at the beginning, so there is no
        // start position to compute. rejectStartPositionAttributes has already refused the four that would imply one.
        StartAt startAt = push ? null : startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        SagaRunnerConfig config = SagaRunnerConfig.defaults()
                .withTimerPollInterval(sagaTimerPollInterval())
                .withRedeliveryDetection(redeliveryDetectionOf(annotation));
        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        SagaRunner<E, C> configured = stream ? SagaRunner.stream(subscribable, converter) : SagaRunner.agnostic(subscribable, converter);
        CompetingConsumerStrategy competingConsumerStrategy = resolveSagaCompetingConsumerStrategy();
        // Effectively final, so a withheld push saga can close over it and run the same registration later.
        final SagaRunner<E, C> runner = competingConsumerStrategy == null ? configured : configured.competingConsumerStrategy(competingConsumerStrategy);

        // A saga replaying its history from the beginning defaults to starting in the background, exactly as
        // @Subscription and @Projection do. startupMode was accepted and ignored here until now, so a saga always
        // held up startup whatever it asked for. A push saga replays only when it catches up, and then always from the
        // beginning, so it takes the push decision instead: only an explicit BACKGROUND moves it off the startup path.
        boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        boolean startsOnItsOwn = SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext);
        boolean waitUntilStarted = push
                ? catchesUp(annotation) && SubscriptionAnnotations.pushCatchUpShouldWaitUntilStarted(annotation.startupMode())
                : startsOnItsOwn && SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode());

        if (!push) {
            // The eager-creation workaround is about the framework's own SubscriptionModel template. A push feed is a
            // bean the application supplies and has already created by the time this runs.
            startPositionSupport.applyStartupWorkarounds();
        }

        if (push && !startsOnItsOwn) {
            // A push feed bypasses the SubscriptionModel bean entirely, so occurrent.subscription.mode=manual never
            // reaches it and the saga would start issuing commands at boot after being told to wait. Defer the whole
            // registration instead, exactly as a push projection does, and run it when the application starts this id.
            //
            // The observation view is published now rather than with the deferred run, because it reads the state store
            // and needs no subscription. Withholding it would leave an application that is deciding whether to start
            // this saga unable to look at the instances it already has, which is the decision manual mode exists for.
            publishSagaInstances(id, SagaInstances.of(stateStore));
            applicationContext.getBean(ManualStartPushSources.class).register(id, () -> {
                SagaSubscription deferred = runner.run(id, saga, stateStore, commandDispatcher, startAt, config, timersEnabledFor(subscribable, id), waitUntilStarted);
                sagaSubscriptions.add(deferred);
                registerSagaSubscriptionSingleton(id, deferred);
                watchBackgroundCatchUpIfNobodyElseWill(annotation, id, deferred, waitUntilStarted);
            });
            return;
        }

        SagaSubscription sagaSubscription = runner.run(id, saga, stateStore, commandDispatcher, startAt, config, timersEnabledFor(subscribable, id), waitUntilStarted);
        sagaSubscriptions.add(sagaSubscription);
        registerSagaSubscriptionSingleton(id, sagaSubscription);
        if (push) {
            watchBackgroundCatchUpIfNobodyElseWill(annotation, id, sagaSubscription, waitUntilStarted);
        }
        publishSagaInstances(id, sagaSubscription.instances());
    }

    // Under startupMode = BACKGROUND nobody joins the replay, so a failure would reach no one. Worse, the model forgets
    // a replay that failed while keeping the registration that now refuses events, so isCatchingUp answers false and
    // the status would report a dead saga as live. Join it on a thread of this registrar's own purely to record it,
    // the same watch the push projection registrar runs.
    private void watchBackgroundCatchUpIfNobodyElseWill(org.occurrent.annotation.Saga annotation, String id, SagaSubscription subscription, boolean waitUntilStarted) {
        if (waitUntilStarted || !catchesUp(annotation)) {
            return;
        }
        Thread.ofVirtual().name("occurrent-saga-catchup-watch-" + id).start(() -> {
            try {
                subscription.waitUntilStarted();
            } catch (RuntimeException | Error e) {
                log.error("The background catch-up of saga {} failed. It has replayed no history and refuses every "
                        + "live event, so the source redelivers rather than losing them. Fix the cause, then cancel "
                        + "the subscription and start it again.", id, e);
                withPushCatchupStatus(status -> status.recordFailure(id, e));
            }
        });
    }

    private static boolean catchesUp(org.occurrent.annotation.Saga annotation) {
        return annotation.catchup() == org.occurrent.annotation.Catchup.FROM_EVENT_STORE;
    }

    // A saga must not issue commands while its own event subscription is not running, which is what
    // occurrent.subscription.mode=manual leaves it as until the application resumes it, and what a saga starting in
    // the background is until its replay hands over. A model with no life cycle cannot answer, so those keep firing
    // timers as before.
    static BooleanSupplier timersEnabledFor(Subscribable subscribable, String subscriptionId) {
        if (!(subscribable instanceof SubscriptionModelLifeCycle lifeCycle)) {
            return () -> true;
        }
        // isRunning(id) is true throughout a replay, so it cannot stand in for the handover here. A timeout that fires
        // mid-replay decides against state that is only half folded up, which is the one thing a saga catching up
        // before it goes live is meant to avoid. Asked through the capability rather than a concrete class, so an
        // event-store catch-up model behind a durable wrapper is held to it too, not only the push one.
        return ReplayAwareSubscriptions.findIn(subscribable)
                .<BooleanSupplier>map(replayAware -> () -> lifeCycle.isRunning(subscriptionId) && !replayAware.isCatchingUp(subscriptionId))
                .orElse(() -> lifeCycle.isRunning(subscriptionId));
    }

    /**
     * A push saga takes no start position, whether or not it catches up: a replay always starts at the beginning, and
     * where the live feed resumes after a restart is the broker's business. {@code startAt},
     * {@code startAtGlobalPosition} and {@code resumeBehavior} would all do nothing, and rejecting them says so instead
     * of letting the caller believe otherwise. {@code @Projection(source = PUSH)} rejects the same three, plus the
     * synchronous mode, which {@code @Saga} does not have.
     * <p>
     * {@code startupMode} is the exception, and only under the default {@code catchup}: that replay is real work on the
     * startup path, so {@code BACKGROUND} has something to move off it. With {@code catchup = NONE} there is no replay
     * to wait for, so setting it is rejected rather than ignored.
     */
    /**
     * The declarative choice translated for the runner. The two enums are deliberately separate types, because the
     * annotation module is a leaf that the saga DSL does not depend on, exactly as {@code Catchup} and
     * {@code StartupMode} are declarative-only and translated here.
     */
    private static RedeliveryDetection redeliveryDetectionOf(org.occurrent.annotation.Saga annotation) {
        return switch (annotation.redeliveryDetection()) {
            case REQUIRED -> RedeliveryDetection.REQUIRED;
            case BEST_EFFORT -> RedeliveryDetection.BEST_EFFORT;
        };
    }

    private void rejectStartPositionAttributes(org.occurrent.annotation.Saga annotation, String id) {
        if (annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT || annotation.startAtGlobalPosition() >= 0
                || annotation.resumeBehavior() != org.occurrent.annotation.ResumeBehavior.DEFAULT) {
            // The startupMode hint only makes sense under the default catchup. With catchup=NONE there is no replay to
            // move off the startup path, and startupMode is rejected there anyway.
            String reason = catchesUp(annotation)
                    ? "It catches up before going live, but always from the beginning, so there is no start position to choose. Use startupMode = BACKGROUND to keep that replay off the startup path"
                    : "With catchup=NONE it takes live events only, so there is no history to position into";
            throw new IllegalArgumentException("@Saga '%s' with source=PUSH cannot set startAt, startAtGlobalPosition or resumeBehavior. %s, and where the live feed resumes is the broker's business.".formatted(id, reason));
        }
        if (!catchesUp(annotation) && annotation.startupMode() != org.occurrent.annotation.StartupMode.DEFAULT) {
            throw new IllegalArgumentException("@Saga '%s' combines source=PUSH with catchup=NONE, so it replays nothing and there is no startup work for startupMode to decide about. Remove startupMode, or drop catchup=NONE if you meant the saga to catch up first.".formatted(id));
        }
    }

    /**
     * The resolved push feed, with a one-time catch-up in front of it unless {@code catchup = NONE}, so a saga that has
     * never run is folded up from the event store before it starts taking live events. With {@code catchup = NONE} the
     * feed is used bare and no event store is touched, which is the only thing that works when the events come from
     * another application's broker.
     * <p>
     * Only a {@code PushSubscriptionModel} is accepted, unlike {@code @Projection}, which also takes a
     * {@code DomainEventFeed}. A domain-event feed carries no stream metadata, and without it a saga cannot recognise a
     * redelivered event, so binding one would quietly cost the saga its redelivery protection.
     */
    private Subscribable pushFeed(org.occurrent.annotation.Saga annotation, String id) {
        Object feedBean = SubscriptionAnnotations.resolveFeedBean(applicationContext, "@Saga", annotation.subscriptionModel(),
                annotation.subscriptionModelName(), id, PushSubscriptionModel.class);
        if (!(feedBean instanceof PushSubscriptionModel pushModel)) {
            throw new IllegalArgumentException("@Saga '%s' with source=PUSH resolved a %s, which is not a PushSubscriptionModel.".formatted(id, feedBean.getClass().getName()));
        }
        if (annotation.catchup() == org.occurrent.annotation.Catchup.NONE) {
            // No history to replay, so it is live as soon as it is running. Asked rather than recorded because
            // occurrent.subscription.mode = manual withholds a push saga, and a recorded Live would tell a readiness
            // probe that a saga nobody has started yet is ready to serve.
            withPushCatchupStatus(status -> status.register(id, () -> false, () -> pushModel.isRunning(id)));
            return pushModel;
        }
        PositionOrderedReader reader = SubscriptionAnnotations.resolveCatchupBean(applicationContext, "@Saga", PositionOrderedReader.class, id);
        CheckpointStorage catchupMarker = SubscriptionAnnotations.resolveCatchupBean(applicationContext, "@Saga", CheckpointStorage.class, id);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, pushModel, catchupMarker,
                ProjectionAnnotationRegistrar.catchupThenLiveOptions(applicationContext.getBean(OccurrentProperties.class)), writeVersionSource);
        // Retained so close() can stop it. Its replay runs on its own thread, so a context that closes without stopping
        // it leaves that replay folding into a store that is closing with it, and a saga folding a replayed history is
        // one that issues commands while it does so.
        pushModels.add(model);
        // Asked rather than recorded, so a model that is stopped and started again, replaying a second time, reports
        // catching up again instead of staying at whatever it reached the first time.
        withPushCatchupStatus(status -> status.register(id, () -> model.isCatchingUp(id), () -> model.isRunning(id)));
        // Published so a CloudEvent-level broker bridge, wired in a separate starter module that never depends on
        // this one, can look this exact object up and gate its own consumption on model::isReadyForLiveDelivery.
        // See CatchupThenPushSubscriptionModelPublisher.
        CatchupThenPushSubscriptionModelPublisher.publish(applicationContext, id, pushModel, model, log);
        return model;
    }

    // getIfAvailable rather than getBean: the starter contributes this bean, but a context that wires the post
    // processor directly has no reason to.
    private void withPushCatchupStatus(Consumer<PushCatchupStatusImpl> action) {
        PushCatchupStatusImpl status = applicationContext.getBeanProvider(PushCatchupStatusImpl.class).getIfAvailable();
        if (status != null) {
            action.accept(status);
        }
    }

    /**
     * Make the saga's {@link SagaInstances} reachable from the application, two ways: added to the
     * {@link SagaInstancesRegistry} (typed, injectable, keyed by saga id) and published as a singleton named
     * {@code sagaInstances-<id>}, for a {@code getBean} or {@code @Qualifier} lookup when the id is already known.
     * <p>
     * The two are independent on purpose. The registry is a bean defined during refresh, so it is populated whatever
     * kind of context this is, while the singleton needs a {@link ConfigurableApplicationContext}. If that registration
     * cannot happen, the registry still works.
     */
    private void publishSagaInstances(String id, SagaInstances instances) {
        addToSagaInstancesRegistry(id, instances);
        registerSagaInstancesSingleton(id, instances);
    }

    // Resolved by its concrete type rather than the interface, because only the implementation can be written to: the
    // public interface is read-only by design.
    private void addToSagaInstancesRegistry(String id, SagaInstances instances) {
        SagaInstancesRegistryImpl registry = applicationContext.getBeanProvider(SagaInstancesRegistryImpl.class).getIfAvailable();
        if (registry != null) {
            registry.register(id, instances);
            return;
        }
        if (applicationContext.getBeanNamesForType(SagaInstancesRegistry.class).length > 0) {
            // A SagaInstancesRegistry exists that Occurrent cannot write to, which can only mean the application
            // replaced the auto-configured one. That bean would stay empty for the lifetime of the context, so every
            // lookup through it would report no sagas at all. Fail at startup rather than serve an observation API that
            // silently answers "nothing is running".
            throw new IllegalStateException("A SagaInstancesRegistry bean is defined that Occurrent cannot populate, so it would stay empty forever and report no sagas. The registry is read-only for applications and is auto-configured; remove your own bean and inject SagaInstancesRegistry instead.");
        }
        // Unreachable through the wiring this library ships: the registry bean and the post-processor that runs this code
        // are gated on the same occurrent.subscription.mode property, so whenever a saga is registered at all, either
        // the auto-configured registry or a user-supplied one exists (and the branch above rejects the latter). Kept
        // rather than asserted, so the two conditions drifting apart in future degrades to a warning instead of an NPE,
        // and because a hand-built harness can reach it. Must not fail a saga that is otherwise running fine.
        log.warn("No SagaInstancesRegistry bean is available, so saga '{}' is not in one. Look it up as '{}' or use SagaSubscription.instances() instead.", id, sagaInstancesBeanName(id));
    }

    /**
     * Publish the saga's {@link SagaInstances} under its own bean name. This registers a singleton rather than a bean
     * definition because a {@code @Saga} factory can only run once its collaborators are wired, which is after the
     * context has refreshed. This particular bean is therefore not available for constructor injection into another
     * singleton, inject an {@code ObjectProvider<SagaInstances>}, look it up with
     * {@code getBean(name, SagaInstances.class)}, or inject the {@link SagaInstancesRegistry}, which does exist during
     * refresh. Registering per id rather than one bean of the type keeps two sagas from making a by-type injection
     * ambiguous.
     */
    private void registerSagaInstancesSingleton(String id, SagaInstances instances) {
        String beanName = sagaInstancesBeanName(id);
        if (!(applicationContext instanceof ConfigurableApplicationContext configurableContext)) {
            // Every Spring Boot context is configurable, so this is only reachable from an exotic harness. The saga
            // itself is running fine and both the registry and SagaSubscription.instances() still work, so this must
            // not fail startup.
            log.warn("Cannot publish '{}' because the application context is not a ConfigurableApplicationContext; use the SagaInstancesRegistry or SagaSubscription.instances() instead.", beanName);
            return;
        }
        ConfigurableListableBeanFactory beanFactory = configurableContext.getBeanFactory();
        if (beanFactory.containsBean(beanName)) {
            // registerSingleton would throw from inside afterSingletonsInstantiated, which fails startup with a message
            // that says nothing about sagas. The name is documented API, so a collision means two different things claim
            // it; say which saga and which name rather than letting Spring report a bare duplicate-singleton error.
            throw new IllegalStateException("Cannot publish the SagaInstances of saga '%s' as '%s' because a bean with that name already exists. Occurrent publishes each @Saga's SagaInstances under 'sagaInstances-<id>', so rename your bean or the saga.".formatted(id, beanName));
        }
        beanFactory.registerSingleton(beanName, instances);
    }

    /**
     * Publish the running {@link SagaSubscription} under {@code sagaSubscription-<id>}, next to the read-only
     * {@code sagaInstances-<id>} above and registered the same way, as a singleton after refresh rather than as a bean
     * definition. It is here because releasing a quarantined instance is the one saga operation that is not on
     * {@link SagaInstances}, and without this bean an application on the annotation path could see a quarantined
     * instance without being able to bring it back.
     * <p>
     * A saga in manual mode gets this bean when the application starts that saga, not at refresh, since there is no
     * subscription to publish before then.
     */
    private void registerSagaSubscriptionSingleton(String id, SagaSubscription sagaSubscription) {
        String beanName = sagaSubscriptionBeanName(id);
        if (!(applicationContext instanceof ConfigurableApplicationContext configurableContext)) {
            log.warn("Cannot publish '{}' because the application context is not a ConfigurableApplicationContext; the saga runs fine, but SagaSubscription.release(sagaId) is only reachable from a context that can hold the bean.", beanName);
            return;
        }
        ConfigurableListableBeanFactory beanFactory = configurableContext.getBeanFactory();
        if (beanFactory.containsBean(beanName)) {
            throw new IllegalStateException("Cannot publish the SagaSubscription of saga '%s' as '%s' because a bean with that name already exists. Occurrent publishes each @Saga's SagaSubscription under 'sagaSubscription-<id>', so rename your bean or the saga.".formatted(id, beanName));
        }
        beanFactory.registerSingleton(beanName, sagaSubscription);
    }

    /** The bean name the {@link SagaInstances} for {@code sagaId} is published under. */
    static String sagaInstancesBeanName(String sagaId) {
        return "sagaInstances-" + sagaId;
    }

    /** The bean name the {@link SagaSubscription} for {@code sagaId} is published under. */
    static String sagaSubscriptionBeanName(String sagaId) {
        return "sagaSubscription-" + sagaId;
    }

    // Gate the saga timer poller on the shared competing-consumer lease so only one instance polls, mirroring the
    // subscription model. On by default, opt out with occurrent.saga.competing-consumer.enabled=false. When disabled,
    // or when no strategy bean exists (for example subscriptions disabled), the poller runs on every instance instead.
    // Several strategy beans with no @Primary refuse to start rather than picking one or leaving the poller ungated.
    private @Nullable CompetingConsumerStrategy resolveSagaCompetingConsumerStrategy() {
        if (!occurrentProperties().getSaga().getCompetingConsumer().isEnabled()) {
            return null;
        }
        return CompetingConsumerStrategies.resolveUnique(applicationContext.getBeanProvider(CompetingConsumerStrategy.class));
    }

    private OccurrentProperties occurrentProperties() {
        return applicationContext.getBean(OccurrentProperties.class);
    }

    void close() {
        // Stop each saga's timer poller so no poller thread survives context shutdown. Before the models, because
        // shutting one down waits for a replay still in flight, and a timer that fires during that wait dispatches a
        // command into a context that is already going down.
        sagaSubscriptions.forEach(SagaSubscription::close);
        sagaSubscriptions.clear();
        // Then the catch-up replays, which the timer pollers are not: a replay runs on a thread of its own and only the
        // model that owns it can stop it.
        pushModels.forEach(CatchupThenPushSubscriptionModel::shutdown);
        pushModels.clear();
    }

    // Resolve the SagaStateStore: by store()/storeName() reference, else the unique SagaStateStore bean, else the
    // store starter's zero-config default, whose state type is read from the factory return type.
    @SuppressWarnings("unchecked")
    private <S> SagaStateStore<S> resolveSagaStateStore(org.occurrent.annotation.Saga annotation, Method factoryMethod, String id) {
        Class<?> storeType = annotation.store();
        String storeName = annotation.storeName();
        boolean byType = storeType != Void.class;
        boolean byName = !storeName.isBlank();
        if (byType || byName) {
            Object storeBean = resolveSagaStoreBeanByReference(storeType, storeName, byType, byName, id);
            if (!(storeBean instanceof SagaStateStore<?>)) {
                throw new IllegalArgumentException("@Saga '%s' store bean must be a SagaStateStore, but was %s.".formatted(id, storeBean.getClass().getName()));
            }
            return (SagaStateStore<S>) storeBean;
        }
        String[] names = applicationContext.getBeanNamesForType(SagaStateStore.class);
        if (names.length == 1) {
            return (SagaStateStore<S>) applicationContext.getBean(names[0]);
        }
        if (names.length > 1) {
            throw new IllegalStateException("@Saga '%s' found %d SagaStateStore beans (%s) and cannot pick one. Name the store with storeName = \"beanName\".".formatted(id, names.length, String.join(", ", names)));
        }
        // The state type is reflected first, so a factory that declares none reports that (the actionable fix) rather
        // than a missing provider.
        Class<S> stateType = (Class<S>) reflectSagaStateType(factoryMethod, id);
        // getIfAvailable() applies @Primary and @Fallback resolution and only throws when the container genuinely
        // cannot pick, so an ambiguous seam is reported with the annotation id rather than as a bare Spring failure.
        final DefaultSagaStateStoreProvider provider;
        try {
            provider = applicationContext.getBeanProvider(DefaultSagaStateStoreProvider.class).getIfAvailable();
        } catch (NoUniqueBeanDefinitionException e) {
            String[] providerNames = applicationContext.getBeanNamesForType(DefaultSagaStateStoreProvider.class);
            throw new IllegalStateException(("@Saga '%s' found %d DefaultSagaStateStoreProvider beans (%s) and cannot pick one to create the zero-config default saga state store. " +
                    "Declare a SagaStateStore bean, select one with store/storeName, or mark one provider @Primary.").formatted(id, providerNames.length, String.join(", ", providerNames)), e);
        }
        if (provider == null) {
            throw new IllegalStateException(("@Saga '%s' found no SagaStateStore bean and this starter contributes no zero-config default. " +
                    "Declare a SagaStateStore bean, or select one with store/storeName.").formatted(id));
        }
        return provider.createDefaultSagaStateStore(id, stateType);
    }

    private Object resolveSagaStoreBeanByReference(Class<?> storeType, String storeName, boolean byType, boolean byName, String id) {
        if (byType) {
            if (byName) {
                try {
                    return applicationContext.getBean(storeName, storeType);
                } catch (BeansException e) {
                    throw new IllegalArgumentException("@Saga '%s' could not resolve a store bean named '%s' of type %s: %s".formatted(id, storeName, storeType.getName(), e.getMessage()), e);
                }
            }
            String[] names = applicationContext.getBeanNamesForType(storeType);
            if (names.length == 0) {
                throw new IllegalStateException("@Saga '%s' found no bean of type %s. Declare one, or leave store unset to resolve by convention.".formatted(id, storeType.getName()));
            }
            if (names.length > 1) {
                throw new IllegalStateException("@Saga '%s' found %d beans of type %s (%s) and cannot pick one. Disambiguate with storeName = \"beanName\".".formatted(id, names.length, storeType.getName(), String.join(", ", names)));
            }
            return applicationContext.getBean(names[0]);
        }
        try {
            return applicationContext.getBean(storeName);
        } catch (BeansException e) {
            throw new IllegalArgumentException("@Saga '%s' could not resolve a store bean named '%s': %s".formatted(id, storeName, e.getMessage()), e);
        }
    }

    // Resolve the CommandDispatcher: by commandDispatcher()/commandDispatcherName() reference, else the unique
    // CommandDispatcher bean. There is no zero-config default, since commands are user types.
    @SuppressWarnings("unchecked")
    private <C> CommandDispatcher<C> resolveCommandDispatcher(org.occurrent.annotation.Saga annotation, String id) {
        Class<?> type = annotation.commandDispatcher();
        String name = annotation.commandDispatcherName();
        boolean byType = type != Void.class;
        boolean byName = !name.isBlank();
        Object dispatcherBean;
        if (byType && byName) {
            try {
                dispatcherBean = applicationContext.getBean(name, type);
            } catch (BeansException e) {
                throw new IllegalArgumentException("@Saga '%s' could not resolve a command dispatcher bean named '%s' of type %s: %s".formatted(id, name, type.getName(), e.getMessage()), e);
            }
        } else if (byType) {
            String[] names = applicationContext.getBeanNamesForType(type);
            if (names.length == 0) {
                throw new IllegalStateException("@Saga '%s' found no bean of type %s.".formatted(id, type.getName()));
            }
            if (names.length > 1) {
                throw new IllegalStateException("@Saga '%s' found %d beans of type %s (%s) and cannot pick one. Disambiguate with commandDispatcherName = \"beanName\".".formatted(id, names.length, type.getName(), String.join(", ", names)));
            }
            dispatcherBean = applicationContext.getBean(names[0]);
        } else if (byName) {
            try {
                dispatcherBean = applicationContext.getBean(name);
            } catch (BeansException e) {
                throw new IllegalArgumentException("@Saga '%s' could not resolve a command dispatcher bean named '%s': %s".formatted(id, name, e.getMessage()), e);
            }
        } else {
            String[] names = applicationContext.getBeanNamesForType(CommandDispatcher.class);
            if (names.length == 0) {
                throw new IllegalStateException(("@Saga '%s' needs a CommandDispatcher bean to run the commands it issues. Declare one, for example a lambda over your ApplicationService: " +
                        "`CommandDispatcher<MyCommand> d = cmd -> applicationService.execute(cmd.streamId(), events -> handle(cmd));`, or wrap a decider with CommandDispatchers.decider(applicationService, decider, MyCommand::streamId). " +
                        "If the saga has no command types and issues Invocations instead, that bean is CommandDispatchers.invocation(applicationService).").formatted(id));
            }
            if (names.length > 1) {
                throw new IllegalStateException("@Saga '%s' found %d CommandDispatcher beans (%s) and cannot pick one. Select one with commandDispatcher/commandDispatcherName.".formatted(id, names.length, String.join(", ", names)));
            }
            dispatcherBean = applicationContext.getBean(names[0]);
        }
        if (!(dispatcherBean instanceof CommandDispatcher<?>)) {
            throw new IllegalArgumentException("@Saga '%s' command dispatcher bean must be a CommandDispatcher, but was %s.".formatted(id, dispatcherBean.getClass().getName()));
        }
        return (CommandDispatcher<C>) dispatcherBean;
    }

    private Duration sagaTimerPollInterval() {
        return occurrentProperties().getSaga().getTimerPollInterval();
    }

    // The saga state type is the second type argument of the factory return type Saga<E, S, C>.
    private static Class<?> reflectSagaStateType(Method factoryMethod, String id) {
        Type returnType = factoryMethod.getGenericReturnType();
        if (returnType instanceof ParameterizedType parameterizedType) {
            Type[] arguments = parameterizedType.getActualTypeArguments();
            if (arguments.length >= 2) {
                Type stateArgument = arguments[1];
                if (stateArgument instanceof Class<?> stateClass) {
                    return stateClass;
                }
                if (stateArgument instanceof ParameterizedType stateParameterized && stateParameterized.getRawType() instanceof Class<?> rawState) {
                    return rawState;
                }
            }
        }
        throw new IllegalArgumentException(("@Saga '%s' needs a state store: either name one with store/storeName (a SagaStateStore), " +
                "or declare the factory return type with a concrete state type (for example Saga<MyEvent, MyState, MyCommand>) so the state store can use the zero-config default.").formatted(id));
    }
}
