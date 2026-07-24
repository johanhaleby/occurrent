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

package org.occurrent.springboot.mongo.blocking;

import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore;
import org.occurrent.springboot.mongo.common.OccurrentProperties;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Registers {@code @Saga} factory methods: subscribes the saga to its events, materializes per-instance state, dispatches
 * issued commands, and polls the store to fire timeouts. Invoked from the coordinator's
 * {@code afterSingletonsInstantiated}, after snapshots, sharing the one duplicate-id registry. Each created
 * {@link SagaSubscription} owns a timer poller that {@link #close()} stops when the context is destroyed. Blocking-stack
 * only.
 */
class SagaAnnotationRegistrar {

    private final ApplicationContext applicationContext;
    private final StartPositionSupport startPositionSupport;
    private final Set<String> registeredIds;
    // Registered sagas own a timer poller each, stop them when the context is destroyed so no poller thread leaks.
    private final List<SagaSubscription> sagaSubscriptions = new ArrayList<>();

    SagaAnnotationRegistrar(ApplicationContext applicationContext, StartPositionSupport startPositionSupport, Set<String> registeredIds) {
        this.applicationContext = applicationContext;
        this.startPositionSupport = startPositionSupport;
        this.registeredIds = registeredIds;
    }

    // A @Saga factory returns a Saga descriptor: subscribe to its events, materialize per-instance state into a
    // SagaStateStore, dispatch the commands it issues through a CommandDispatcher, and poll the store to fire timeouts.
    // Registered after other subscriptions so a saga cannot reuse an id. Blocking-stack only.
    @SuppressWarnings("unchecked")
    <E, S, C> void processSagaAnnotation(Object bean, Method method, org.occurrent.annotation.Saga annotation) {
        String id = annotation.id();
        if (!registeredIds.add(id)) {
            throw new IllegalArgumentException("Duplicate subscription/projection/snapshot/saga id '%s' (used by @Saga on %s#%s), each id must be unique because it is the durable checkpoint key.".formatted(id, bean.getClass().getName(), method.getName()));
        }
        if (method.getParameterCount() != 0) {
            throw new IllegalArgumentException("@Saga factory method %s#%s must take no parameters and return a Saga.".formatted(bean.getClass().getName(), method.getName()));
        }
        if (annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT && annotation.startAtGlobalPosition() >= 0) {
            throw new IllegalArgumentException("Specify either startAt or startAtGlobalPosition for @Saga '%s', not both.".formatted(id));
        }

        Object descriptor = invokeSagaFactory(method, bean);
        if (!(descriptor instanceof Saga<?, ?, ?>)) {
            throw new IllegalArgumentException("@Saga '%s' method %s#%s must return a Saga, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor.getClass().getName()));
        }
        Saga<E, S, C> saga = (Saga<E, S, C>) descriptor;

        CloudEventConverter<E> converter = applicationContext.getBean(CloudEventConverter.class);
        Subscribable subscribable = applicationContext.getBean(Subscribable.class);
        SagaStateStore<S> stateStore = resolveSagaStateStore(annotation, method, id);
        CommandDispatcher<C> commandDispatcher = resolveCommandDispatcher(annotation, id);
        StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        SagaRunnerConfig config = SagaRunnerConfig.defaults().withTimerPollInterval(sagaTimerPollInterval());
        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        SagaRunner<E, C> runner = stream ? SagaRunner.stream(subscribable, converter) : SagaRunner.agnostic(subscribable, converter);
        CompetingConsumerStrategy competingConsumerStrategy = resolveSagaCompetingConsumerStrategy();
        if (competingConsumerStrategy != null) {
            runner = runner.competingConsumerStrategy(competingConsumerStrategy);
        }

        startPositionSupport.applyStartupWorkarounds();
        sagaSubscriptions.add(runner.run(id, saga, stateStore, commandDispatcher, startAt, config));
    }

    // Gate the saga timer poller on the shared competing-consumer lease so only one instance polls, mirroring the
    // subscription model. On by default, opt out with occurrent.saga.competing-consumer.enabled=false. When disabled, or
    // when no strategy bean exists (for example subscriptions disabled), the poller runs on every instance as before.
    private CompetingConsumerStrategy resolveSagaCompetingConsumerStrategy() {
        if (!occurrentProperties().getSaga().getCompetingConsumer().isEnabled()) {
            return null;
        }
        return applicationContext.getBeanProvider(CompetingConsumerStrategy.class).getIfAvailable();
    }

    private OccurrentProperties occurrentProperties() {
        return applicationContext.getBean(OccurrentProperties.class);
    }

    void close() {
        // Stop each saga's timer poller so no poller thread survives context shutdown.
        sagaSubscriptions.forEach(SagaSubscription::close);
        sagaSubscriptions.clear();
    }

    private static Object invokeSagaFactory(Method method, Object bean) {
        try {
            method.setAccessible(true);
            Object result = method.invoke(bean);
            if (result == null) {
                throw new IllegalStateException("@Saga factory %s#%s returned null.".formatted(bean.getClass().getName(), method.getName()));
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to invoke @Saga factory %s#%s.".formatted(bean.getClass().getName(), method.getName()), e);
        }
    }

    // Resolve the SagaStateStore: by store()/storeName() reference, else the unique SagaStateStore bean, else a
    // zero-config MongoDB store in a "saga-<id>" collection whose state type is read from the factory return type.
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
        MongoOperations mongoOperations = applicationContext.getBean(MongoOperations.class);
        Class<S> stateType = (Class<S>) reflectSagaStateType(factoryMethod, id);
        if (stateType == FlowState.class) {
            // A flow saga's FlowState holds domain events, serialize them as CloudEvents (stable types) so they can move packages.
            CloudEventConverter<?> converter = applicationContext.getBean(CloudEventConverter.class);
            return new SpringMongoSagaStateStore<>(mongoOperations, "saga-" + id, stateType, converter);
        }
        return new SpringMongoSagaStateStore<>(mongoOperations, "saga-" + id, stateType);
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
                        "`CommandDispatcher<MyCommand> d = cmd -> applicationService.execute(cmd.streamId(), events -> handle(cmd));`, or wrap a decider with CommandDispatchers.decider(applicationService, decider, MyCommand::streamId).").formatted(id));
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
                "or declare the factory return type with a concrete state type (for example Saga<MyEvent, MyState, MyCommand>) so the store can default to MongoDB.").formatted(id));
    }
}
