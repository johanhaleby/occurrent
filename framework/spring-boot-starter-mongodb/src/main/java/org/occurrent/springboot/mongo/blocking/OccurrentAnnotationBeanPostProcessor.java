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

import kotlin.Unit;
import kotlin.jvm.functions.Function2;
import org.jetbrains.annotations.NotNull;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.blocking.EventMetadata;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.TimeBasedSubscriptionPosition;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static org.occurrent.filter.Filter.CompositionOperator.OR;
import static org.occurrent.subscription.OccurrentSubscriptionFilter.filter;

class OccurrentAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, @NotNull String beanName) throws BeansException {
        Class<?> managedBeanClass = bean.getClass();
        for (Method method : managedBeanClass.getDeclaredMethods()) {
            Subscription subscription = AnnotationUtils.findAnnotation(method, Subscription.class);
            if (subscription != null) {
                processSubscribeAnnotation(bean, method, subscription);
            }
        }
        return bean;
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(Object bean, Method method, Subscription subscription) {
        String id = subscription.id();
        final Filter filter;
        List<Class<?>> parameterTypes = new ArrayList<>();
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventTypeMapper = applicationContext.getBean(CloudEventConverter.class);

            for (Class<?> parameterType : method.getParameterTypes()) {
                if (EventMetadata.class.isAssignableFrom(parameterType)) {
                    if (parameterTypes.contains(parameterType)) {
                        throw new IllegalArgumentException("EventMetadata already specified");
                    }
                    parameterTypes.add(parameterType);
                } else {
                    if (parameterTypes.isEmpty()) {
                        parameterTypes.add(parameterType);
                    } else if (parameterTypes.size() == 2) {
                        throw new IllegalArgumentException("Already specified parameters");
                    } else if (parameterTypes.contains(EventMetadata.class)) {
                        parameterTypes.add(parameterType);
                    } else {
                        throw new IllegalArgumentException("Already specified event parameter");
                    }
                }
            }

            if (parameterTypes.isEmpty() || parameterTypes.size() == 1 && parameterTypes.contains(EventMetadata.class)) {
                throw new IllegalArgumentException("You need to declare an event type");
            }

            //noinspection OptionalGetWithoutIsPresent
            Class<E> specifiedEventType = (Class<E>) parameterTypes.stream().filter(not(EventMetadata.class::isAssignableFrom)).findFirst().get();
            Class<?>[] eventTypesSpecifiedInAnnotation = subscription.eventTypes();

            final List<Class<E>> domainEventTypesToSubscribeTo;
            if (eventTypesSpecifiedInAnnotation.length == 0) {
                domainEventTypesToSubscribeTo = getConcreteEventTypes(id, specifiedEventType);
            } else {
                domainEventTypesToSubscribeTo = Arrays.stream(eventTypesSpecifiedInAnnotation)
                        .flatMap(e -> getConcreteEventTypes(id, (Class<E>) e).stream())
                        .peek(e -> {
                            if (!specifiedEventType.isAssignableFrom(e)) {
                                throw new IllegalStateException("Event type %s specified in the @Subscription annotation with id %s is not assignable from the event type specified in %s#%s(..).".formatted(e.getName(), id, bean.getClass().getName(), method.getName()));
                            }
                        })
                        .toList();
            }

            if (domainEventTypesToSubscribeTo.size() == 1) {
                String cloudEventType = cloudEventTypeMapper.getCloudEventType(domainEventTypesToSubscribeTo.get(0));
                filter = Filter.type(cloudEventType);
            } else {
                List<Filter> typeFilters = domainEventTypesToSubscribeTo.stream()
                        .map(cloudEventTypeMapper::getCloudEventType)
                        .map(Filter::type)
                        .toList();
                filter = new Filter.CompositionFilter(OR, typeFilters);
            }
            // Assuming 'myBean' is a bean that can be subscribed to, and it's available in the context
        } else {
            filter = Filter.all();
        }


        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            final Object[] arguments;
            if (parameterTypes.size() == 1) {
                // Subscription method only specified domain event
                arguments = new Object[]{event};
            } else {
                arguments = Stream.of(metadata, event).sorted((o1, o2) -> {
                    int index1 = parameterTypes.indexOf(o1.getClass());
                    int index2 = parameterTypes.indexOf(o2.getClass());
                    return Integer.compare(index1, index2);
                }).toArray(Object[]::new);
            }

            try {
                method.setAccessible(true);
                method.invoke(bean, arguments);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Unit.INSTANCE;
        };

        StartAt startAt = generateStartAt(subscription);

        Subscriptions<E> subscribable = applicationContext.getBean(Subscriptions.class);
        subscribable.subscribe(id, filter(filter), startAt, consumer).waitUntilStarted();
    }

    private static @NotNull StartAt generateStartAt(Subscription subscription) {
        return switch (subscription.startAt()) {
            case BEGINNING_OF_TIME -> switch (subscription.resumeBehavior()) {
                case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                    boolean isDurableSubscription = DurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                    if (isDurableSubscription) {
                        // Since we now know that we always start AND resume from the beginning of time for this subscription,
                        // we don't need to store the position in a durable storage, because we will always stream all events
                        // each time the subscription restarts anyway. Thus, we return null to instruct the DurableSubscriptionModel
                        // to simply delegate to the parent subscription. Note that this works because the parent of the CatchupSubscriptionModel
                        // is a DurableSubscriptionModel, so after the catch-up phase it'll hand over to the DurableSubscriptionModel.
                        // Unfortunately the CatchupSubscriptionModel is configured to write the position when the catch-up phase is completed,
                        // but it's harder to get around that.
                        return null;
                    } else {
                        return StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime());
                    }
                });
                case DEFAULT -> StartAt.subscriptionModelDefault();
            };
            case NOW -> StartAt.now();
            case DEFAULT -> StartAt.dynamic(ctx -> {
                // By default, we don't want to run the "default" behavior of the CatchupSubscriptionModel, which is to
                // start streaming from the beginning of time. We want to instruct the CatchupSubscriptionModel to simply
                // delegate to the parent subscription, which is what we do if we return null.
                boolean isCatchupSubscription = CatchupSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isCatchupSubscription ? null : StartAt.subscriptionModelDefault();
            });
        };
    }

    private static <E> @NotNull List<Class<E>> getConcreteEventTypes(String subscriptionId, Class<E> specifiedEventType) {
        final List<Class<E>> domainEventTypesToSubscribeTo;
        if (specifiedEventType.isSealed()) {
            //noinspection unchecked
            Class<E>[] permittedSubclasses = (Class<E>[]) specifiedEventType.getPermittedSubclasses();
            domainEventTypesToSubscribeTo = Arrays.stream(permittedSubclasses).flatMap(c -> getConcreteEventTypes(subscriptionId, c).stream()).toList();
        } else if (specifiedEventType.isInterface() || specifiedEventType.isArray() || Modifier.isAbstract(specifiedEventType.getModifiers())) {
            String msg = "You need cannot subscribe to a non-sealed interfaces or abstract types (problem is with %s). A concrete or sealed event type is required. You can also specify event types explicitly by using @Subscription(id = \"%s\", eventTypes = [MyEvent1.class, MyEvent2.class]))";
            throw new IllegalArgumentException(msg.formatted(specifiedEventType.getName(), subscriptionId));
        } else {
            domainEventTypesToSubscribeTo = List.of(specifiedEventType);
        }
        return domainEventTypesToSubscribeTo;
    }
}