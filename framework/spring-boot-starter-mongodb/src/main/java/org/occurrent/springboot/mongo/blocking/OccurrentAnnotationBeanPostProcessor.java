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
import org.occurrent.annotation.Subscription.ResumeBehavior;
import org.occurrent.annotation.Subscription.StartPosition;
import org.occurrent.annotation.Subscription.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.blocking.EventMetadata;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.TimeBasedSubscriptionPosition;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.mongodb.core.MongoOperations;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.util.function.Predicate.not;
import static org.occurrent.filter.Filter.CompositionOperator.OR;
import static org.occurrent.subscription.OccurrentSubscriptionFilter.filter;

/**
 * Implements support for the {@link Subscription} annotation in Spring Boot
 */
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
        } else {
            filter = Filter.all();
        }


        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            final Object[] arguments;
            if (parameterTypes.size() == 1) {
                // Method annotated with @Subscription only specified domain event
                arguments = new Object[]{event};
            } else {
                // Method annotated with @Subscription specifies domain event and metadata
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

        StartPositionToUse startPositionToUse = findStartPositionToUseOrThrow(subscription.id(), subscription.startAtISO8601(), subscription.startAtTimeEpochMillis(), subscription.startAt());
        ResumeBehavior resumeBehavior = subscription.resumeBehavior();
        StartAt startAt = generateStartAt(subscription.id(), startPositionToUse, resumeBehavior);

        boolean shouldWaitUntilStarted = shouldWaitUntilStarted(startPositionToUse, subscription.startupMode());
        Subscriptions<E> subscribable = applicationContext.getBean(Subscriptions.class);

        // These are workarounds for https://github.com/spring-projects/spring-framework/issues/32904
        applicationContext.getBean(MongoOperations.class);
        try {
            applicationContext.getBean("springApplicationAdminRegistrar");
        } catch (NoSuchBeanDefinitionException ignored) {
        }
        // End workarounds

        subscribable.subscribe(id, filter(filter), startAt, shouldWaitUntilStarted, consumer);
    }

    // TODO Also check resume behavior if subscription exists!
    private static boolean shouldWaitUntilStarted(StartPositionToUse startPositionToUse, StartupMode startupMode) {
        return switch (startupMode) {
            case DEFAULT -> {
                if (startPositionToUse instanceof StartPositionToUse.StartAtISO8601 || startPositionToUse instanceof StartPositionToUse.StartAtTimeEpoch) {
                    yield false;
                } else {
                    StartPositionToUse.StartAtStartPosition startPosition = (StartPositionToUse.StartAtStartPosition) startPositionToUse;
                    yield switch (startPosition.startPosition) {
                        case BEGINNING_OF_TIME -> false;
                        case NOW, DEFAULT -> true;
                    };
                }
            }
            case WAIT_UNTIL_STARTED -> true;
            case BACKGROUND -> false;
        };
    }

    private @NotNull StartAt generateStartAt(String subscriptionId, StartPositionToUse startPositionToUse, ResumeBehavior resumeBehavior) {
        final StartAt startAt;
        if (startPositionToUse instanceof StartPositionToUse.StartAtISO8601 iso8601) {
            startAt = switch (resumeBehavior) {
                case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                    boolean isCompetingConsumerSubscription = CompetingConsumerSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                    if (isCompetingConsumerSubscription) {
                        // Since we now know that we always start AND resume from the beginning of time for this subscription,
                        // we don't want the competing consumer to kick in. This is because the subscription will be in-memory only.
                        return null;
                    }

                    boolean isDurableSubscription = DurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                    if (isDurableSubscription) {
                        // Since we now know that we always start AND resume from the specified iso8601 for this subscription,
                        // we don't need to store the position in a durable storage, because we will always stream all events
                        // each time the subscription restarts anyway. Thus, we return null to instruct the DurableSubscriptionModel
                        // to simply delegate to the parent subscription.
                        return null;
                    } else {
                        return StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.from(iso8601.offsetDateTime()));
                    }
                });
                case DEFAULT -> StartAt.dynamic(() -> {
                    // Here we want to start the given IS8601 date/time the first time the subscription is started,
                    // but then return from the lastest stored subscription position. To figure this out, we load the
                    // default SubscriptionPositionStorage bean and check if a subscription position exists for this subscription.
                    // If it does, we know that it was not the first time the subscription was started, and thus we just let the
                    // subscription model operate according to its default. Otherwise, we explicitly specify the ISO8601 date as
                    // start date.
                    SubscriptionPositionStorage subscriptionPositionStorage = applicationContext.getBean(SubscriptionPositionStorage.class);
                    boolean subscriptionPositionExistsForSubscription = subscriptionPositionStorage.exists(subscriptionId);
                    if (subscriptionPositionExistsForSubscription) {
                        return StartAt.subscriptionModelDefault();
                    } else {
                        return StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.from(iso8601.offsetDateTime()));
                    }
                });
            };
        } else if (startPositionToUse instanceof StartPositionToUse.StartAtTimeEpoch epoch) {
            OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(epoch.startAtTimeEpoch), ZoneOffset.UTC);
            startAt = generateStartAt(subscriptionId, new StartPositionToUse.StartAtISO8601(offsetDateTime), resumeBehavior);
        } else if (startPositionToUse instanceof StartPositionToUse.StartAtStartPosition startAtStartPosition) {
            startAt = switch (startAtStartPosition.startPosition) {
                case BEGINNING_OF_TIME -> switch (resumeBehavior) {
                    case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                        boolean isCompetingConsumerSubscription = CompetingConsumerSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                        if (isCompetingConsumerSubscription) {
                            // Since we now know that we always start AND resume from the beginning of time for this subscription,
                            // we don't want the competing consumer to kick in. This is because the subscription will be in-memory only.
                            return null;
                        }

                        boolean isDurableSubscription = DurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                        if (isDurableSubscription) {
                            // Since we now know that we always start AND resume from the beginning of time for this subscription,
                            // we don't need to store the position in a durable storage, because we will always stream all events
                            // each time the subscription restarts anyway. Thus, we return null to instruct the DurableSubscriptionModel
                            // to simply delegate to the parent subscription.
                            return null;
                        } else {
                            return StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime());
                        }
                    });
                    case DEFAULT -> {
                        // Here we want to start the beginning of time the first time the subscription is started,
                        // but then return from the lastest stored subscription position. To figure this out, we load the
                        // default SubscriptionPositionStorage bean and check if a subscription position exists for this subscription.
                        // If it does, we know that it was not the first time the subscription was started, and thus we just let the
                        // subscription model operate according to its default. Otherwise, we explicitly specify "beginning of time" as
                        // start date.
                        SubscriptionPositionStorage subscriptionPositionStorage = applicationContext.getBean(SubscriptionPositionStorage.class);
                        boolean subscriptionPositionExistsForSubscription = subscriptionPositionStorage.exists(subscriptionId);
                        if (subscriptionPositionExistsForSubscription) {
                            yield StartAt.subscriptionModelDefault();
                        } else {
                            yield StartAt.subscriptionPosition(TimeBasedSubscriptionPosition.beginningOfTime());
                        }
                    }
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
        } else {
            throw new IllegalStateException("Internal error: Didn't recognize start position");
        }

        return startAt;
    }

    private static StartPositionToUse findStartPositionToUseOrThrow(String subscriptionId, String startAtISO8601, long startAtTimeEpoch, StartPosition startPosition) {
        StartPositionToUse iso8601 = startAtISO8601.isBlank() ? null : new StartPositionToUse.StartAtISO8601(startAtISO8601);
        StartPositionToUse epoch = startAtTimeEpoch < 0 ? null : new StartPositionToUse.StartAtTimeEpoch(startAtTimeEpoch);
        // Next, we include the start position based on whether a time has also been explicitly defined
        // (because StartPositionToUse is DEFAULT if not specified explicitly)
        boolean timeExplicitlyDefined = iso8601 != null || epoch != null;
        final StartPositionToUse startAtStartPosition;
        if (timeExplicitlyDefined) {
            startAtStartPosition = startPosition == StartPosition.DEFAULT ? null : new StartPositionToUse.StartAtStartPosition(startPosition);
        } else {
            startAtStartPosition = new StartPositionToUse.StartAtStartPosition(startPosition);
        }
        var definedStartPositions = Stream.of(iso8601, epoch, startAtStartPosition).filter(Objects::nonNull).toList();

        if (definedStartPositions.isEmpty()) {
            throw new IllegalArgumentException("You need to specify at least one valid start position for subscription '%s'.".formatted(subscriptionId));
        } else if (definedStartPositions.size() > 1) {
            String startPositionNames = definedStartPositions.stream().map(position -> {
                if (position instanceof StartPositionToUse.StartAtISO8601) {
                    return "startAtISO8601";
                } else if (position instanceof StartPositionToUse.StartAtTimeEpoch) {
                    return "startAtTimeEpoch";
                } else {
                    return "startAt";
                }
            }).collect(Collectors.joining(" and "));
            throw new IllegalArgumentException("You can only specify one start position for subscription '%s', both %s are defined.".formatted(subscriptionId, startPositionNames));
        } else {
            return definedStartPositions.get(0);
        }
    }

    private sealed interface StartPositionToUse {
        record StartAtISO8601(OffsetDateTime offsetDateTime) implements StartPositionToUse {

            StartAtISO8601(String iso8601) {
                this(toOffsetDateTime(iso8601));
            }

            static OffsetDateTime toOffsetDateTime(String iso8601) {
                try {
                    // Attempt to parse as OffsetDateTime directly which will fail if timezone is missing
                    return OffsetDateTime.parse(iso8601.trim(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                } catch (DateTimeParseException e) {
                    // Parsing failed, parse as LocalDateTime and convert to OffsetDateTime with default zone
                    LocalDateTime localDateTime = LocalDateTime.parse(iso8601.trim(), ISO_LOCAL_DATE_TIME);
                    try {
                        return localDateTime.atOffset(ZoneOffset.UTC);
                    } catch (DateTimeParseException ex) {
                        throw new IllegalArgumentException("Invalid ISO8601 format: '" + iso8601 + "'", e);
                    }
                }
            }
        }

        record StartAtTimeEpoch(long startAtTimeEpoch) implements StartPositionToUse {
            public StartAtTimeEpoch {
                if (startAtTimeEpoch < 0) {
                    throw new IllegalArgumentException("startAtTimeEpoch cannot be negative");
                }
            }
        }

        record StartAtStartPosition(StartPosition startPosition) implements StartPositionToUse {
        }
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