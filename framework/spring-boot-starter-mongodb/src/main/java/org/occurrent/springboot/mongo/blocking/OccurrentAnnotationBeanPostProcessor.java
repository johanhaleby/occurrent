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
import org.jspecify.annotations.NonNull;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.ResumeBehavior;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.annotation.StreamSubscription.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.blocking.DcbEventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.subscription.blocking.EventMetadata;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.DcbStartAt;
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
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.util.function.Predicate.not;
import static org.occurrent.filter.Filter.CompositionOperator.OR;
import static org.occurrent.subscription.OccurrentSubscriptionFilter.filter;

/**
 * Implements support for the {@link StreamSubscription} and {@link DcbSubscription} annotations, and the deprecated
 * {@link Subscription} alias, in Spring Boot.
 */
class OccurrentAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, @NonNull String beanName) throws BeansException {
        Class<?> managedBeanClass = bean.getClass();
        for (Method method : managedBeanClass.getDeclaredMethods()) {
            StreamSubscription streamSubscription = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
            Subscription subscription = AnnotationUtils.findAnnotation(method, Subscription.class);
            DcbSubscription dcbSubscription = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
            long annotationCount = Stream.of(streamSubscription, subscription, dcbSubscription).filter(Objects::nonNull).count();
            if (annotationCount > 1) {
                throw new IllegalArgumentException("Method %s#%s is annotated with more than one of @StreamSubscription, @DcbSubscription and the deprecated @Subscription, use only one.".formatted(bean.getClass().getName(), method.getName()));
            }
            if (streamSubscription != null) {
                processSubscribeAnnotation(bean, method, StreamSubscriptionDefinition.from(streamSubscription));
            } else if (subscription != null) {
                processSubscribeAnnotation(bean, method, StreamSubscriptionDefinition.from(subscription));
            } else if (dcbSubscription != null) {
                processDcbSubscribeAnnotation(bean, method, dcbSubscription);
            }
        }
        return bean;
    }

    /**
     * The normalized form of a stream subscription declaration, built from either the {@link StreamSubscription}
     * annotation or the deprecated {@link Subscription} alias. The deprecated annotation's enums are mapped to the
     * canonical {@link StreamSubscription} enums by name, since the constants are identical.
     */
    private record StreamSubscriptionDefinition(String id, Class<?>[] eventTypes, String startAtISO8601,
                                                long startAtTimeEpochMillis, StartPosition startAt,
                                                ResumeBehavior resumeBehavior, StartupMode startupMode, String annotationName) {

        static StreamSubscriptionDefinition from(StreamSubscription subscription) {
            return new StreamSubscriptionDefinition(subscription.id(), subscription.eventTypes(), subscription.startAtISO8601(),
                    subscription.startAtTimeEpochMillis(), subscription.startAt(), subscription.resumeBehavior(), subscription.startupMode(), "@StreamSubscription");
        }

        @SuppressWarnings("deprecation")
        static StreamSubscriptionDefinition from(Subscription subscription) {
            return new StreamSubscriptionDefinition(subscription.id(), subscription.eventTypes(), subscription.startAtISO8601(),
                    subscription.startAtTimeEpochMillis(), StartPosition.valueOf(subscription.startAt().name()),
                    ResumeBehavior.valueOf(subscription.resumeBehavior().name()), StartupMode.valueOf(subscription.startupMode().name()), "@Subscription");
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(Object bean, Method method, StreamSubscriptionDefinition subscription) {
        String id = subscription.id();
        final Filter filter;
        final List<Class<?>> parameterTypes;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventTypeMapper = applicationContext.getBean(CloudEventConverter.class);
            parameterTypes = analyzeParameters(method, OccurrentAnnotationBeanPostProcessor::isStreamMetadataParameter);
            Class<E> specifiedEventType = (Class<E>) eventTypeOf(parameterTypes, OccurrentAnnotationBeanPostProcessor::isStreamMetadataParameter);
            List<Class<E>> domainEventTypesToSubscribeTo = resolveDomainEventTypes(id, bean, method, specifiedEventType, subscription.eventTypes(), subscription.annotationName());

            if (domainEventTypesToSubscribeTo.size() == 1) {
                filter = Filter.type(cloudEventTypeMapper.getCloudEventType(domainEventTypesToSubscribeTo.get(0)));
            } else {
                List<Filter> typeFilters = domainEventTypesToSubscribeTo.stream()
                        .map(cloudEventTypeMapper::getCloudEventType)
                        .map(Filter::type)
                        .toList();
                filter = new Filter.CompositionFilter(OR, typeFilters);
            }
        } else {
            throw new IllegalArgumentException("A subscription method must declare an event parameter, but %s#%s has none.".formatted(bean.getClass().getName(), method.getName()));
        }

        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            invoke(method, bean, bindArguments(parameterTypes, event, metadata, OccurrentAnnotationBeanPostProcessor::isStreamMetadataParameter));
            return Unit.INSTANCE;
        };

        StartPositionToUse startPositionToUse = findStartPositionToUseOrThrow(subscription.id(), subscription.startAtISO8601(), subscription.startAtTimeEpochMillis(), subscription.startAt());
        ResumeBehavior resumeBehavior = subscription.resumeBehavior();
        StartAt startAt = generateStartAt(subscription.id(), startPositionToUse, resumeBehavior);

        boolean shouldWaitUntilStarted = shouldWaitUntilStarted(startPositionToUse, subscription.startupMode());
        StreamSubscriptions<E> subscribable = applicationContext.getBean(StreamSubscriptions.class);

        applyStartupWorkarounds();

        subscribable.subscribe(id, filter(filter), startAt, shouldWaitUntilStarted, consumer);
    }

    @SuppressWarnings("unchecked")
    private <E> void processDcbSubscribeAnnotation(Object bean, Method method, DcbSubscription annotation) {
        String id = annotation.id();
        final DcbQuery query;
        final List<Class<?>> parameterTypes;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventConverter = applicationContext.getBean(CloudEventConverter.class);
            parameterTypes = analyzeParameters(method, OccurrentAnnotationBeanPostProcessor::isDcbMetadataParameter);
            Class<E> specifiedEventType = (Class<E>) eventTypeOf(parameterTypes, OccurrentAnnotationBeanPostProcessor::isDcbMetadataParameter);
            List<Class<E>> domainEventTypesToSubscribeTo = resolveDomainEventTypes(id, bean, method, specifiedEventType, annotation.eventTypes(), "@DcbSubscription");
            List<String> cloudEventTypes = domainEventTypesToSubscribeTo.stream().map(cloudEventConverter::getCloudEventType).toList();
            query = buildDcbQuery(cloudEventTypes, List.of(annotation.tagsAllOf()));
        } else {
            throw new IllegalArgumentException("A @DcbSubscription method must declare an event parameter, but %s#%s has none.".formatted(bean.getClass().getName(), method.getName()));
        }

        BiConsumer<DcbEventMetadata, E> consumer = (dcbMetadata, event) -> {
            Object metadataArgument = parameterTypes.contains(DcbEventMetadata.class) ? dcbMetadata : dcbMetadata.eventMetadata();
            invoke(method, bean, bindArguments(parameterTypes, event, metadataArgument, OccurrentAnnotationBeanPostProcessor::isDcbMetadataParameter));
        };

        long startAtDcbPosition = annotation.startAtDcbPosition();
        if (startAtDcbPosition >= 0 && annotation.startAt() != DcbSubscription.DcbStartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtDcbPosition for @DcbSubscription '%s', not both.".formatted(id));
        }
        DcbStartAt startAt = generateDcbStartAt(id, annotation.startAt(), startAtDcbPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtDcbPosition >= 0 || annotation.startAt() == DcbSubscription.DcbStartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = shouldWaitUntilStartedDcb(replaysHistory, annotation.startupMode());
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);

        applyStartupWorkarounds();

        var subscription = dcbSubscriptions.subscribeWithMetadata(id, query, startAt, consumer);
        if (shouldWaitUntilStarted) {
            subscription.waitUntilStarted();
        }
    }

    private static boolean isStreamMetadataParameter(Class<?> parameterType) {
        return EventMetadata.class.isAssignableFrom(parameterType);
    }

    private static boolean isDcbMetadataParameter(Class<?> parameterType) {
        return EventMetadata.class.isAssignableFrom(parameterType) || DcbEventMetadata.class.isAssignableFrom(parameterType);
    }

    private static List<Class<?>> analyzeParameters(Method method, Predicate<Class<?>> isMetadataParameter) {
        List<Class<?>> parameterTypes = new ArrayList<>();
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (isMetadataParameter.test(parameterType)) {
                if (parameterTypes.stream().anyMatch(isMetadataParameter)) {
                    throw new IllegalArgumentException("A subscription method may declare at most one metadata parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                parameterTypes.add(parameterType);
            } else {
                if (parameterTypes.isEmpty()) {
                    parameterTypes.add(parameterType);
                } else if (parameterTypes.size() == 2) {
                    throw new IllegalArgumentException("A subscription method may declare an event parameter and at most one metadata parameter, but %s#%s declares more.".formatted(method.getDeclaringClass().getName(), method.getName()));
                } else if (parameterTypes.stream().anyMatch(isMetadataParameter)) {
                    parameterTypes.add(parameterType);
                } else {
                    throw new IllegalArgumentException("A subscription method may declare only one event parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
            }
        }
        return parameterTypes;
    }

    private static Class<?> eventTypeOf(List<Class<?>> parameterTypes, Predicate<Class<?>> isMetadataParameter) {
        return parameterTypes.stream().filter(not(isMetadataParameter)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("You need to declare an event type"));
    }

    @SuppressWarnings("unchecked")
    private static <E> List<Class<E>> resolveDomainEventTypes(String id, Object bean, Method method, Class<E> specifiedEventType, Class<?>[] eventTypesSpecifiedInAnnotation, String annotationName) {
        if (eventTypesSpecifiedInAnnotation.length == 0) {
            return getConcreteEventTypes(id, specifiedEventType);
        }
        return Arrays.stream(eventTypesSpecifiedInAnnotation)
                .flatMap(e -> getConcreteEventTypes(id, (Class<E>) e).stream())
                .peek(e -> {
                    if (!specifiedEventType.isAssignableFrom(e)) {
                        throw new IllegalStateException("Event type %s specified in the %s annotation with id %s is not assignable from the event type specified in %s#%s(..).".formatted(e.getName(), annotationName, id, bean.getClass().getName(), method.getName()));
                    }
                })
                .toList();
    }

    private static DcbQuery buildDcbQuery(List<String> cloudEventTypes, List<String> tagsAllOf) {
        boolean hasTypes = !cloudEventTypes.isEmpty();
        boolean hasTags = !tagsAllOf.isEmpty();
        if (!hasTypes && !hasTags) {
            return DcbQuery.all();
        } else if (hasTypes && hasTags) {
            return DcbQuery.typeAndTagsAllOf(cloudEventTypes, tagsAllOf);
        } else if (hasTypes) {
            return DcbQuery.types(cloudEventTypes.get(0), cloudEventTypes.stream().skip(1).toArray(String[]::new));
        } else {
            return DcbQuery.tagsAllOf(tagsAllOf.get(0), tagsAllOf.stream().skip(1).toArray(String[]::new));
        }
    }

    private static Object[] bindArguments(List<Class<?>> parameterTypes, Object event, Object metadata, Predicate<Class<?>> isMetadataParameter) {
        if (parameterTypes.size() == 1) {
            return new Object[]{event};
        }
        // Place each argument by which declared parameter slot is the metadata type, not by runtime assignability. A
        // broad event parameter (for example Object) is assignable from the metadata value too, so an isInstance check
        // would misplace it, this keys off the declared types instead and honors a metadata-first parameter order.
        Object first = isMetadataParameter.test(parameterTypes.get(0)) ? metadata : event;
        Object second = first == metadata ? event : metadata;
        return new Object[]{first, second};
    }

    private static void invoke(Method method, Object bean, Object[] arguments) {
        try {
            method.setAccessible(true);
            method.invoke(bean, arguments);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void applyStartupWorkarounds() {
        // These are workarounds for https://github.com/spring-projects/spring-framework/issues/32904
        applicationContext.getBean(MongoOperations.class);
        try {
            applicationContext.getBean("springApplicationAdminRegistrar");
        } catch (NoSuchBeanDefinitionException ignored) {
        }
        // End workarounds
    }

    private static boolean shouldWaitUntilStartedDcb(boolean replaysHistory, DcbSubscription.StartupMode startupMode) {
        return switch (startupMode) {
            // A subscription that replays history may have a lot to read, so by default it starts in the background.
            case DEFAULT -> !replaysHistory;
            case WAIT_UNTIL_STARTED -> true;
            case BACKGROUND -> false;
        };
    }

    private DcbStartAt generateDcbStartAt(String subscriptionId, DcbSubscription.DcbStartPosition startPosition, long startAtDcbPosition, DcbSubscription.ResumeBehavior resumeBehavior) {
        if (startAtDcbPosition >= 0) {
            // Start after a specific position, applying the same replay-then-resume logic BEGINNING uses.
            return replayThenResume(subscriptionId, DcbStartAt.afterPosition(startAtDcbPosition), resumeBehavior);
        }
        return switch (startPosition) {
            case NOW -> DcbStartAt.now();
            case DEFAULT -> DcbStartAt.subscriptionModelDefault();
            case BEGINNING -> replayThenResume(subscriptionId, DcbStartAt.beginning(), resumeBehavior);
        };
    }

    // Replay from replayStart, then on later restarts either resume from the stored position (DEFAULT) or replay again
    // (SAME_AS_START_AT). SAME_AS_START_AT also disables the competing consumer and durable position storage by
    // delegating to the parent subscription model for those layers, so an in-memory read model rebuilt on every boot
    // sees every event and keeps no checkpoint.
    private DcbStartAt replayThenResume(String subscriptionId, DcbStartAt replayStart, DcbSubscription.ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> DcbStartAt.dynamic(ctx -> {
                boolean isCompetingConsumerSubscription = CompetingConsumerSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                boolean isDurableSubscription = DurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isCompetingConsumerSubscription || isDurableSubscription ? null : replayStart;
            });
            case DEFAULT -> DcbStartAt.dynamic(ctx -> {
                SubscriptionPositionStorage subscriptionPositionStorage = applicationContext.getBean(SubscriptionPositionStorage.class);
                return subscriptionPositionStorage.exists(subscriptionId) ? DcbStartAt.subscriptionModelDefault() : replayStart;
            });
        };
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

    private @NonNull StartAt generateStartAt(String subscriptionId, StartPositionToUse startPositionToUse, ResumeBehavior resumeBehavior) {
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

    private static <E> @NonNull List<Class<E>> getConcreteEventTypes(String subscriptionId, Class<E> specifiedEventType) {
        final List<Class<E>> domainEventTypesToSubscribeTo;
        if (specifiedEventType.isSealed()) {
            //noinspection unchecked
            Class<E>[] permittedSubclasses = (Class<E>[]) specifiedEventType.getPermittedSubclasses();
            domainEventTypesToSubscribeTo = Arrays.stream(permittedSubclasses).flatMap(c -> getConcreteEventTypes(subscriptionId, c).stream()).toList();
        } else if (specifiedEventType.isInterface() || specifiedEventType.isArray() || Modifier.isAbstract(specifiedEventType.getModifiers())) {
            String msg = "You cannot subscribe to a non-sealed interface, abstract type, or array (problem is with %s for subscription '%s'). A concrete or sealed event type is required, or list the event types explicitly with the annotation's eventTypes attribute (for example eventTypes = {MyEvent1.class, MyEvent2.class}).";
            throw new IllegalArgumentException(msg.formatted(specifiedEventType.getName(), subscriptionId));
        } else {
            domainEventTypesToSubscribeTo = List.of(specifiedEventType);
        }
        return domainEventTypesToSubscribeTo;
    }
}