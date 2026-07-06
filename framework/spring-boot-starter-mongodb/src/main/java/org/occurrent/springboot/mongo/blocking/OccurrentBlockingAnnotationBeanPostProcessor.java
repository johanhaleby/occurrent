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
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.mongo.common.SubscriptionAnnotations;
import org.occurrent.springboot.mongo.common.SubscriptionAnnotations.StreamSubscriptionDefinition;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.TimeBasedCheckpoint;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.mongodb.core.MongoOperations;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static org.occurrent.filter.Filter.CompositionOperator.OR;
import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Implements support for the {@link Subscription}, {@link StreamSubscription} and {@link DcbSubscription} annotations in
 * Spring Boot. The stack-neutral reflection and event-type resolution is shared with the reactive processor through
 * {@link SubscriptionAnnotations}.
 */
class OccurrentBlockingAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {

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
                throw new IllegalArgumentException("Method %s#%s is annotated with more than one of @Subscription, @StreamSubscription and @DcbSubscription, use only one.".formatted(bean.getClass().getName(), method.getName()));
            }
            if (streamSubscription != null) {
                processSubscribeAnnotation(bean, method, StreamSubscriptionDefinition.from(streamSubscription));
            } else if (subscription != null) {
                processAgnosticSubscribeAnnotation(bean, method, subscription);
            } else if (dcbSubscription != null) {
                processDcbSubscribeAnnotation(bean, method, dcbSubscription);
            }
        }
        return bean;
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(Object bean, Method method, StreamSubscriptionDefinition subscription) {
        String id = subscription.id();
        final Filter filter;
        final List<Class<?>> parameterTypes;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventTypeMapper = applicationContext.getBean(CloudEventConverter.class);
            parameterTypes = SubscriptionAnnotations.analyzeParameters(method, SubscriptionAnnotations::isStreamMetadataParameter);
            Class<E> specifiedEventType = (Class<E>) SubscriptionAnnotations.eventTypeOf(parameterTypes, SubscriptionAnnotations::isStreamMetadataParameter);
            List<Class<E>> domainEventTypesToSubscribeTo = SubscriptionAnnotations.resolveDomainEventTypes(id, bean, method, specifiedEventType, subscription.eventTypes(), subscription.annotationName());

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
            invoke(method, bean, SubscriptionAnnotations.bindArguments(parameterTypes, event, metadata, SubscriptionAnnotations::isStreamMetadataParameter));
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
    private <E> void processAgnosticSubscribeAnnotation(Object bean, Method method, Subscription annotation) {
        String id = annotation.id();
        final Filter filter;
        final List<Class<?>> parameterTypes;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventTypeMapper = applicationContext.getBean(CloudEventConverter.class);
            parameterTypes = SubscriptionAnnotations.analyzeParameters(method, SubscriptionAnnotations::isStreamMetadataParameter);
            Class<E> specifiedEventType = (Class<E>) SubscriptionAnnotations.eventTypeOf(parameterTypes, SubscriptionAnnotations::isStreamMetadataParameter);
            List<Class<E>> domainEventTypesToSubscribeTo = SubscriptionAnnotations.resolveDomainEventTypes(id, bean, method, specifiedEventType, annotation.eventTypes(), "@Subscription");

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
            invoke(method, bean, SubscriptionAnnotations.bindArguments(parameterTypes, event, metadata, SubscriptionAnnotations::isStreamMetadataParameter));
            return Unit.INSTANCE;
        };

        long startAtGlobalPosition = annotation.startAtGlobalPosition();
        if (startAtGlobalPosition >= 0 && annotation.startAt() != Subscription.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtGlobalPosition for @Subscription '%s', not both.".formatted(id));
        }
        StartAt startAt = generateAgnosticStartAt(id, annotation.startAt(), startAtGlobalPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtGlobalPosition >= 0 || annotation.startAt() == Subscription.StartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = shouldWaitUntilStartedAgnostic(replaysHistory, annotation.startupMode());
        Subscriptions<E> subscribable = applicationContext.getBean(Subscriptions.class);

        applyStartupWorkarounds();

        subscribable.subscribe(id, AgnosticSubscriptionFilter.filter(filter), startAt, shouldWaitUntilStarted, consumer);
    }

    private static boolean shouldWaitUntilStartedAgnostic(boolean replaysHistory, Subscription.StartupMode startupMode) {
        return switch (startupMode) {
            // A subscription that replays history may have a lot to read, so by default it starts in the background.
            case DEFAULT -> !replaysHistory;
            case WAIT_UNTIL_STARTED -> true;
            case BACKGROUND -> false;
        };
    }

    // Build the neutral StartAt over the unified global position. BEGINNING replays from global position 0,
    // startAtGlobalPosition replays after a specific position, both applying the same replay-then-resume logic. NOW and
    // DEFAULT go straight to live.
    private StartAt generateAgnosticStartAt(String subscriptionId, Subscription.StartPosition startPosition, long startAtGlobalPosition, Subscription.ResumeBehavior resumeBehavior) {
        if (startAtGlobalPosition >= 0) {
            return replayThenResumeAgnostic(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(startAtGlobalPosition)), resumeBehavior);
        }
        return switch (startPosition) {
            case NOW -> StartAt.now();
            case DEFAULT -> StartAt.dynamic(ctx -> {
                // Do not let the catch-up model run its default (replay from the beginning); delegate to the parent
                // live subscription instead by returning null to the catch-up layer.
                boolean isCatchupSubscription = CatchupSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isCatchupSubscription ? null : StartAt.subscriptionModelDefault();
            });
            case BEGINNING -> replayThenResumeAgnostic(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), resumeBehavior);
        };
    }

    // Replay from replayStart, then on later restarts either resume from the stored position (DEFAULT) or replay again
    // (SAME_AS_START_AT). SAME_AS_START_AT also disables the competing consumer and durable position storage by
    // delegating to the parent subscription model for those layers, so an in-memory read model rebuilt on every boot
    // sees every event and keeps no checkpoint. Mirrors the DCB replayThenResume.
    private StartAt replayThenResumeAgnostic(String subscriptionId, StartAt replayStart, Subscription.ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                boolean isCompetingConsumerSubscription = CompetingConsumerSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                boolean isDurableSubscription = DurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isCompetingConsumerSubscription || isDurableSubscription ? null : replayStart;
            });
            case DEFAULT -> StartAt.dynamic(ctx -> {
                CheckpointStorage checkpointStorage = applicationContext.getBean(CheckpointStorage.class);
                return checkpointStorage.exists(subscriptionId) ? StartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    @SuppressWarnings("unchecked")
    private <E> void processDcbSubscribeAnnotation(Object bean, Method method, DcbSubscription annotation) {
        String id = annotation.id();
        final DcbCriteria query;
        final List<Class<?>> parameterTypes;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventConverter = applicationContext.getBean(CloudEventConverter.class);
            parameterTypes = SubscriptionAnnotations.analyzeParameters(method, SubscriptionAnnotations::isDcbMetadataParameter);
            Class<E> specifiedEventType = (Class<E>) SubscriptionAnnotations.eventTypeOf(parameterTypes, SubscriptionAnnotations::isDcbMetadataParameter);
            List<Class<E>> domainEventTypesToSubscribeTo = SubscriptionAnnotations.resolveDomainEventTypes(id, bean, method, specifiedEventType, annotation.eventTypes(), "@DcbSubscription");
            List<String> cloudEventTypes = domainEventTypesToSubscribeTo.stream().map(cloudEventConverter::getCloudEventType).toList();
            List<Tag> tags = new ArrayList<>();
            for (String tag : annotation.tags()) {
                try {
                    tags.add(Tag.parse(tag));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("@DcbSubscription(id=\"%s\") has a malformed tag \"%s\", expected \"key:value\": %s".formatted(id, tag, e.getMessage()), e);
                }
            }
            query = SubscriptionAnnotations.buildDcbCriteria(cloudEventTypes, tags);
        } else {
            throw new IllegalArgumentException("A @DcbSubscription method must declare an event parameter, but %s#%s has none.".formatted(bean.getClass().getName(), method.getName()));
        }

        BiConsumer<DcbEventMetadata, E> consumer = (dcbMetadata, event) -> {
            Object metadataArgument = parameterTypes.contains(DcbEventMetadata.class) ? dcbMetadata : dcbMetadata.eventMetadata();
            invoke(method, bean, SubscriptionAnnotations.bindArguments(parameterTypes, event, metadataArgument, SubscriptionAnnotations::isDcbMetadataParameter));
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
                CheckpointStorage checkpointStorage = applicationContext.getBean(CheckpointStorage.class);
                return checkpointStorage.exists(subscriptionId) ? DcbStartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    // TODO Also check resume behavior if subscription exists!
    private static boolean shouldWaitUntilStarted(StartPositionToUse startPositionToUse, StartupMode startupMode) {
        return switch (startupMode) {
            case DEFAULT -> switch (startPositionToUse) {
                case StartPositionToUse.StartAtISO8601 ignored -> false;
                case StartPositionToUse.StartAtTimeEpoch ignored -> false;
                case StartPositionToUse.StartAtStartPosition startPosition -> switch (startPosition.startPosition) {
                    case BEGINNING_OF_TIME -> false;
                    case NOW, DEFAULT -> true;
                };
            };
            case WAIT_UNTIL_STARTED -> true;
            case BACKGROUND -> false;
        };
    }

    private @NonNull StartAt generateStartAt(String subscriptionId, StartPositionToUse startPositionToUse, ResumeBehavior resumeBehavior) {
        return switch (startPositionToUse) {
            case StartPositionToUse.StartAtISO8601 iso8601 -> switch (resumeBehavior) {
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
                        return StartAt.checkpoint(TimeBasedCheckpoint.from(iso8601.offsetDateTime()));
                    }
                });
                case DEFAULT -> StartAt.dynamic(() -> {
                    // Here we want to start the given IS8601 date/time the first time the subscription is started,
                    // but then return from the lastest stored checkpoint. To figure this out, we load the
                    // default CheckpointStorage bean and check if a checkpoint exists for this subscription.
                    // If it does, we know that it was not the first time the subscription was started, and thus we just let the
                    // subscription model operate according to its default. Otherwise, we explicitly specify the ISO8601 date as
                    // start date.
                    CheckpointStorage checkpointStorage = applicationContext.getBean(CheckpointStorage.class);
                    boolean checkpointExistsForSubscription = checkpointStorage.exists(subscriptionId);
                    if (checkpointExistsForSubscription) {
                        return StartAt.subscriptionModelDefault();
                    } else {
                        return StartAt.checkpoint(TimeBasedCheckpoint.from(iso8601.offsetDateTime()));
                    }
                });
            };
            case StartPositionToUse.StartAtTimeEpoch epoch -> {
                OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(epoch.startAtTimeEpoch), ZoneOffset.UTC);
                yield generateStartAt(subscriptionId, new StartPositionToUse.StartAtISO8601(offsetDateTime), resumeBehavior);
            }
            case StartPositionToUse.StartAtStartPosition startAtStartPosition -> switch (startAtStartPosition.startPosition) {
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
                            return StartAt.checkpoint(TimeBasedCheckpoint.beginningOfTime());
                        }
                    });
                    case DEFAULT -> {
                        // Here we want to start the beginning of time the first time the subscription is started,
                        // but then return from the lastest stored checkpoint. To figure this out, we load the
                        // default CheckpointStorage bean and check if a checkpoint exists for this subscription.
                        // If it does, we know that it was not the first time the subscription was started, and thus we just let the
                        // subscription model operate according to its default. Otherwise, we explicitly specify "beginning of time" as
                        // start date.
                        CheckpointStorage checkpointStorage = applicationContext.getBean(CheckpointStorage.class);
                        boolean checkpointExistsForSubscription = checkpointStorage.exists(subscriptionId);
                        if (checkpointExistsForSubscription) {
                            yield StartAt.subscriptionModelDefault();
                        } else {
                            yield StartAt.checkpoint(TimeBasedCheckpoint.beginningOfTime());
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
        };
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
            String startPositionNames = definedStartPositions.stream()
                    .map(position -> switch (position) {
                        case StartPositionToUse.StartAtISO8601 ignored -> "startAtISO8601";
                        case StartPositionToUse.StartAtTimeEpoch ignored -> "startAtTimeEpoch";
                        case StartPositionToUse.StartAtStartPosition ignored -> "startAt";
                    })
                    .collect(Collectors.joining(" and "));
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
}
