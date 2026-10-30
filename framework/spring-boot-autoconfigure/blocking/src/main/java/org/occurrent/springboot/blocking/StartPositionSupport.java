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
import org.jspecify.annotations.Nullable;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.springboot.common.StartupWorkaround;
import org.occurrent.springboot.common.SubscriptionResumePoints;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.TimeBasedCheckpoint;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * The blocking-stack start-position and startup logic shared by the annotation registrars: it turns the annotation
 * start knobs into a {@link StartAt}/{@link DcbStartAt} (with the replay-then-resume behavior), decides whether a
 * subscription should block until started, and applies the Spring startup workarounds. The reactive stack rejects
 * time-based start positions, so this logic is deliberately not shared with it.
 */
class StartPositionSupport {

    private final ApplicationContext applicationContext;

    StartPositionSupport(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    // TODO Also check resume behavior if subscription exists!
    static boolean shouldWaitUntilStarted(StartPositionToUse startPositionToUse, StartupMode startupMode) {
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

    StartAt generateAgnosticStartAt(String subscriptionId, org.occurrent.annotation.StartPosition startPosition, long startAtGlobalPosition, ResumeBehavior resumeBehavior) {
        return withResumePoint(subscriptionId, agnosticStartAt(subscriptionId, startPosition, startAtGlobalPosition, resumeBehavior));
    }

    // Build the neutral StartAt over the unified global position. BEGINNING replays from global position 0,
    // startAtGlobalPosition replays after a specific position, both applying the same replay-then-resume logic. NOW and
    // DEFAULT go straight to live.
    private StartAt agnosticStartAt(String subscriptionId, org.occurrent.annotation.StartPosition startPosition, long startAtGlobalPosition, ResumeBehavior resumeBehavior) {
        if (startAtGlobalPosition >= 0) {
            return replayThenResumeAgnostic(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(startAtGlobalPosition)), resumeBehavior);
        }
        return switch (startPosition) {
            case NOW -> nowThenResume(resumeBehavior);
            case DEFAULT -> StartAt.dynamic(ctx -> {
                // Do not let the catch-up model run its default (replay from the beginning); delegate to the parent
                // live subscription instead by returning null to the catch-up layer.
                boolean isCatchupSubscription = CatchupSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isCatchupSubscription ? null : StartAt.subscriptionModelDefault();
            });
            case BEGINNING -> replayThenResumeAgnostic(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), resumeBehavior);
        };
    }

    // Competing consumer and durable layers store a position and let the instances compete, which a subscription that
    // starts at the same place on every boot must not do, so they are told to delegate to the parent model.
    private static boolean keepsNoPosition(SubscriptionModelContext ctx) {
        Class<?> type = ctx.subscriptionModelType();
        return CompetingConsumerSubscriptionModel.class.isAssignableFrom(type) || DurableSubscriptionModel.class.isAssignableFrom(type);
    }

    // Replay from replayStart, then on later restarts either resume from the stored position (DEFAULT) or replay again
    // (SAME_AS_START_AT). SAME_AS_START_AT also disables the competing consumer and durable position storage by
    // delegating to the parent subscription model for those layers, so an in-memory read model rebuilt on every boot
    // sees every event and keeps no checkpoint. Mirrors the DCB replayThenResume.
    private StartAt replayThenResumeAgnostic(String subscriptionId, StartAt replayStart, ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                return keepsNoPosition(ctx) ? null : replayStart;
            });
            case DEFAULT -> StartAt.dynamic(ctx -> {
                CheckpointStorage checkpointStorage = applicationContext.getBean(CheckpointStorage.class);
                return checkpointStorage.exists(subscriptionId) ? StartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    // NOW is where the subscription starts the first time. Under DEFAULT a restart continues from the stored position,
    // which a plain StartAt.now() leaves to the durable layer. Under SAME_AS_START_AT every start is NOW, so the
    // competing consumer and durable layers are told to stay out, as replayThenResumeAgnostic does for a replay start.
    private StartAt nowThenResume(ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case DEFAULT -> StartAt.now();
            case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                return keepsNoPosition(ctx) ? null : StartAt.now();
            });
        };
    }

    private DcbStartAt dcbNowThenResume(ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case DEFAULT -> DcbStartAt.now();
            case SAME_AS_START_AT -> DcbStartAt.dynamic(ctx -> {
                return keepsNoPosition(ctx) ? null : DcbStartAt.now();
            });
        };
    }

    void applyStartupWorkarounds() {
        // These are workarounds for https://github.com/spring-projects/spring-framework/issues/32904
        // Each store starter contributes the beans its own stack has to force into existence, this module knows none.
        applicationContext.getBeanProvider(StartupWorkaround.class).forEach(StartupWorkaround::apply);
        try {
            applicationContext.getBean("springApplicationAdminRegistrar");
        } catch (NoSuchBeanDefinitionException ignored) {
        }
        // End workarounds
    }

    DcbStartAt generateDcbStartAt(String subscriptionId, org.occurrent.annotation.StartPosition startPosition, long startAtDcbPosition, ResumeBehavior resumeBehavior) {
        return withResumePoint(subscriptionId, dcbStartAt(subscriptionId, startPosition, startAtDcbPosition, resumeBehavior));
    }

    @SuppressWarnings("removal")
    private DcbStartAt dcbStartAt(String subscriptionId, org.occurrent.annotation.StartPosition startPosition, long startAtDcbPosition, ResumeBehavior resumeBehavior) {
        if (startAtDcbPosition >= 0) {
            // Start after a specific position, applying the same replay-then-resume logic BEGINNING uses.
            return replayThenResume(subscriptionId, DcbStartAt.afterPosition(startAtDcbPosition), resumeBehavior);
        }
        return switch (startPosition) {
            case NOW -> dcbNowThenResume(resumeBehavior);
            case DEFAULT -> DcbStartAt.subscriptionModelDefault();
            case BEGINNING -> replayThenResume(subscriptionId, DcbStartAt.beginning(), resumeBehavior);
        };
    }

    // Replay from replayStart, then on later restarts either resume from the stored position (DEFAULT) or replay again
    // (SAME_AS_START_AT). SAME_AS_START_AT also disables the competing consumer and durable position storage by
    // delegating to the parent subscription model for those layers, so an in-memory read model rebuilt on every boot
    // sees every event and keeps no checkpoint.
    private DcbStartAt replayThenResume(String subscriptionId, DcbStartAt replayStart, ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> DcbStartAt.dynamic(ctx -> {
                return keepsNoPosition(ctx) ? null : replayStart;
            });
            case DEFAULT -> DcbStartAt.dynamic(ctx -> {
                CheckpointStorage checkpointStorage = applicationContext.getBean(CheckpointStorage.class);
                return checkpointStorage.exists(subscriptionId) ? DcbStartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    @NonNull StartAt generateStartAt(String subscriptionId, StartPositionToUse startPositionToUse, ResumeBehavior resumeBehavior) {
        return withResumePoint(subscriptionId, streamStartAt(subscriptionId, startPositionToUse, resumeBehavior));
    }

    private StartAt streamStartAt(String subscriptionId, StartPositionToUse startPositionToUse, ResumeBehavior resumeBehavior) {
        return switch (startPositionToUse) {
            case StartPositionToUse.StartAtISO8601 iso8601 -> switch (resumeBehavior) {
                case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                    if (keepsNoPosition(ctx)) {
                        // The subscription always starts and resumes at the same place, so it is in-memory only. The competing
                        // consumer must not kick in and the durable model has no position to store, so both delegate to the parent.
                        return null;
                    }
                    return StartAt.checkpoint(TimeBasedCheckpoint.from(iso8601.offsetDateTime()));
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
                yield streamStartAt(subscriptionId, new StartPositionToUse.StartAtISO8601(offsetDateTime), resumeBehavior);
            }
            case StartPositionToUse.StartAtStartPosition startAtStartPosition -> switch (startAtStartPosition.startPosition) {
                case BEGINNING_OF_TIME -> switch (resumeBehavior) {
                    case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                        if (keepsNoPosition(ctx)) {
                            // The subscription always starts and resumes at the same place, so it is in-memory only. The competing
                            // consumer must not kick in and the durable model has no position to store, so both delegate to the parent.
                            return null;
                        }
                        return StartAt.checkpoint(TimeBasedCheckpoint.beginningOfTime());
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
                case NOW -> nowThenResume(resumeBehavior);
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

    // Asks the application's SubscriptionResumePoints bean, when it declared one, each time a subscription model resolves
    // the start. A non-null answer replaces the annotation's start, except where that start is null for a competing
    // consumer or durable model, as under SAME_AS_START_AT. Those stay off, so a catch-up above them runs from the answer
    // without its live delegate. A null answer resolves what the annotation built.
    private StartAt withResumePoint(String subscriptionId, StartAt startAt) {
        @Nullable SubscriptionResumePoints resumePoints = applicationContext.getBeanProvider(SubscriptionResumePoints.class).getIfAvailable();
        if (resumePoints == null) {
            return startAt;
        }
        return StartAt.dynamic(context -> {
            @Nullable StartAt built = startAt.get(context);
            if (built == null && !CatchupSubscriptionModel.class.isAssignableFrom(context.subscriptionModelType())) {
                return null;
            }
            @Nullable String resumePoint = resumePoints.resumePointFor(subscriptionId);
            return resumePoint == null ? built : StartAt.checkpoint(new StringBasedCheckpoint(resumePoint));
        });
    }

    private DcbStartAt withResumePoint(String subscriptionId, DcbStartAt startAt) {
        @Nullable SubscriptionResumePoints resumePoints = applicationContext.getBeanProvider(SubscriptionResumePoints.class).getIfAvailable();
        if (resumePoints == null) {
            return startAt;
        }
        return DcbStartAt.dynamic(context -> {
            @Nullable DcbStartAt built = resolve(startAt, context);
            if (built == null) {
                return null;
            }
            @Nullable String resumePoint = resumePoints.resumePointFor(subscriptionId);
            return resumePoint == null ? built : DcbStartAt.checkpoint(resumePoint);
        });
    }

    private static @Nullable DcbStartAt resolve(DcbStartAt startAt, SubscriptionModelContext context) {
        @Nullable DcbStartAt resolved = startAt;
        while (resolved instanceof DcbStartAt.Dynamic dynamic) {
            resolved = dynamic.function().apply(context);
        }
        return resolved;
    }

    static StartPositionToUse findStartPositionToUseOrThrow(String subscriptionId, String startAtISO8601, long startAtTimeEpoch, StartPosition startPosition) {
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

    sealed interface StartPositionToUse {
        record StartAtISO8601(OffsetDateTime offsetDateTime) implements StartPositionToUse {

            StartAtISO8601(String iso8601) {
                this(toOffsetDateTime(iso8601));
            }

            static OffsetDateTime toOffsetDateTime(String iso8601) {
                try {
                    // Attempt to parse as OffsetDateTime directly which will fail if timezone is missing
                    return OffsetDateTime.parse(iso8601.trim(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                } catch (DateTimeParseException e) {
                    // No timezone, parse as a LocalDateTime and apply UTC
                    try {
                        return LocalDateTime.parse(iso8601.trim(), ISO_LOCAL_DATE_TIME).atOffset(ZoneOffset.UTC);
                    } catch (DateTimeParseException ex) {
                        throw new IllegalArgumentException("Invalid ISO8601 format: '" + iso8601 + "'", ex);
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
