/*
 *
 *  Copyright 2026 Johan Haleby
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

import org.jspecify.annotations.Nullable;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.StartupWorkaround;
import org.occurrent.springboot.common.SubscriptionAnnotations.StreamSubscriptionDefinition;
import org.occurrent.springboot.common.SubscriptionResumePoints;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

/**
 * Start-position and startup logic for the reactive stack. The reactive catch-up model replays only by position (or,
 * for DCB, by DCB position), so this diverges from the blocking stack's time-based start machinery: a
 * {@link StreamSubscriptionDefinition} that asks for a specific historical time fails loud rather than resolving a
 * wall-clock time to a position.
 */
class StartPositionSupport {

    private final ApplicationContext applicationContext;

    StartPositionSupport(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
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

    // Ask the event store, not any reader that happens to be in the context. Shared with the store starter that decides
    // whether to layer in a catch-up model, so the answer here can never promise a replay the wiring cannot perform.
    private @Nullable PositionOrderedReader eventStoreReader() {
        return PositionOrderedEventStores.find(applicationContext);
    }

    // A capability-agnostic subscription replays over the unified global position, so replay is supported whenever the
    // store writes a position, regardless of which capabilities are enabled (unlike stream replay, which also requires
    // the STREAM capability).
    boolean positionReplaySupported() {
        PositionOrderedReader reader = eventStoreReader();
        return reader != null && reader.writesPosition();
    }

    // A @StreamSubscription can replay history when the store has the STREAM capability and writes stream position,
    // which wires a catch-up model that replays stream filters by position. A combined STREAM+DCB store replays too. A
    // DCB-only store also writes position but has no stream events, so it does not support stream history replay.
    boolean streamHistoryReplaySupported() {
        PositionOrderedReader reader = eventStoreReader();
        if (reader == null || !reader.writesPosition()) {
            return false;
        }
        OccurrentProperties occurrentProperties = applicationContext.getBean(OccurrentProperties.class);
        return occurrentProperties.getEventStore().getCapabilities().contains(EventStoreCapability.STREAM);
    }

    // A stream subscription's start position. A specific start time (startAtISO8601 or startAtTimeEpochMillis) always
    // fails loud, since position replay cannot resolve a wall-clock time to a position. BEGINNING_OF_TIME replays
    // history when replay is supported (a STREAM store that writes position), and fails loud otherwise rather than
    // silently starting live. NOW and DEFAULT are always supported.
    StartAt generateStreamStartAt(StreamSubscriptionDefinition subscription, boolean historyReplaySupported) {
        boolean specificTimeStart = !subscription.startAtISO8601().isBlank()
                || subscription.startAtTimeEpochMillis() >= 0;
        if (specificTimeStart) {
            throw new IllegalArgumentException(("@StreamSubscription '%s' specifies a specific start time (startAtISO8601 or startAtTimeEpochMillis), but the reactive stack's position-based " +
                    "stream catch-up cannot honor a specific historical start time, it can only replay from BEGINNING_OF_TIME, NOW, or DEFAULT. Use startAt = BEGINNING_OF_TIME to replay all history, " +
                    "or NOW/DEFAULT, instead of a specific start time.").formatted(subscription.id()));
        }
        boolean beginningOfTimeStart = subscription.startAt() == StartPosition.BEGINNING_OF_TIME;
        if (beginningOfTimeStart && !historyReplaySupported) {
            throw new IllegalArgumentException(("@StreamSubscription '%s' asks to replay history (BEGINNING_OF_TIME), but this store does not support reactive stream history replay " +
                    "(it has no STREAM capability, or stream position is off). Enable stream position (on by default) for a STREAM store, use startAt = NOW or DEFAULT, or use @DcbSubscription for a DCB store.").formatted(subscription.id()));
        }
        if (beginningOfTimeStart) {
            // Map BEGINNING_OF_TIME to position 0, which the reactive stream catch-up model replays before going live.
            return replayThenResumeAgnostic(subscription.id(), StartAt.checkpoint(GlobalCheckpoint.of(0)), subscription.resumeBehavior());
        }
        return switch (subscription.startAt()) {
            case NOW -> nowThenResume(subscription.resumeBehavior());
            // DEFAULT resumes from the durably stored position, falling back to the subscription model default on first start.
            case DEFAULT -> StartAt.subscriptionModelDefault();
            case BEGINNING_OF_TIME -> throw new IllegalStateException("Unreachable: BEGINNING_OF_TIME handled above");
        };
    }

    // NOW is where the subscription starts the first time. Under DEFAULT a restart continues from the stored position
    // instead, so an event written while the application was down is not skipped. The model default does that, and it
    // records the position of the first start, which a plain StartAt.now() would not. Under SAME_AS_START_AT every start
    // is NOW.
    private StartAt nowThenResume(ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> StartAt.now();
            case DEFAULT -> StartAt.subscriptionModelDefault();
        };
    }

    // The DCB counterpart of nowThenResume.
    private DcbStartAt dcbNowThenResume(ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> DcbStartAt.now();
            case DEFAULT -> DcbStartAt.subscriptionModelDefault();
        };
    }

    // Build the neutral StartAt over the unified global position. BEGINNING replays from global position 0,
    // startAtGlobalPosition replays after a specific position, both applying the same replay-then-resume logic. NOW
    // starts through nowThenResume, so under DEFAULT it resumes like the model default does.
    StartAt generateAgnosticStartAt(String subscriptionId, org.occurrent.annotation.StartPosition startPosition, long startAtGlobalPosition, ResumeBehavior resumeBehavior) {
        if (startAtGlobalPosition >= 0) {
            return replayThenResumeAgnostic(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(startAtGlobalPosition)), resumeBehavior);
        }
        return switch (startPosition) {
            case NOW -> nowThenResume(resumeBehavior);
            case DEFAULT -> StartAt.subscriptionModelDefault();
            case BEGINNING -> replayThenResumeAgnostic(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), resumeBehavior);
        };
    }

    // Replay from replayStart, then on later restarts either resume from the stored position (DEFAULT) or replay again
    // (SAME_AS_START_AT). SAME_AS_START_AT disables durable position storage by delegating to the parent subscription
    // model, so an in-memory read model rebuilt on every boot sees every event and keeps no checkpoint. There is no
    // reactive competing-consumer model, so only the durable layer is considered. Mirrors the DCB replayThenResume.
    //
    // CheckpointStorage is looked up only when the subscription model evaluates the start, so a model that never does
    // needs none. A missing one fails that evaluation with a NoSuchBeanDefinitionException, which a late subscribe does
    // not try again. One that fails to build is tried again or not by why it failed, as LateSubscriber.retriable says.
    StartAt replayThenResumeAgnostic(String subscriptionId, StartAt replayStart, ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                boolean isDurableSubscription = ReactorDurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isDurableSubscription ? null : replayStart;
            });
            case DEFAULT -> StartAt.dynamic(ctx -> {
                CheckpointStorage storage = applicationContext.getBean(CheckpointStorage.class);
                return storage.read(subscriptionId).blockOptional().isPresent() ? StartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    @SuppressWarnings("removal")
    DcbStartAt generateDcbStartAt(String subscriptionId, org.occurrent.annotation.StartPosition startPosition, long startAtDcbPosition, ResumeBehavior resumeBehavior) {
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
    // (SAME_AS_START_AT). SAME_AS_START_AT disables durable position storage by delegating to the parent subscription
    // model, so an in-memory read model rebuilt on every boot sees every event and keeps no checkpoint. There is no
    // reactive competing-consumer model, so only the durable layer is considered.
    DcbStartAt replayThenResume(String subscriptionId, DcbStartAt replayStart, ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> DcbStartAt.dynamic(ctx -> {
                boolean isDurableSubscription = ReactorDurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isDurableSubscription ? null : replayStart;
            });
            case DEFAULT -> DcbStartAt.dynamic(ctx -> {
                CheckpointStorage storage = applicationContext.getBean(CheckpointStorage.class);
                return storage.read(subscriptionId).blockOptional().isPresent() ? DcbStartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    // Asks the application's SubscriptionResumePoints bean, when it declared one, each time a subscription model resolves
    // the start. A non-null answer replaces the annotation's start, except where that start is null for a durable model,
    // as under SAME_AS_START_AT. That model stays off, so a catch-up above it runs from the answer without its live
    // delegate. A null answer resolves what the annotation built, so LateSubscriber.startIsFixed takes the start from
    // before this wrap.
    StartAt withResumePoint(String subscriptionId, StartAt startAt) {
        @Nullable SubscriptionResumePoints resumePoints = applicationContext.getBeanProvider(SubscriptionResumePoints.class).getIfAvailable();
        if (resumePoints == null) {
            return startAt;
        }
        return StartAt.dynamic(context -> {
            @Nullable StartAt built = startAt.get(context);
            if (built == null) {
                return null;
            }
            @Nullable String resumePoint = resumePoints.resumePointFor(subscriptionId);
            return resumePoint == null ? built : StartAt.checkpoint(new StringBasedCheckpoint(resumePoint));
        });
    }

    DcbStartAt withResumePoint(String subscriptionId, DcbStartAt startAt) {
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
}
