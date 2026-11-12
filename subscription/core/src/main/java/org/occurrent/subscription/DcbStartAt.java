/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.CheckpointCarrier;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Specifies where a DCB subscription should start. This is the DCB counterpart to {@link StartAt}: apart from
 * {@link #checkpoint(String)}, which takes the string a subscription delivered, it can only express DCB start
 * positions (a {@code position} or one of the relative starts), never a time-based stream position, so a DCB
 * subscription cannot be handed a position that belongs to a stream subscription.
 * <p>
 * Convert to the generic {@link StartAt} the shared subscription model consumes with {@link #toStartAt()}.
 */
@NullMarked
public sealed interface DcbStartAt {

    /**
     * Convert this DCB start position into the generic {@link StartAt} understood by the shared subscription model.
     */
    StartAt toStartAt();

    /**
     * Start subscribing at this moment in time, delivering only events written from now on (no history replay).
     */
    static DcbStartAt now() {
        return Relative.NOW;
    }

    /**
     * Start subscribing at the subscription model default. Typically this resumes from the last stored DCB position if
     * one exists, otherwise it behaves like {@link #now()}.
     */
    static DcbStartAt subscriptionModelDefault() {
        return Relative.DEFAULT;
    }

    /**
     * Start subscribing from the beginning of the DCB sequence, replaying the whole history by {@code position} before
     * switching to live delivery. DCB positions are assigned from {@code 1}, so the replay starts after position
     * {@code 0} and delivers the first event. A catch-up subscription model replays the history, and the Spring Boot
     * stack includes one. A subscription model without catch-up support may not replay past events from this position,
     * and the MongoDB subscription models refuse it with an {@link UnsupportedStartAtException}, also when a
     * {@link #dynamic(Function) dynamic} start returns it.
     */
    static DcbStartAt beginning() {
        return new AtPosition(0);
    }

    /**
     * Start subscribing after the given DCB sequence position, delivering events from {@code lastProcessedPosition + 1}
     * onward. {@code 0} replays the whole history, the same as {@link #beginning()}. A position above {@code 0} is
     * refused at start, since an event that committed below it after you read it would never be delivered. To resume
     * after an event, keep the checkpoint the subscription delivers with it and start from {@link #checkpoint(String)}.
     *
     * @deprecated A position above {@code 0} is refused. Use {@link #beginning()} to replay the whole history, and
     * {@link #checkpoint(String)} to resume after an event.
     */
    @Deprecated(forRemoval = true)
    static DcbStartAt afterPosition(long lastProcessedPosition) {
        return new AtPosition(lastProcessedPosition);
    }

    /**
     * Start subscribing from a checkpoint the subscription delivered with an event, read from
     * {@link EventMetadata#getCheckpoint()} or {@code DcbEventMetadata.checkpoint()}. Store it with the application's
     * state, in the same transaction, and pass it back here on restart. The same as
     * {@code StartAt.checkpoint(new StringBasedCheckpoint(checkpoint))}. {@link CheckpointCarrier#checkpoint()} says
     * which subscription models understand it, and what a restart from it does once the oplog no longer reaches back
     * to it.
     */
    static DcbStartAt checkpoint(String checkpoint) {
        return new AtCheckpoint(checkpoint);
    }

    /**
     * Create a DCB start position that is resolved each time the subscription is started, for example to start from the
     * beginning the first time and then resume from the stored position. The function receives the subscription model
     * context and may return {@code null} to signal that the subscription should not start at this layer and should
     * delegate to the parent subscription model, the same contract as {@link StartAt#dynamic(Function)}.
     */
    static DcbStartAt dynamic(Function<SubscriptionModelContext, @Nullable DcbStartAt> function) {
        return new Dynamic(requireNonNull(function, "Dynamic DcbStartAt function cannot be null"));
    }

    enum Relative implements DcbStartAt {
        NOW {
            @Override
            public StartAt toStartAt() {
                return StartAt.now();
            }
        },
        DEFAULT {
            @Override
            public StartAt toStartAt() {
                return StartAt.subscriptionModelDefault();
            }
        }
    }

    record AtPosition(long lastProcessedPosition) implements DcbStartAt {
        public AtPosition {
            if (lastProcessedPosition < 0) {
                throw new IllegalArgumentException("DCB position cannot be negative, was " + lastProcessedPosition);
            }
        }

        @Override
        public StartAt toStartAt() {
            return StartAt.checkpoint(GlobalCheckpoint.of(lastProcessedPosition));
        }
    }

    record AtCheckpoint(String checkpoint) implements DcbStartAt {
        public AtCheckpoint {
            requireNonNull(checkpoint, "DCB checkpoint cannot be null");
            if (checkpoint.isBlank()) {
                throw new IllegalArgumentException("DCB checkpoint cannot be blank");
            }
        }

        @Override
        public StartAt toStartAt() {
            return StartAt.checkpoint(new StringBasedCheckpoint(checkpoint));
        }
    }

    record Dynamic(Function<SubscriptionModelContext, @Nullable DcbStartAt> function) implements DcbStartAt {
        public Dynamic {
            requireNonNull(function, "Dynamic DcbStartAt function cannot be null");
        }

        @Override
        public StartAt toStartAt() {
            return StartAt.dynamic(context -> {
                DcbStartAt resolved = function.apply(context);
                return resolved == null ? null : resolved.toStartAt();
            });
        }
    }
}
