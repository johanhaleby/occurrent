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

/**
 * Specifies where a DCB subscription should start. This is the DCB counterpart to {@link StartAt}: it can only express
 * DCB start positions (a {@code dcbposition} or one of the relative starts), never a time-based stream position, so a
 * DCB subscription cannot be handed a position that belongs to a stream subscription.
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
     * Start subscribing from the beginning of the DCB sequence, replaying the whole history by {@code dcbposition} before
     * switching to live delivery. Shorthand for {@code afterPosition(0)} (DCB positions are assigned from {@code 1}, so
     * "after 0" is the first event). The history replay is performed by a catch-up-capable subscription model (the Spring
     * Boot stack includes one), so a subscription model without catch-up support may not replay past events from this
     * position.
     */
    static DcbStartAt beginning() {
        return afterPosition(0);
    }

    /**
     * Start subscribing after the given DCB sequence position, delivering events from {@code lastProcessedPosition + 1}
     * onward. This is resume semantics: {@code lastProcessedPosition} is the position you have already processed, so the
     * event at that position is not redelivered. Use {@code 0} (or {@link #beginning()}) to replay the whole history,
     * since DCB positions are assigned from {@code 1}.
     */
    static DcbStartAt afterPosition(long lastProcessedPosition) {
        return new AtPosition(lastProcessedPosition);
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
            return StartAt.subscriptionPosition(DcbSubscriptionPosition.of(lastProcessedPosition));
        }
    }
}
