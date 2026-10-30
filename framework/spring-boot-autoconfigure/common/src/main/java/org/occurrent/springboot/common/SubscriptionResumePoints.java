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

package org.occurrent.springboot.common;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.CheckpointCarrier;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.DcbEventMetadata;

/**
 * Where an annotated subscription resumes, for an application that stores the checkpoint of each event it handles in
 * the same transaction as its own state. Declare one as a bean, and Spring Boot asks it whenever a
 * {@code @Subscription}, {@code @StreamSubscription}, {@code @DcbSubscription}, {@code @Projection}, {@code @Saga} or
 * {@code @Snapshot} subscription starts, whatever start position the annotation gives, {@code NOW} and {@code DEFAULT}
 * included. Spring Boot never asks it about a subscription that reads a push feed or runs synchronously.
 * <p>
 * Return the checkpoint stored with the state. From the checkpoint of a live event, or of an event that a catch-up
 * replays by position, the subscription resumes after the last event that state includes. A catch-up that replays by
 * time resumes at that event's time instead, so it delivers the events with that time again, that event included. An
 * event without a time holds the checkpoint of the event before it, and resumes where that one does. When its
 * {@code catchupPhaseSortBy} doesn't start with the time ascending, an event from its first read resumes the whole
 * replay from where it started.
 * <p>
 * The checkpoint is the string the subscription delivered with that event, {@link EventMetadata#getCheckpoint()} or
 * {@link DcbEventMetadata#checkpoint()}. The string means something only to a catch-up subscription, and
 * {@link CheckpointCarrier#checkpoint()} says what the other subscription models do with it.
 * <p>
 * A non-null answer wins over the checkpoint that Spring Boot's own {@code CheckpointStorage} holds for the
 * subscription. When that stored checkpoint is newer than the answer, the events between the two are delivered again.
 * <p>
 * With {@code resumeBehavior = SAME_AS_START_AT} on a start that replays, {@code BEGINNING} or an explicit position,
 * the answer changes only where the replay starts. The subscription keeps no checkpoint in {@code CheckpointStorage}
 * once it is live, and a competing consumer model does not compete for it. A catch-up that stores checkpoints during its
 * replay still does, and deletes the last one when the replay goes live.
 * <p>
 * Spring Boot decides from the annotation, not from the answer, whether application startup waits for the subscription.
 * When {@code occurrent.subscription.mode} is {@code manual}, nothing waits. Otherwise, under the default
 * {@code startupMode}, a subscription whose annotation starts at {@code NOW} or {@code DEFAULT} waits,
 * also when the answer makes it replay. One whose annotation replays, from the beginning, a position or a time, starts in
 * the background, also when the answer makes it go live. Set {@code startupMode} on the annotation to choose.
 * <p>
 * Return {@code null} to start the subscription from its own start attributes, as it starts without this bean.
 * <p>
 * Spring Boot can ask more than once for one start, once for each subscription model that reads the start position, so
 * return the same answer each time.
 */
@NullMarked
@FunctionalInterface
public interface SubscriptionResumePoints {

    /**
     * @param subscriptionId The id of the subscription that starts.
     * @return The checkpoint to resume from, or {@code null} to start from the subscription's own start attributes.
     */
    @Nullable String resumePointFor(String subscriptionId);
}
