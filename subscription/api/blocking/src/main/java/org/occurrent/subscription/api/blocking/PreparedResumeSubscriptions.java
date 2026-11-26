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

package org.occurrent.subscription.api.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;

import java.util.Optional;

/**
 * A subscription model whose resume reads storage, and which can do that reading apart from the resume.
 * {@link #prepareResume(String)} does the reading, and {@link #resumeAsPrepared(String, PreparedResume)} resumes from what
 * was read without reading anything. A model that wraps such a model, such as a catch-up subscription model, can
 * prepare outside its own locks and resume under them, so a storage that is slow to answer, or never answers, holds up
 * only the subscription it reads for. A catch-up subscription model prepares under its lock when the resume is made on
 * a thread that already holds it, such as from code a start runs.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface PreparedResumeSubscriptions extends SubscriptionModelCapability {

    /**
     * Read what a resume of {@code subscriptionId} needs from storage, without resuming it. The result is good for one
     * {@link #resumeAsPrepared(String, PreparedResume)} of the same subscription on this model, and only until what it
     * read changes.
     *
     * @param subscriptionId The id of the subscription to prepare a resume for.
     * @return What the resume needs, to pass to {@link #resumeAsPrepared(String, PreparedResume)}.
     */
    PreparedResume prepareResume(String subscriptionId);

    /**
     * Resume a paused subscription as {@code prepared} says, without reading storage, where
     * {@link SubscriptionModelLifeCycle#resumeSubscription(String)} would have resumed it when the preparation read. If
     * a change by this model to what the preparation read has returned since, or a call for this subscription that can
     * change it is under way on this model, nothing is resumed, the result is empty, and the caller prepares again. A
     * change made since by another node, or by another model instance over the same storage, isn't seen, so the
     * subscription resumes from what was read. Neither is a write this model still makes for an event it delivered
     * before the subscription was paused, as {@link SubscriptionModelLifeCycle#resumeSubscription(String)} wouldn't see
     * one either.
     *
     * @param subscriptionId The id of the subscription to resume.
     * @param prepared       What {@link #prepareResume(String)} returned for {@code subscriptionId} on this model.
     * @return The resumed subscription, or empty when the caller should prepare again.
     * @throws UnknownSubscriptionException        If this subscription model has no subscription with that id.
     * @throws SubscriptionAlreadyRunningException If the subscription is already running.
     * @throws IllegalArgumentException            If {@code prepared} doesn't come from this model's {@link #prepareResume(String)} for {@code subscriptionId}.
     */
    Optional<Subscription> resumeAsPrepared(String subscriptionId, PreparedResume prepared);

    /**
     * What {@link #prepareResume(String)} read, opaque to the caller.
     */
    interface PreparedResume {
    }

    /**
     * {@code subscriptionModel} when it can read for a resume apart from the resume. A {@link SubscriptionModelWrapper} that doesn't implement
     * this is not looked past, because a resume through the model beneath it would go around the state the wrapper keeps
     * for the subscription.
     *
     * @param subscriptionModel A {@link Subscribable}, a {@link SubscriptionModelLifeCycle}, a whole {@link SubscriptionModel},
     *                          or a {@link SubscriptionModelWrapper} around one of these.
     * @return {@code subscriptionModel} as a model that splits a resume, or empty if it doesn't implement this.
     */
    static Optional<PreparedResumeSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(PreparedResumeSubscriptions.class);
    }
}
