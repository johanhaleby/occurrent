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

package org.occurrent.annotation;

/**
 * What a {@code @Saga} does with an event it cannot recognise a redelivery of. A saga tells a redelivered event from a
 * new one by its {@code streamid} together with its {@code streamversion}, or by its {@code position}. An event
 * carrying none of those leaves nothing to compare against, so the reaction runs again on every redelivery and issues
 * its commands again. Applies only to a {@link Source#PUSH} saga, since the event store's own events always carry the
 * metadata, and so does the replay in front of a push feed.
 */
public enum RedeliveryDetection {
    /**
     * The default. An event with no {@code streamid}/{@code streamversion} and no {@code position} is refused rather
     * than reacted to, so the feed that dropped the metadata announces itself instead of quietly costing the saga its
     * redelivery protection. The event is not acknowledged, which is what makes it visible. A broker hands the same
     * message over more than once as a matter of course, so it offers this one again and the saga refuses it again
     * until the listener is fixed or this attribute is set to {@link #BEST_EFFORT}.
     */
    REQUIRED,
    /**
     * Take the event anyway, logging a warning the first time. Choose this for a feed that genuinely carries no such
     * metadata, which is usually another application's broker feeding a {@link Catchup#NONE} saga, and only when every
     * command the saga issues is safe to receive more than once, since a redelivery runs every reaction again. This is
     * the same duplication {@link #REQUIRED} exists to refuse, accepted knowingly rather than quietened.
     */
    BEST_EFFORT
}
