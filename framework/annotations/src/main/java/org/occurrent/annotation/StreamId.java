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

import java.lang.annotation.*;

/**
 * Injects the stream id of the delivered event into a subscription handler parameter, so a handler does not have to
 * declare an {@link org.occurrent.cloudevents.EventMetadata} parameter and call {@code getStreamId()}. For example:
 *
 * <pre lang="java">
 * &#64;StreamSubscription(id = "mySubscription")
 * void mySubscription(MyDomainEvent event, &#64;StreamId String streamId) { .. }
 * </pre>
 *
 * <p>
 * The annotated parameter must be of type {@link String}. It may be combined with a {@link StreamVersion} parameter and
 * an {@link org.occurrent.cloudevents.EventMetadata} parameter, in any order.
 * </p>
 * <p>
 * Usable on {@link Subscription}, {@link StreamSubscription}, and {@code @SynchronousSubscription} handlers. On a
 * {@link DcbSubscription} handler it is rejected at startup, because a DCB handler's stream id is an internal partition
 * id rather than a domain stream id.
 * </p>
 * <p>
 * Note that on the capability-agnostic {@link Subscription}, an event that was DCB-appended carries the internal
 * generated partition id (for example {@code dcb:partition:37}) rather than a domain stream id. The value is always
 * present, it is just an internal id for a DCB-sourced event, which is the same semantics
 * {@link org.occurrent.cloudevents.EventMetadata#getStreamId()} has there.
 * </p>
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamId {
}
