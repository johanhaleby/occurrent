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
 * Injects the stream version of the delivered event into a subscription handler parameter, so a handler does not have
 * to declare an {@link org.occurrent.dsl.subscription.EventMetadata} parameter and call {@code getStreamVersion()}. For
 * example:
 *
 * <pre lang="java">
 * &#64;StreamSubscription(id = "mySubscription")
 * void mySubscription(MyDomainEvent event, &#64;StreamVersion long streamVersion) { .. }
 * </pre>
 *
 * <p>
 * The annotated parameter must be of type {@code long} or {@link Long}. It may be combined with a {@link StreamId}
 * parameter and an {@link org.occurrent.dsl.subscription.EventMetadata} parameter, in any order.
 * </p>
 * <p>
 * Usable on {@link Subscription}, {@link StreamSubscription}, and {@code @SynchronousSubscription} handlers. On a
 * {@link DcbSubscription} handler it is rejected at startup, because a DCB handler's stream version is an internal
 * per-partition counter rather than a domain stream version.
 * </p>
 * <p>
 * Note that on the capability-agnostic {@link Subscription}, an event that was DCB-appended carries the internal
 * per-partition counter rather than a domain stream version. The value is always present, it is just an internal
 * counter for a DCB-sourced event, which is the same semantics
 * {@link org.occurrent.dsl.subscription.EventMetadata#getStreamVersion()} has there.
 * </p>
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamVersion {
}
