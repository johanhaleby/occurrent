/*
 *
 *  Copyright 2023 Johan Haleby
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
 * Enable Occurrent autoconfiguration
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface Subscription {
    /**
     * The unique identifier of the subscription.
     */
    String id();

    /**
     * Specify event types to subscribe to. Useful if you want to for example to subscribe to two events, MyEvent1 and MyEvent2 and you want them to be received as "MyEvent", which may
     * have other subtypes besides MyEvent1 and MyEvent2.
     */
    Class<?>[] eventTypes() default {};

    StartPosition startAt() default StartPosition.DEFAULT;

    long startAtTimeEpoch() default -1;
    String startAtISO8601() default "";

    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    enum StartPosition {
        BEGINNING_OF_TIME, NOW, DEFAULT
    }

    enum ResumeBehavior {
        SAME_AS_START_AT, DEFAULT
    }
}