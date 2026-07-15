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

/**
 * Specifies how a subscription or projection is resumed after the application restarts. Shared by the subscription
 * annotations ({@link Subscription}, {@link StreamSubscription}, {@link DcbSubscription}) and by {@link Projection}.
 */
public enum ResumeBehavior {
    /**
     * Always start at the configured start position. Even if a checkpoint is stored for the subscription it is ignored
     * on application restart, and the subscription resumes from the configured start position.
     */
    SAME_AS_START_AT,
    /**
     * Use the default resume behavior of the underlying subscription model. For example, with a start position of
     * beginning and {@code DEFAULT}, the subscription starts from the beginning the first time it runs, then on restart
     * it continues from the last received event (its stored checkpoint).
     */
    DEFAULT
}
