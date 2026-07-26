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

package org.occurrent.springboot.blocking;

/**
 * The bean names a blocking store starter and this module have to agree on. A holder of names keeps the machinery that
 * uses them package-private, since a starter needs the name rather than the type that resolves it.
 */
public final class OccurrentBlockingBeanNames {

    /**
     * The bean name of the synchronous {@code Subscriptions} DSL declared by the auto-configuration. Resolved by name
     * (rather than by type) so it does not collide with the asynchronous {@code Subscriptions} bean, which is of the
     * same type.
     */
    public static final String SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME = "occurrentSynchronousSubscriptionDsl";

    private OccurrentBlockingBeanNames() {
    }
}
