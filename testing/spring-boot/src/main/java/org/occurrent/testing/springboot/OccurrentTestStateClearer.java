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

package org.occurrent.testing.springboot;

/**
 * Marks the bean that {@link OccurrentTestingConfiguration} and {@link OccurrentReactorTestingConfiguration} pass to
 * a subscriptions extension's {@code clearingStateWith(..)} once {@link EnableOccurrentTesting#clearState()} has
 * wired one in.
 * <p>
 * A plain {@code Runnable} bean would do the same job but cannot be looked up without also matching whatever
 * unrelated {@code Runnable} bean an application already has in its context. This interface exists only to be that
 * unambiguous lookup key. A {@code clearState = true} store integration, such as
 * {@code OccurrentMongoFlushTestingConfiguration}, is what implements it.
 */
@FunctionalInterface
interface OccurrentTestStateClearer extends Runnable {
}
