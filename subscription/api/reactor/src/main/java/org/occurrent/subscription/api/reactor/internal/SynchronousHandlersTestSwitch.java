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

package org.occurrent.subscription.api.reactor.internal;

/**
 * Internal, not part of the public API, and meant for test support only. A model of synchronous handlers implements it
 * so a test can stop every subscription, start only the ones it checks, and keep writing. The JUnit extensions call
 * {@link #dropEventsWhileStopped()} on each such model they are given.
 */
public interface SynchronousHandlersTestSwitch {

    /**
     * From now on, a write made while the model is stopped or a subscription is paused goes through, and the stopped
     * handlers never see its events.
     */
    void dropEventsWhileStopped();
}
