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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

// Package-private on purpose. A member type nested in an interface is unconditionally public, so the singleton
// backing PushObserver.noop() lives in a top-level class instead, where it can stay out of the public API.
@NullMarked
final class PushObserverNoop implements PushObserver {

    static final PushObserver INSTANCE = new PushObserverNoop();

    private PushObserverNoop() {
    }

    @Override
    public void observe(CloudEvent cloudEvent, boolean matched) {
    }
}
