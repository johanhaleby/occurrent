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

import org.occurrent.annotation.StartupMode;
import org.occurrent.subscription.internal.LostStartHold;

/**
 * Subscribes a subscription that asks for {@code startupMode = BACKGROUND} so that a position MongoDB no longer has the
 * history back to holds that one subscription paused instead of stopping the application from starting. Only the
 * annotation's own value counts, not the mode {@link StartupMode#DEFAULT} resolves to, so a subscription that leaves
 * {@code startupMode} unset still fails the startup.
 */
final class BackgroundStartup {

    private BackgroundStartup() {
    }

    static void subscribe(StartupMode startupMode, Runnable subscribe) {
        if (startupMode == StartupMode.BACKGROUND) {
            LostStartHold.askedDuring(subscribe);
        } else {
            subscribe.run();
        }
    }
}
