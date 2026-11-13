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

package org.occurrent.springboot.reactor.bridged;

import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.springboot.reactor.other.CrossPackageEvent;

import java.util.List;

/**
 * A package-private class with a public handler. javac gives {@link PublicBridgedSubscriber} a bridge method for it,
 * and the scan reads the handler from that bridge.
 */
class PackagePrivateBridgedHandler {
    private final List<String> bodies;

    PackagePrivateBridgedHandler(List<String> bodies) {
        this.bodies = bodies;
    }

    @Subscription(id = "bridged-handler", startupMode = StartupMode.BACKGROUND)
    public void on(CrossPackageEvent event) {
        bodies.add("bridged-body");
    }
}
