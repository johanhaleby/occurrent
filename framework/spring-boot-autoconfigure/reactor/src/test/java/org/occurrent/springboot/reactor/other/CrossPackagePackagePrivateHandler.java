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

package org.occurrent.springboot.reactor.other;

import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;

import java.util.List;

/**
 * Declared outside {@code org.occurrent.springboot.reactor}, so a subclass in that package inherits a package-private
 * handler from another package, which a CGLIB proxy of the subclass cannot override.
 */
public abstract class CrossPackagePackagePrivateHandler {
    protected final List<String> bodies;

    protected CrossPackagePackagePrivateHandler(List<String> bodies) {
        this.bodies = bodies;
    }

    @Subscription(id = "cross-package-package-private-handler", startupMode = StartupMode.BACKGROUND)
    void on(CrossPackageEvent event) {
        bodies.add("package-private-body");
    }
}
