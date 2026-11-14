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

package org.occurrent.springboot.reactor.shadowed.b;

import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.springboot.reactor.other.CrossPackageEvent;
import org.occurrent.springboot.reactor.shadowed.a.A;

import java.util.List;

/**
 * Declares an annotated public method of the signature of the package-private handler of {@link A}, which does not
 * override it since it is in another package.
 */
public class MidAnnotated extends A {
    public MidAnnotated(List<String> bodies) {
        super(bodies);
    }

    @Subscription(id = "mid-handler", startupMode = StartupMode.BACKGROUND)
    public void on(CrossPackageEvent event) {
        bodies.add("mid-body");
    }
}
