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

package org.occurrent.springboot.blocking.shadowed.b;

import org.occurrent.springboot.blocking.other.CrossPackageEvent;
import org.occurrent.springboot.blocking.shadowed.a.A;

import java.util.List;

/**
 * Declares a private, unannotated method of the signature of the handler of {@link A}, which neither overrides it nor
 * is visible to a subclass.
 */
public class MidPrivate extends A {
    public MidPrivate(List<String> bodies) {
        super(bodies);
    }

    @SuppressWarnings("unused")
    private void on(CrossPackageEvent event) {
        bodies.add("mid-private-body");
    }
}
