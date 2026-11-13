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

import org.occurrent.springboot.reactor.shadowing.CrossPackageShadowing;

import java.util.List;

/**
 * Declares its method in the package of the handler, which overrides the handler although the class it extends only
 * shadows it. The scan refuses {@link CrossPackageShadowing} and accepts this class.
 */
public class CrossPackageFixedShadowing extends CrossPackageShadowing {

    public CrossPackageFixedShadowing(List<String> bodies) {
        super(bodies);
    }

    @Override
    void on(CrossPackageEvent event) {
        bodies.add("fixed-body");
    }
}
