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

package org.occurrent.springboot.blocking.shadowing;

import org.occurrent.springboot.blocking.other.CrossPackageEvent;
import org.occurrent.springboot.blocking.other.CrossPackagePackagePrivateHandler;

import java.util.List;

/**
 * Declares an unannotated package-private method of the signature of the package-private handler of
 * {@link CrossPackagePackagePrivateHandler}, from a package other than the one that declares it. It does not
 * override the handler, so the scan refuses this class.
 */
public abstract class CrossPackageShadowing extends CrossPackagePackagePrivateHandler {

    protected CrossPackageShadowing(List<String> bodies) {
        super(bodies);
    }

    void on(CrossPackageEvent event) {
        bodies.add("shadowing-body");
    }
}
