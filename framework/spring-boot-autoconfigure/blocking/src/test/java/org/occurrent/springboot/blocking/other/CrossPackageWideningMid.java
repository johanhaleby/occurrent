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

package org.occurrent.springboot.blocking.other;

import java.util.List;

/**
 * Widens the package-private handler of {@link CrossPackagePackagePrivateHandler} to public from the package that
 * declares it, with no annotation of its own.
 */
public abstract class CrossPackageWideningMid extends CrossPackagePackagePrivateHandler {

    protected CrossPackageWideningMid(List<String> bodies) {
        super(bodies);
    }

    @Override
    public void on(CrossPackageEvent event) {
        bodies.add("widened-mid-body");
    }
}
