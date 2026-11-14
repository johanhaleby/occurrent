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

package org.occurrent.springboot.common.declaring;

import org.occurrent.annotation.Subscription;

/**
 * The package-private handler the shapes in this package and {@code between} start from. Its package differs from the
 * package of the test class, so a subclass there never overrides it.
 */
public abstract class PackagePrivateHandlerBase {

    @Subscription(id = "h")
    void on(String event) {
    }
}
