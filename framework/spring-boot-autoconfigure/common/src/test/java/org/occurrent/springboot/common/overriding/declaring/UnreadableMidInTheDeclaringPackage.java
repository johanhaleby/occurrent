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

package org.occurrent.springboot.common.overriding.declaring;

/**
 * Declares no method of the signature of the package-private handler of its superclass, and a method naming a class that
 * is missing at runtime, so the scan can't read its methods either.
 */
public abstract class UnreadableMidInTheDeclaringPackage extends UnreadableBaseWithPackagePrivateHandler {
    @SuppressWarnings("unused")
    void alsoUses(MissingAtRuntime missing) {
    }
}
