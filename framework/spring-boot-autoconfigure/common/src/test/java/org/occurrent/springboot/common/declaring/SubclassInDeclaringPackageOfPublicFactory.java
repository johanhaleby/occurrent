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

import org.occurrent.springboot.common.between.PublicFactoryThatDoesNotOverride;

/**
 * Declares nothing, in the package of the package-private factory, below a class in another package whose public
 * method of the same signature does not override it.
 */
public class SubclassInDeclaringPackageOfPublicFactory extends PublicFactoryThatDoesNotOverride {
}
