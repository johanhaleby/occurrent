/*
 *
 *  Copyright 2025 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.view.testsupport

import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import java.util.*


fun nameDefined(userId: String = UUID.randomUUID().toString(), name: String) = NameDefined(UUID.randomUUID().toString(), Date(), userId, name)
fun nameChanged(userId: String, name: String) = NameWasChanged(UUID.randomUUID().toString(), Date(), userId, name)
