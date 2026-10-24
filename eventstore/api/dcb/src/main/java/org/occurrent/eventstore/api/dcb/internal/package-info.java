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

/**
 * Internal types shared by the DCB event stores and the DCB catch-up subscription models. These are {@code public} only
 * because the event stores and the subscription models live in separate modules. They are not part of the public API
 * and may change or be removed at any time. End users work with {@code org.occurrent.eventstore.api.dcb} and never
 * reference this package.
 */
@NullMarked
package org.occurrent.eventstore.api.dcb.internal;

import org.jspecify.annotations.NullMarked;
