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
 * Internal implementation types for the filter layer. These are {@code public} only because the saga DSL and the
 * subscription annotation support live in separate modules and call them directly. They are not part of the public API
 * and may change or be removed at any time. End users work with {@code org.occurrent.filter} and never reference this
 * package.
 * <p>
 * {@link org.occurrent.filter.internal.EventTypeExpansion#expandWhatCanBeFound} is best-effort. It returns only the
 * concrete types reflection can find and, unlike {@code expand}, does not refuse a hierarchy it cannot find all of. It
 * must not be mistaken for a complete expansion.
 */
@NullMarked
package org.occurrent.filter.internal;

import org.jspecify.annotations.NullMarked;
