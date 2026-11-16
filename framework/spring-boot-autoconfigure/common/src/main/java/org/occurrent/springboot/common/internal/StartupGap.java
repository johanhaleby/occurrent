/*
 *
 *  Copyright 2026 Johan Haleby
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

package org.occurrent.springboot.common.internal;

/**
 * Internal, not part of the public API. The time between the first bean the context creates and the moment the
 * annotation post-processor has registered every synchronous handler. A write made then, from a {@code @PostConstruct}
 * method for example, reaches only the synchronous handlers already registered, and a handler that misses a write never
 * sees it.
 * <p>
 * The starter refuses every write through the application service while {@link #isOpen()} returns {@code true}.
 */
public interface StartupGap {

    /**
     * @return {@code true} until the startup scan has registered the synchronous handlers, then {@code false} for good.
     */
    boolean isOpen();
}
