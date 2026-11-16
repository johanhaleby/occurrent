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
 * The starter asks {@link #closeBeforeWrite()} before every write through the application service.
 */
public interface StartupGap {

    /**
     * Registers every synchronous handler the context declares and has not registered yet, on the calling thread, when
     * startup has not registered them all. A handler whose bean is lazy, or whose bean definition hides the annotation
     * behind its declared type, is left out. Returns at once after startup.
     *
     * @throws org.springframework.beans.factory.BeanCurrentlyInCreationException (possibly wrapped in a
     *                                                                            {@link org.springframework.beans.factory.BeanCreationException})
     *                                                                            when a handler's bean can't be created
     *                                                                            before the write, because the write is
     *                                                                            made while that bean, or a bean it
     *                                                                            needs, is still being created
     */
    void closeBeforeWrite();
}
