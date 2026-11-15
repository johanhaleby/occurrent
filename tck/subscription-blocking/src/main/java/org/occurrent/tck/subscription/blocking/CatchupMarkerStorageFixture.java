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

package org.occurrent.tck.subscription.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.api.blocking.CatchupMarkerStorage;

/**
 * What a {@link CatchupMarkerStorage} implementation hands {@link CatchupMarkerStorageConformance}.
 * <p>
 * A fixture is created for every test method, and the storage it returns must hold no checkpoints and no
 * generations.
 */
@NullMarked
public interface CatchupMarkerStorageFixture {

    /**
     * The storage under test, holding no checkpoints and no generations.
     */
    CatchupMarkerStorage catchupMarkerStorage();

    /**
     * Releases whatever the fixture opened. Called after every test method, including a failing one.
     */
    default void close() {
    }
}
