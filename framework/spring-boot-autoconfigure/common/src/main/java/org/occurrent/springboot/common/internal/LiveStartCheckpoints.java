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

package org.occurrent.springboot.common.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;

/**
 * Internal, not part of the public API. Turns the live start an event store's catch-up read scope reports into the
 * checkpoint an annotated subscription's first start is pinned to. A store starter declares one as a bean, since only
 * the store knows what its live start means. Without one, a first start that goes straight to live starts where it
 * did before.
 */
@FunctionalInterface
public interface LiveStartCheckpoints {

    /**
     * @param liveStart The live start the store reported
     * @return The checkpoint for {@code liveStart}, or {@code null} when it carries no position to start from
     */
    @Nullable Checkpoint checkpointOf(String liveStart);
}
