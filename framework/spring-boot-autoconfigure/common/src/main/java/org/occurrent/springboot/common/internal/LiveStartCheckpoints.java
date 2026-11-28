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
 * <p>
 * The live start is read when the event store bean is created, long before the subscription registers and asks the
 * checkpoint storage whether a checkpoint is stored. Only a storage that compares the checkpoint this returns with one
 * another node stored in between keeps the earlier of the two. The store starter knows which storages do that, so
 * {@link #ordersCheckpointsIn(Object)} asks it, and a first start over any other storage starts where it did before.
 * A write a {@code @PostConstruct} method makes directly to the event store before the subscription registers is then
 * missed on a first start, as in 0.34.0, on a single node as much as on several.
 */
@FunctionalInterface
public interface LiveStartCheckpoints {

    /**
     * @param liveStart The live start the store reported
     * @return The checkpoint for {@code liveStart}, or {@code null} when it carries no position to start from
     */
    @Nullable Checkpoint checkpointOf(String liveStart);

    /**
     * Whether {@code checkpointStorage} compares a checkpoint {@link #checkpointOf(String)} returns with one another
     * node stored in a first-position race and keeps the earlier one, so the starter can store it as a first checkpoint. False
     * unless the store starter says so. A storage that can't compare them would keep a checkpoint another node stored
     * after the live start was read, and the subscription would skip the events between the two.
     *
     * @param checkpointStorage The blocking or reactive {@code CheckpointStorage} bean the subscription stores its
     *                          position in
     * @return {@code true} only for a storage the store starter knows compares the checkpoint
     */
    default boolean ordersCheckpointsIn(Object checkpointStorage) {
        return false;
    }
}
