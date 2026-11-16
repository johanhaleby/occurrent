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

package org.occurrent.subscription.mongodb.internal;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import java.util.function.Supplier;

import static org.occurrent.subscription.mongodb.internal.MongoCommons.extractCatchupMarkerGeneration;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.isLinearizableReadNotSupported;

/**
 * Reads the generation of a catch-up marker with {@code LINEARIZABLE}, and with {@code MAJORITY} from the first time
 * the server refuses {@code LINEARIZABLE} because it is not a replica set member. One instance per checkpoint
 * storage, so the storage asks with {@code LINEARIZABLE} only until that first refusal.
 */
public final class CatchupMarkerGenerationReads {
    private volatile boolean linearizableReadNotSupported = false;

    /**
     * @param linearizableRead finds the generation document with {@code LINEARIZABLE}
     * @param majorityRead     finds the generation document with {@code MAJORITY}
     * @return The generation, {@code 0} when the generation document is missing
     */
    public long read(Supplier<@Nullable Document> linearizableRead, Supplier<@Nullable Document> majorityRead) {
        if (!linearizableReadNotSupported) {
            try {
                return extractCatchupMarkerGeneration(linearizableRead.get());
            } catch (RuntimeException e) {
                if (!isLinearizableReadNotSupported(e)) {
                    throw e;
                }
                linearizableReadNotSupported = true;
            }
        }
        return extractCatchupMarkerGeneration(majorityRead.get());
    }
}
