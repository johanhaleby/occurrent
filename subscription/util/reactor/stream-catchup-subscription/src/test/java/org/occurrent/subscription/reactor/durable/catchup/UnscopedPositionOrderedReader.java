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


package org.occurrent.subscription.reactor.durable.catchup;

import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import reactor.core.publisher.Mono;

/**
 * A fake position reader that holds its events in memory, so a position and its event become visible together and
 * the scope can read it directly.
 */
interface UnscopedPositionOrderedReader extends PositionOrderedReader, CatchupReads {

    @Override
    default Mono<CatchupReadScope> openCatchupReadScope(@Nullable String notBefore) {
        return CatchupReads.unscoped(this).openCatchupReadScope(notBefore);
    }
}
