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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.PositionAwareCloudEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The data-access seam {@link PositionCatchupPipeline} replays through. A stream store and a DCB store both expose a
 * global {@code position} sequence, so the pipeline only needs a window read and a head read, and stays free of
 * either store's specific query type.
 */
@NullMarked
public interface CatchupReader {

    /**
     * Reads events in {@code (fromExclusive, toInclusive]}, in position order, already wrapped as a
     * {@link PositionAwareCloudEvent} so a durable model layered on top can persist replay progress.
     */
    Flux<CloudEvent> readWindow(long fromExclusive, long toInclusive);

    /**
     * The store's current position high-watermark.
     */
    Mono<Long> currentHead();
}
