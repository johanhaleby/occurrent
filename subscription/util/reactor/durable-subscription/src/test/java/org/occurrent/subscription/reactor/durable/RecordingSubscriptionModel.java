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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Records what start position it is asked to read from, and hands back no events, since the tests using it are about
 * the position and the model's own bookkeeping rather than delivery.
 */
final class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel {

    final List<StartAt> startedAt = new CopyOnWriteArrayList<>();
    /**
     * Counts the reads at subscription time rather than at assembly time, since that is when a read costs anything and
     * what a caching model has to avoid doing twice.
     */
    final AtomicInteger globalCheckpointReads = new AtomicInteger();
    @Nullable Checkpoint globalCheckpoint;
    boolean failGlobalCheckpoint = false;
    /**
     * How many reads still fail before the rest succeed. A budget rather than a flag, so a test can prove a
     * registration that read once and failed is not read again by letting the second read succeed and finding it never
     * happened.
     */
    int failGlobalCheckpointTimes = 0;

    RecordingSubscriptionModel(String initialGlobalCheckpoint) {
        this.globalCheckpoint = new StringBasedCheckpoint(initialGlobalCheckpoint);
    }

    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        startedAt.add(startAt);
        return Flux.never();
    }

    /**
     * Answers empty for a null checkpoint, which is the unresolvable problem
     * {@link CheckpointAwareSubscriptionModel#globalCheckpoint()} documents rather than a position.
     */
    @Override
    public Mono<Checkpoint> globalCheckpoint() {
        return Mono.defer(() -> {
            globalCheckpointReads.incrementAndGet();
            if (failGlobalCheckpoint || failGlobalCheckpointTimes-- > 0) {
                return Mono.error(new IllegalStateException("Cannot read the position right now"));
            }
            return Mono.justOrEmpty(globalCheckpoint);
        });
    }
}
