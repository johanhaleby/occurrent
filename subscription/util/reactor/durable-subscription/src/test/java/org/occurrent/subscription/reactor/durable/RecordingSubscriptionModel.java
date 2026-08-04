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

/**
 * Records what start position it is asked to read from, and hands back no events, since the tests using it are about
 * the position and the model's own bookkeeping rather than delivery.
 */
final class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel {

    final List<StartAt> startedAt = new CopyOnWriteArrayList<>();
    Checkpoint globalCheckpoint;
    boolean failGlobalCheckpoint = false;

    RecordingSubscriptionModel(String initialGlobalCheckpoint) {
        this.globalCheckpoint = new StringBasedCheckpoint(initialGlobalCheckpoint);
    }

    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        startedAt.add(startAt);
        return Flux.never();
    }

    @Override
    public Mono<Checkpoint> globalCheckpoint() {
        return failGlobalCheckpoint
                ? Mono.error(new IllegalStateException("Cannot read the position right now"))
                : Mono.just(globalCheckpoint);
    }
}
