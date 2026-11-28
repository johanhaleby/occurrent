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

package org.occurrent.springboot.mongo.reactor;

import io.cloudevents.CloudEvent;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.springboot.common.internal.StartupGap;
import org.springframework.beans.factory.ObjectProvider;
import reactor.core.publisher.Mono;

import java.util.List;

// Refuses a write through the application service while it could miss a synchronous handler that registers later, before the
// model's own refusals are asked, so the application service needs no startup logic of its own. The application
// service turns the throw into a failed Mono.
record StartupWriteGate(ReactiveSynchronousEventDispatcher delegate, List<StartupGap> startupGaps) implements ReactiveSynchronousEventDispatcher {

    // One gap per annotation post-processor in the context, which is none when annotation processing is off
    static ReactiveSynchronousEventDispatcher refusingWritesUntilStarted(ReactiveSynchronousEventDispatcher dispatcher, ObjectProvider<StartupGap> startupGaps) {
        List<StartupGap> gaps = startupGaps.orderedStream().toList();
        return gaps.isEmpty() ? dispatcher : new StartupWriteGate(dispatcher, gaps);
    }

    @Override
    public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
        return delegate.dispatch(writtenCloudEvents, transactional);
    }

    @Override
    public boolean hasSubscriptions() {
        return delegate.hasSubscriptions();
    }

    @Override
    public WriteCheck beforeWrite() {
        if (!startupGaps.stream().allMatch(StartupGap::admitsWrite)) {
            throw WriteRefusedException.beforeStartupCompleted();
        }
        return delegate.beforeWrite();
    }
}
