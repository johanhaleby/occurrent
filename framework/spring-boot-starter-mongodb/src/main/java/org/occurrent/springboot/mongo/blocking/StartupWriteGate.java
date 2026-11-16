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

package org.occurrent.springboot.mongo.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.springboot.common.internal.StartupGap;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;

// Refuses a write through the application service until startup has registered the synchronous handlers, before the
// model's own refusals are asked, so the application service needs no startup logic of its own
record StartupWriteGate(SynchronousEventDispatcher delegate, List<StartupGap> startupGaps) implements SynchronousEventDispatcher {

    // One gap per annotation post-processor in the context, which is none when annotation processing is off
    static SynchronousEventDispatcher refusingWritesUntilStarted(SynchronousEventDispatcher dispatcher, ObjectProvider<StartupGap> startupGaps) {
        List<StartupGap> gaps = startupGaps.orderedStream().toList();
        return gaps.isEmpty() ? dispatcher : new StartupWriteGate(dispatcher, gaps);
    }

    @Override
    public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
        delegate.dispatch(writtenCloudEvents, transactional);
    }

    @Override
    public boolean hasSubscriptions() {
        return delegate.hasSubscriptions();
    }

    @Override
    public WriteCheck beforeWrite() {
        if (startupGaps.stream().anyMatch(StartupGap::isOpen)) {
            throw WriteRefusedException.beforeStartupCompleted();
        }
        return delegate.beforeWrite();
    }
}
