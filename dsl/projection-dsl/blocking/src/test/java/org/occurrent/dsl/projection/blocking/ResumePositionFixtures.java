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
package org.occurrent.dsl.projection.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;

import java.net.URI;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.fail;

/**
 * Shared by the tests of what a catch-up marker means (ADR 179).
 */
final class ResumePositionFixtures {

    private ResumePositionFixtures() {
    }

    record Counted(String eventId) {
    }

    static CloudEventConverter<Counted> converter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                return CloudEventBuilder.v1().withId(domainEvent.eventId()).withSource(URI.create("urn:occurrent:test")).withType("Counted").build();
            }

            @Override
            public Counted toDomainEvent(CloudEvent cloudEvent) {
                return new Counted(cloudEvent.getId());
            }

            @Override
            public String getCloudEventType(Class<? extends Counted> type) {
                return "Counted";
            }
        };
    }

    // Records each call the way a view sees it, so a test can say which callbacks reached it.
    static final class RecordingReplayAwareView implements MaterializedView<Counted>, ReplayAware {
        final List<String> calls = new java.util.concurrent.CopyOnWriteArrayList<>();
        volatile Runnable onUpdate = () -> {
        };

        @Override
        public void update(Counted event) {
            calls.add("update:" + event.eventId());
            onUpdate.run();
        }

        @Override
        public void update(EventMetadata metadata, Counted event) {
            update(event);
        }

        @Override
        public void replayStarted() {
            calls.add("replayStarted");
        }

        @Override
        public void replayCompleted() {
            calls.add("replayCompleted");
        }

        @Override
        public void replayAbandoned() {
            calls.add("replayAbandoned");
        }

        @Override
        public void alreadyDeliveredByReplay(EventMetadata metadata) {
            calls.add("alreadyDeliveredByReplay");
        }
    }

    // acceptAndWait(..) returns only once the catch-up has folded the event, so an event fed ahead of the catch-up
    // comes from its own thread, already parked in the buffer when this returns.
    static FutureTask<Void> bufferedLiveDelivery(Runnable acceptAndWait) {
        FutureTask<Void> calling = new FutureTask<>(acceptAndWait, null);
        Thread thread = new Thread(calling, "live-delivery");
        thread.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.WAITING) {
            if (!thread.isAlive()) {
                fail("acceptAndWait(..) ended without waiting");
            }
            if (System.nanoTime() > deadline) {
                fail("acceptAndWait(..) did not start waiting within 5 seconds, it is " + thread.getState());
            }
            Thread.onSpinWait();
        }
        return calling;
    }
}
