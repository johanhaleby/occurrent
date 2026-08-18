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

package org.occurrent.broker.api.blocking;

import io.cloudevents.CloudEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CloudEventSink} that records what it published, and can be told to fail every publish so a test can
 * observe how a forwarder reacts.
 */
class FakeCloudEventSink implements CloudEventSink {

    private final List<CloudEvent> published = new ArrayList<>();
    private boolean failing;

    @Override
    public void publish(CloudEvent cloudEvent) {
        if (failing) {
            throw new RuntimeException("Simulated publish failure");
        }
        published.add(cloudEvent);
    }

    List<CloudEvent> published() {
        return published;
    }

    void failOnNextPublish() {
        this.failing = true;
    }
}
