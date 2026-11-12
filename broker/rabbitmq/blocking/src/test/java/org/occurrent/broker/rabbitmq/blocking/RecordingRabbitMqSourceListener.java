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
package org.occurrent.broker.rabbitmq.blocking;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records each callback in the order it arrived, as an {@link Call}, so a test asserts on the whole sequence.
 */
public final class RecordingRabbitMqSourceListener implements RabbitMqSourceListener {

    public record Call(String callback, String queue, @Nullable Throwable failure) {
    }

    private final List<Call> calls = new CopyOnWriteArrayList<>();

    public List<Call> calls() {
        return calls;
    }

    public List<String> callbacks() {
        return calls.stream().map(Call::callback).toList();
    }

    @Override
    public void delivered(String queue) {
        calls.add(new Call("delivered", queue, null));
    }

    @Override
    public void failed(String queue, @Nullable Throwable failure) {
        calls.add(new Call("failed", queue, failure));
    }

    @Override
    public void parked(String queue) {
        calls.add(new Call("parked", queue, null));
    }

    @Override
    public void redelivered(String queue) {
        calls.add(new Call("redelivered", queue, null));
    }
}
