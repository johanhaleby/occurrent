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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records each callback in the order it arrived, as an {@link Call}, so a test asserts on the whole sequence.
 */
public final class RecordingRabbitMqSinkListener implements RabbitMqSinkListener {

    public record Call(String callback, RabbitMqDestination destination, Duration publishTime, Throwable failure) {
    }

    private final List<Call> calls = new CopyOnWriteArrayList<>();

    public List<Call> calls() {
        return calls;
    }

    public List<String> callbacks() {
        return calls.stream().map(Call::callback).toList();
    }

    @Override
    public void published(RabbitMqDestination destination, Duration publishTime) {
        calls.add(new Call("published", destination, publishTime, null));
    }

    @Override
    public void unroutable(RabbitMqDestination destination) {
        calls.add(new Call("unroutable", destination, null, null));
    }

    @Override
    public void failed(RabbitMqDestination destination, Throwable failure) {
        calls.add(new Call("failed", destination, null, failure));
    }
}
