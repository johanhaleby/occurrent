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
import java.util.function.Supplier;

/**
 * Records the name of each callback it receives, then throws what {@code toThrow} supplies, from every callback or
 * only from the ones named. Records before it throws, so a test sees that a callback was reached.
 */
public final class ThrowingRabbitMqSourceListener implements RabbitMqSourceListener {

    private final Supplier<Throwable> toThrow;
    private final List<String> throwingCallbacks;
    private final List<String> callbacks = new CopyOnWriteArrayList<>();

    public ThrowingRabbitMqSourceListener(Supplier<Throwable> toThrow, String... throwingCallbacks) {
        this.toThrow = toThrow;
        this.throwingCallbacks = List.of(throwingCallbacks);
    }

    public static ThrowingRabbitMqSourceListener fromEveryCallback(Supplier<Throwable> toThrow) {
        return new ThrowingRabbitMqSourceListener(toThrow, "delivered", "failed", "parked", "redelivered");
    }

    public List<String> callbacks() {
        return callbacks;
    }

    @Override
    public void delivered(String queue) {
        record("delivered");
    }

    @Override
    public void failed(String queue, @Nullable Throwable failure) {
        record("failed");
    }

    @Override
    public void parked(String queue) {
        record("parked");
    }

    @Override
    public void redelivered(String queue) {
        record("redelivered");
    }

    private void record(String callback) {
        callbacks.add(callback);
        if (throwingCallbacks.contains(callback)) {
            sneakyThrow(toThrow.get());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
        throw (T) throwable;
    }
}
