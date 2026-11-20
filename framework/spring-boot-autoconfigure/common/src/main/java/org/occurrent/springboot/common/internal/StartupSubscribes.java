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

package org.occurrent.springboot.common.internal;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds back the asynchronous subscriptions the startup scan registers until {@link #start()}, so no delivery runs, and
 * no handler writes through the application service, before every synchronous handler has registered. A subscription
 * registered after {@link #start()} starts at once.
 */
public final class StartupSubscribes {

    // Read and changed only while holding its own lock, like started
    private final List<Runnable> heldBack = new ArrayList<>();
    private boolean started;

    /**
     * Runs {@code start} now when {@link #start()} has run, otherwise from {@link #start()}.
     */
    public void run(Runnable start) {
        synchronized (heldBack) {
            if (!started) {
                heldBack.add(start);
                return;
            }
        }
        start.run();
    }

    /**
     * Runs what was held back, in the order it came, on the calling thread. A subscription one of them registers, from a
     * bean its replay builds, starts at once.
     */
    public void start() {
        List<Runnable> toRun;
        synchronized (heldBack) {
            started = true;
            toRun = List.copyOf(heldBack);
            heldBack.clear();
        }
        toRun.forEach(Runnable::run);
    }
}
