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

package org.occurrent.springboot.common.internal;

import java.util.ArrayList;
import java.util.List;

/**
 * Internal, not part of the public API. The time between the first bean the context creates and the end of the
 * annotation post-processor's startup scan, which registers every handler. A synchronous handler has no replay, so a
 * write it misses it never sees.
 * <p>
 * The starter asks {@link #admitsWrite()} before every write through the application service and refuses the write
 * when it answers {@code false}. A context with both the blocking and the reactor post-processor has two gaps, and a
 * write is refused while either is closed. So a post-processor refuses a bean whose synchronous handler isn't
 * registered only once every gap admits writes. Until then it registers the handler, counted on every gap still
 * closed, so none of them opens before the handler is subscribed.
 */
public interface StartupGap {

    /**
     * @return {@code false} until the startup scan has registered every handler, then {@code true} for good.
     */
    boolean admitsWrite();

    /**
     * Runs the asynchronous subscriptions the startup scan held back. Called once every gap in the context admits writes,
     * and a second call runs nothing.
     */
    void startHeldBack();

    /**
     * Called before another gap's post-processor registers a handler it can no longer refuse, since this gap may still
     * be closed.
     *
     * @return Whether the registration is counted, in which case this gap stays closed until
     * {@link #registrationEnds()}, and {@code false} once it admits writes
     */
    boolean registrationStarts();

    /**
     * Called once a registration that {@link #registrationStarts()} counted has ended, whether or not it succeeded.
     */
    void registrationEnds();

    /**
     * Runs the held-back subscriptions of every gap in the context, {@code self} included, when every one of them admits
     * writes, and otherwise nothing. A context with both the blocking and the reactor post-processor has two gaps, and a
     * write through either application service is refused while one of them is closed, so a replay that writes through
     * one may start only once both scans have opened theirs. The post-processor whose gap opens last starts them all.
     *
     * @param self              The gap whose scan just opened it
     * @param gapsInTheContext  Every gap the context holds, in the order their held-back subscriptions start
     */
    static void startHeldBackOnceEveryGapAdmitsWrites(StartupGap self, List<StartupGap> gapsInTheContext) {
        List<StartupGap> gaps = new ArrayList<>(gapsInTheContext);
        if (!gaps.contains(self)) {
            gaps.add(self);
        }
        if (gaps.stream().allMatch(StartupGap::admitsWrite)) {
            gaps.forEach(StartupGap::startHeldBack);
        }
    }
}
