/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.util.predicate;

import io.cloudevents.CloudEvent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * A stateful predicate that matches every N event. I.e. if {@code n} is set to 10 then this predicate will return {@code true} for every 10th event.
 * <p>
 * One instance keeps one count. A subscription model configured with one instance for all of its subscriptions calls
 * {@link #forOneSubscription(Predicate)} for each subscription it starts, so every subscription counts its own events.
 */
public class EveryN implements Predicate<CloudEvent> {
    private final int n;
    private final AtomicInteger state = new AtomicInteger(1);


    public EveryN(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be greater than or equal to 1");
        }
        this.n = n;
    }

    /**
     * @return An instance of {@link EveryN} that returns {@code true} for every event
     */
    public static EveryN everyEvent() {
        return new EveryN(1);
    }

    /**
     * @return An instance of {@link EveryN} that returns {@code true} for every {@code n} event.
     */
    public static EveryN every(int n) {
        return new EveryN(n);
    }

    /**
     * The predicate to use for one subscription, when {@code configured} is shared by every subscription of a model.
     * An {@link EveryN} gives a new instance with the same {@code n} and a count of its own, since a shared count would
     * let one subscription use up the matches of another. Any other predicate, a subclass of {@link EveryN} included,
     * is returned as it is.
     *
     * @param configured The predicate a subscription model was configured with.
     * @return A predicate that counts this subscription's events only, or {@code configured} if it is not an {@link EveryN}.
     */
    public static Predicate<CloudEvent> forOneSubscription(Predicate<CloudEvent> configured) {
        return configured.getClass() == EveryN.class ? new EveryN(((EveryN) configured).n) : configured;
    }

    @Override
    public boolean test(CloudEvent __) {
        return state.updateAndGet(operand -> {
            if (operand % n == 0) {
                return 1;
            } else {
                return operand + 1;
            }
        }) == 1;
    }
}
