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

package org.occurrent.subscription;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Supplier;

/**
 * Specifies in which position a subscription should start when subscribing to it
 */
public abstract class StartAt {

    public abstract StartAt get();

    public boolean isNow() {
        return this instanceof Now;
    }

    public boolean isDefault() {
        return this instanceof Default;
    }

    public static class Now extends StartAt {
        private Now() {
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

        @Override
        public StartAt get() {
            return this;
        }
    }

    public static class Default extends StartAt {
        private Default() {
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

        @Override
        public StartAt get() {
            return this;
        }
    }

    public static class Dynamic extends StartAt {
        public final Supplier<StartAt> supplier;

        private Dynamic(Supplier<StartAt> supplier) {
            Objects.requireNonNull(supplier, "StartAt supplier cannot be null");
            this.supplier = supplier;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Dynamic.class.getSimpleName() + "[", "]")
                    .add("supplier=" + supplier)
                    .toString();
        }

        @Override
        public StartAt get() {
            StartAt startAt = supplier.get();
            if (startAt instanceof Dynamic) {
                return startAt.get();
            } else if (startAt == null) {
                throw new IllegalArgumentException("Dynamic \"start at\" was null which is not supported.");
            }
            return startAt;
        }
    }

    public static class StartAtSubscriptionPosition extends StartAt {
        public final SubscriptionPosition subscriptionPosition;

        private StartAtSubscriptionPosition(SubscriptionPosition subscriptionPosition) {
            Objects.requireNonNull(subscriptionPosition, SubscriptionPosition.class.getSimpleName() + " cannot be null");
            this.subscriptionPosition = subscriptionPosition;
        }

        @Override
        public String toString() {
            return subscriptionPosition.asString();
        }

        @Override
        public StartAt get() {
            return this;
        }
    }

    /**
     * Start subscribing at this moment in time
     */
    public static StartAt.Now now() {
        return new Now();
    }


    /**
     * Start subscribing to the subscription model default. Typically this would be the same as "now", but
     * subscription models may override this default behavior e.g. to start from the last stored position instead of now.
     */
    public static StartAt.Default subscriptionModelDefault() {
        return new Default();
    }

    /**
     * Start subscribing to the subscription from the given subscription position
     */
    public static StartAt subscriptionPosition(SubscriptionPosition subscriptionPosition) {
        return new StartAtSubscriptionPosition(subscriptionPosition);
    }

    /**
     * Create a "dynamic" start at position that may change during the life-cycle of a subcription model.
     * For example, it could return the latest subscription position from a subscription position storage.
     */
    public static StartAt dynamic(Supplier<StartAt> supplier) {
        return new Dynamic(supplier);
    }
}
