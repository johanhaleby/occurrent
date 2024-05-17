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
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Specifies in which position a subscription should start when subscribing to it
 */
public sealed interface StartAt {

    StartAt get(SubscriptionModelContext context);

    default boolean isNow() {
        return this instanceof Now;
    }

    default boolean isDefault() {
        return this instanceof Default;
    }

    default boolean isDynamic() {
        return this instanceof Dynamic;
    }

    final class Now implements StartAt {
        private Now() {
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

        @Override
        public StartAt get(SubscriptionModelContext ignored) {
            return this;
        }
    }

    final class Default implements StartAt {
        private Default() {
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

        @Override
        public StartAt get(SubscriptionModelContext ignored) {
            return this;
        }
    }

    final class Dynamic implements StartAt {
        public final Function<SubscriptionModelContext, StartAt> function;

        private Dynamic(Function<SubscriptionModelContext, StartAt> function) {
            requireNonNull(function, "Dynamic StartAt function cannot be null");
            this.function = function;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Dynamic.class.getSimpleName() + "[", "]")
                    .add("supplier=" + function)
                    .toString();
        }

        @Override
        public StartAt get(SubscriptionModelContext context) {
            StartAt startAt = function.apply(context);
            if (startAt instanceof Dynamic) {
                return startAt.get(context);
            }
            return startAt;
        }
    }

    final class StartAtSubscriptionPosition implements StartAt {
        public final SubscriptionPosition subscriptionPosition;

        private StartAtSubscriptionPosition(SubscriptionPosition subscriptionPosition) {
            requireNonNull(subscriptionPosition, SubscriptionPosition.class.getSimpleName() + " cannot be null");
            this.subscriptionPosition = subscriptionPosition;
        }

        @Override
        public String toString() {
            return subscriptionPosition.asString();
        }

        @Override
        public StartAt get(SubscriptionModelContext ignored) {
            return this;
        }
    }

    /**
     * Start subscribing at this moment in time
     */
    static StartAt.Now now() {
        return new Now();
    }


    /**
     * Start subscribing to the subscription model default. Typically, this would be the same as "now", but
     * subscription models may override this default behavior e.g. to start from the last stored position instead of now.
     */
    static StartAt.Default subscriptionModelDefault() {
        return new Default();
    }

    /**
     * Start subscribing to the subscription from the given subscription position
     */
    static StartAt subscriptionPosition(SubscriptionPosition subscriptionPosition) {
        return new StartAtSubscriptionPosition(subscriptionPosition);
    }

    /**
     * Create a "dynamic" start at position that may change during the life-cycle of a subscription model.
     * For example, it could return the latest subscription position from a subscription position storage.
     */
    static StartAt dynamic(Supplier<StartAt> supplier) {
        return new Dynamic(__ -> supplier.get());
    }

    /**
     * Create a "dynamic" start at position that may change during the life-cycle of a subscription model.
     * For example, it could return the latest subscription position from a subscription position storage.
     */
    static StartAt dynamic(Function<SubscriptionModelContext, StartAt> supplier) {
        return new Dynamic(supplier);
    }

    record SubscriptionModelContext(Class<?> subscriptionModelType) {
        public SubscriptionModelContext {
            requireNonNull(subscriptionModelType, "subscriptionModelType cannot be null");
        }

        public boolean hasSubscriptionModelType(Class<?> subscriptionModelType) {
            return Objects.equals(this.subscriptionModelType, subscriptionModelType);
        }
    }
}
