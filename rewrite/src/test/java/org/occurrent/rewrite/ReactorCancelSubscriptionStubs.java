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
package org.occurrent.rewrite;

/**
 * The reactor {@code CancellableSubscriptions} and {@code DcbSubscriptionModel} as 0.34.0 declares them, with
 * {@code cancelSubscription(String)} returning {@code Mono<Void>}, handed to the parser as a compiled dependency. The
 * source under test is a 0.33.0 implementation that returns {@code void}, compiled against these.
 */
final class ReactorCancelSubscriptionStubs {

    private ReactorCancelSubscriptionStubs() {
    }

    static final String MONO = """
            package reactor.core.publisher;

            public abstract class Mono<T> {
                public static <T> Mono<T> empty() {
                    return null;
                }
            }
            """;

    static final String CANCELLABLE_SUBSCRIPTIONS = """
            package org.occurrent.subscription.api.reactor;

            import reactor.core.publisher.Mono;

            public interface CancellableSubscriptions {
                Mono<Void> cancelSubscription(String subscriptionId);
            }
            """;

    static final String DCB_SUBSCRIPTION_MODEL = """
            package org.occurrent.subscription.api.reactor;

            import reactor.core.publisher.Mono;

            public interface DcbSubscriptionModel {
                Mono<Void> cancelSubscription(String subscriptionId);
            }
            """;
}
