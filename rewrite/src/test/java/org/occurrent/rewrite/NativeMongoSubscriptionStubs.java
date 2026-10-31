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
 * The 0.35.0 {@code NativeMongoSubscription} record, with three components, for the record-pattern migration tests.
 * Handed to the parser as a compiled dependency, the same reason {@link DcbMarkerVersionsStubs} exists. The source
 * under test is a 0.34.0 caller's two-binding record pattern, unchanged, meeting the 0.35.0 classpath whose canonical
 * arity it no longer matches. The {@code startFailure} component is a plain {@code Supplier<RuntimeException>} here,
 * since the rewrite module has no nullness annotations on its parser classpath and the recipe does not read them.
 */
final class NativeMongoSubscriptionStubs {

    private NativeMongoSubscriptionStubs() {
    }

    static final String NATIVE_MONGO_SUBSCRIPTION = """
            package org.occurrent.subscription.mongodb.nativedriver.blocking;

            import java.util.concurrent.CountDownLatch;
            import java.util.function.Supplier;

            public record NativeMongoSubscription(String subscriptionId, CountDownLatch subscriptionStartedLatch,
                                                  Supplier<RuntimeException> startFailure) {
            }
            """;
}
