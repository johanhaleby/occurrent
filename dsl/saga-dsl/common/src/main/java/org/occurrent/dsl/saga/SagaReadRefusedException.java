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

package org.occurrent.dsl.saga;

/**
 * Thrown by {@link SagaStateStoreSkippedInputs#findWithFirstSkippedInput(String)} when the store can't read the
 * instance and its first record from one moment where it is called, before it reads anything.
 * {@code SpringMongoSagaStateStore} throws it inside a MongoDB transaction of the caller's, and inside a Spring
 * transaction of another resource that synchronizes its resources, as one does by default, since a template with
 * {@code SessionSynchronization.ALWAYS} starts a MongoDB transaction in it at the release's first write.
 * <p>
 * {@code SagaSubscription.release(sagaId)} throws it as it is, so a release inside a caller's transaction reads and
 * writes nothing. A store of your own that throws a plain {@link IllegalStateException} instead gets the release's
 * decoding checks first, which read the instance again.
 */
public class SagaReadRefusedException extends IllegalStateException {
    public SagaReadRefusedException(String message) {
        super(message);
    }
}
