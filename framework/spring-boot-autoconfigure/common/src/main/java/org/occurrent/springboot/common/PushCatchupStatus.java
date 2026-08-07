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

package org.occurrent.springboot.common;

import org.jspecify.annotations.NullMarked;

import java.util.Map;
import java.util.Objects;

/**
 * Where each {@code source = PUSH} projection and saga is in its catch-up, so an application can tell a read model that
 * is still filling from one that is ready to serve.
 * <p>
 * This exists because {@code startupMode = BACKGROUND} starts the application while the replay is still running. Nobody
 * waits for that replay, which is the whole point, so neither its progress nor its failure has anywhere to be returned
 * or thrown. Inject this bean and check it from a health indicator or a readiness probe. A failure is also logged at
 * {@code ERROR}, so an application that never injects this still sees it in the logs.
 * <p>
 * The states are exhaustive, which is what makes a readiness probe expressible:
 * <pre>{@code
 * switch (status.of("orders")) {
 *     case CatchingUp ignored -> notReadyYet();
 *     case Live ignored       -> ready();
 *     case NotStarted ignored -> notStartedYet();
 *     case Failed failed      -> unhealthy(failed.cause());
 *     case Unknown ignored    -> notRegisteredHere();
 * }
 * }</pre>
 * Where there is a subscription model to ask, {@link CatchingUp}, {@link Live} and {@link NotStarted} are derived from
 * it rather than recorded, so a model that was stopped and started again, replaying its history a second time, reports
 * {@link CatchingUp} again rather than staying at whatever it reached the first time. A {@code DomainEventFeed} cannot
 * be asked, so those ids carry a recorded state instead.
 * <p>
 * Read-only on purpose, mirroring the DSL module's {@code SagaInstancesRegistry} / {@code SagaInstancesRegistryImpl}
 * split: registering a source and recording its progress is the framework's job and has no legitimate caller in
 * application code, so those methods are not on this interface at all rather than being public with a comment asking
 * callers not to use them. They live on {@link PushCatchupStatusImpl}, which the {@code @Projection} and {@code @Saga}
 * registrars look up by that concrete type. An application keeps injecting this interface by its established name;
 * only the writer moved.
 */
@NullMarked
public interface PushCatchupStatus {

    /**
     * Where one subscription id is in its catch-up. Sealed, so a caller can switch over every case, and only
     * {@link Failed} carries a cause.
     */
    sealed interface CatchupStatus permits NotStarted, CatchingUp, Live, Failed, Unknown {
        /**
         * @return The subscription or projection id this status is about.
         */
        String id();
    }

    /**
     * The projection or saga is registered but its subscription has not been started, so it is neither replaying nor
     * taking live events. That is what {@code occurrent.subscription.mode = manual} leaves it as until the application
     * starts it, and what a stopped subscription model leaves it as until something starts it again.
     * <p>
     * Distinct from {@link Unknown}, which is an id nothing here registered at all, and from {@link CatchingUp}, which
     * is working through history on its own and will reach {@link Live} without anyone intervening. This one will not.
     */
    record NotStarted(String id) implements CatchupStatus {
        public NotStarted {
            Objects.requireNonNull(id, "id cannot be null");
        }
    }

    /**
     * The catch-up is replaying history. Whatever this id projects into is incomplete, so a read model behind it is
     * not ready to serve.
     */
    record CatchingUp(String id) implements CatchupStatus {
        public CatchingUp {
            Objects.requireNonNull(id, "id cannot be null");
        }
    }

    /**
     * The catch-up finished and handed over, so this id is taking live events. Includes {@code catchup = NONE}, which
     * has no history to replay and is live from the start.
     */
    record Live(String id) implements CatchupStatus {
        public Live {
            Objects.requireNonNull(id, "id cannot be null");
        }
    }

    /**
     * The catch-up failed and will not recover on its own. The subscription keeps its registration and refuses every
     * event afterwards, so the source redelivers rather than losing them. Fix the cause, then cancel the subscription
     * and subscribe again.
     */
    record Failed(String id, Throwable cause) implements CatchupStatus {
        public Failed {
            Objects.requireNonNull(id, "id cannot be null");
            Objects.requireNonNull(cause, "cause cannot be null");
        }
    }

    /**
     * Nothing here knows this id. It is not a push projection or saga registered by an Occurrent starter, or it is
     * spelled differently. Deliberately distinct from {@link Live}, since the question a readiness probe asks is
     * whether a named read model is ready, and an unknown name is not an answer of yes.
     */
    record Unknown(String id) implements CatchupStatus {
        public Unknown {
            Objects.requireNonNull(id, "id cannot be null");
        }
    }

    /**
     * Where the projection or saga with this id is in its catch-up.
     *
     * @param id The projection or saga id.
     * @return Its status, or {@link Unknown} if nothing here knows the id.
     */
    CatchupStatus of(String id);

    /**
     * @param id The projection or saga id.
     * @return {@code true} only when {@code id} is known and has handed over to live events. {@link Unknown} answers
     * {@code false}, because a readiness probe asking about a name nothing recognises has not been told yes.
     */
    boolean isCaughtUp(String id);

    /**
     * Every push projection and saga this application registered, keyed by id, in registration order.
     */
    Map<String, CatchupStatus> all();
}
