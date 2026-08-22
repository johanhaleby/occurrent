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

package org.occurrent.annotation;

import java.lang.annotation.*;

/**
 * Marks a no-arg factory method returning a {@code SnapshotView}, registering it as a framework-maintained snapshot: a
 * per-stream, resume-ready fold of the stream's state that a snapshot-accelerated application service can load instead of
 * replaying the whole stream. For example:
 *
 * <pre lang="java">
 * &#64;OccurrentSnapshot(id = "accountSnapshot")
 * SnapshotView&lt;Account, AccountEvent&gt; accountSnapshot() {
 *     return SnapshotView.&lt;Account, AccountEvent&gt;builder(Account.EMPTY)
 *         .schemaVersion(1)
 *         .on(Opened.class, (state, event) -> state.opened(event))
 *         .on(Deposited.class, (state, event) -> state.deposited(event))
 *         .build();
 * }
 * </pre>
 * The Kotlin equivalent is {@code snapshotView(Account.EMPTY) { schemaVersion(1); on<Opened> { s, e -> s.opened(e) } }}.
 * <p>
 * The method may live on any Spring bean, a {@code @Bean} in a {@code @Configuration} or a method on a
 * {@code @Component}.
 *
 * <h4>What a snapshot is</h4>
 * <p>
 * A snapshot is a discardable optimization, never a source of truth. It is stored with the stream version it was folded
 * up to and the {@code schemaVersion} declared on the returned {@code SnapshotView}. When the schema version changes the
 * stored snapshot is ignored and the state is rebuilt from history, so a changed state shape fails safe to a full
 * replay. Maintaining a snapshot adds one write per handled event on the maintenance path, and using it saves folding
 * the whole history on the write path.
 * </p>
 *
 * <h4>Mode and startup behavior</h4>
 * <p>
 * {@link #mode()} chooses asynchronous maintenance (the default, the snapshot is updated from a catch-up subscription)
 * or synchronous (the snapshot is updated on the write path, so it is current for read-your-writes). Synchronous mode is
 * mutually exclusive with {@link #startAt()}, {@link #startAtGlobalPosition()}, {@link #resumeBehavior()}, and {@link #startupMode()}.
 * </p>
 * <p>
 * {@link #startAt()} defaults to {@link StartPosition#BEGINNING} because a maintained snapshot must fold every event of
 * a stream to be correct, so it replays history the first time and then resumes from its durable checkpoint.
 * </p>
 *
 * <h4>State store</h4>
 * <p>
 * The {@link #store()} and {@link #storeName()} attributes select a {@code SnapshotStore} bean by type or by name. With
 * both unset the store resolves by convention: the unique {@code SnapshotStore} bean, otherwise a zero-config MongoDB
 * store keyed by this snapshot's {@code id}. A zero-config store needs the factory return type to declare a concrete
 * state type (for example {@code SnapshotView<Account, AccountEvent>}) so the state can be read back from MongoDB.
 * </p>
 * <p>
 * A factory method may return a {@code SnapshotView} to maintain a stream or capability-agnostic snapshot, or a
 * {@code DcbSnapshotView} to maintain a DCB snapshot, one per boundary. DCB snapshots do not support the synchronous
 * {@link #mode()}.
 * </p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OccurrentSnapshot {
    /**
     * The unique identifier of the snapshot (required, no default). It is the durable checkpoint key and the namespace
     * for the zero-config store, and must be unique across all subscriptions, projections, and snapshots.
     */
    String id();

    /**
     * The start position for the maintenance subscription. Defaults to {@link StartPosition#BEGINNING}, deliberately:
     * a maintained snapshot must fold an entity's full history to be a correct stand-in for a full replay, so unlike a
     * projection or a plain subscription (which may legitimately start from {@code NOW}), a snapshot cannot skip the
     * backlog without producing a snapshot that silently omits events its readers assume it has folded. Mutually
     * exclusive with {@link #startAtGlobalPosition()} and with {@link Mode#SYNCHRONOUS}.
     */
    StartPosition startAt() default StartPosition.BEGINNING;

    /**
     * Start after a specific global position instead of a predefined {@link #startAt()}. The default of -1 means unset.
     * Mutually exclusive with a non-{@link StartPosition#BEGINNING} {@link #startAt()} and with {@link Mode#SYNCHRONOUS}.
     */
    long startAtGlobalPosition() default -1;

    /**
     * How the maintenance subscription resumes on restart. By default it resumes from its last stored checkpoint after
     * the initial replay. Mutually exclusive with {@link Mode#SYNCHRONOUS}.
     */
    ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

    /**
     * How the maintenance subscription behaves during startup. The default defers to the framework.
     */
    StartupMode startupMode() default StartupMode.DEFAULT;

    /**
     * Save the snapshot at most once every {@code n} stream versions instead of on every handled event. The default of
     * 1 saves after every handled event. A larger value trades snapshot freshness for fewer writes: the snapshot then
     * lags by up to {@code n} events, which a snapshot-accelerated read simply folds on top. When a save does happen
     * after skipped events, the intervening events are folded from the store so the saved state stays correct.
     * <p>
     * This is the deliberate annotation-level trigger ceiling. The schema version and richer trigger policies (an
     * event type, a state predicate, or a decider's terminal state, for example the "closing the books" case) live on
     * the {@code SnapshotView} returned by the factory method, via its builder. Drop to the DSL {@code SnapshotPolicy}
     * directly for those triggers.
     */
    int everyNEvents() default 1;

    /**
     * The capability scope. {@link Capability#AGNOSTIC} folds events from all capabilities (stream and DCB), while
     * {@link Capability#STREAM} folds only stream-written events.
     */
    Capability capability() default Capability.AGNOSTIC;

    /**
     * The maintenance mode. {@link Mode#ASYNC} (the default) updates the snapshot from a catch-up subscription.
     * {@link Mode#SYNCHRONOUS} updates it on the write path, so it is current for read-your-writes. Synchronous mode is
     * mutually exclusive with {@link #startAt()}, {@link #startAtGlobalPosition()}, {@link #resumeBehavior()}, and {@link #startupMode()}.
     */
    Mode mode() default Mode.ASYNC;

    /**
     * The snapshot store to maintain, given as the store bean's type ({@code SnapshotStore.class} or a subtype).
     * {@link Void} (the default) leaves the type unset, in which case {@link #storeName()} or convention-based
     * resolution applies.
     */
    Class<?> store() default Void.class;

    /**
     * Optional Spring bean name of the snapshot store. Used on its own to resolve the store by name, or together with
     * {@link #store()} to pick one bean when several of that type exist. An empty string (the default) means unset.
     */
    String storeName() default "";
}
