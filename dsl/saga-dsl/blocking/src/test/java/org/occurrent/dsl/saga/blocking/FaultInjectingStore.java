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

package org.occurrent.dsl.saga.blocking;

import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStateStore;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delegates to the in-memory store and can be told to fail or lose the next calls, to throw after a write took effect,
 * and to run another delivery at a named point of a call.
 */
final class FaultInjectingStore<S> implements SagaStateStore<S>, SagaHeldEvents<S> {
    final InMemorySagaStateStore<S> d = new InMemorySagaStateStore<>();
    final AtomicInteger loseWithoutState = new AtomicInteger();
    final AtomicInteger loseCas = new AtomicInteger();
    final AtomicInteger failHold = new AtomicInteger();
    final AtomicInteger failRelease = new AtomicInteger();
    final AtomicInteger failMark = new AtomicInteger();
    final AtomicInteger failFind = new AtomicInteger();
    final AtomicInteger writeThenThrowCas = new AtomicInteger();
    final AtomicInteger writeThenThrowWithoutState = new AtomicInteger();
    final AtomicInteger failHeldRead = new AtomicInteger();
    final AtomicInteger failFindWithoutState = new AtomicInteger();
    volatile boolean cannotDecode;
    volatile String interleaveAt;
    volatile Runnable interleaveAction;

    void point(String p) {
        if (p.equals(interleaveAt)) {
            Runnable a = interleaveAction;
            interleaveAt = null;
            interleaveAction = null;
            a.run();
        }
    }

    private static boolean take(AtomicInteger c) {
        return c.getAndUpdate(v -> v > 0 ? v - 1 : 0) > 0;
    }

    @Override public Optional<SagaEnvelope<S>> find(String id) {
        if (cannotDecode || take(failFind)) throw new IllegalStateException("cannot decode");
        Optional<SagaEnvelope<S>> r = d.find(id);
        point("afterFind");
        return r;
    }
    @Override public Optional<SagaEnvelope<S>> findWithoutState(String id) {
        if (take(failFindWithoutState)) throw new IllegalStateException("cannot read");
        Optional<SagaEnvelope<S>> r = d.find(id);
        point("afterFindWithoutState");
        return r;
    }
    @Override public boolean compareAndSave(String id, SagaEnvelope<S> e, long v) {
        point("beforeCas");
        if (take(loseCas)) return false;
        boolean r = d.compareAndSave(id, e, v);
        if (r && take(writeThenThrowCas)) throw new IllegalStateException("response lost");
        return r;
    }
    @Override public boolean compareAndSaveWithoutState(String id, SagaEnvelope<S> e, long v) {
        point("beforeCasWithoutState");
        if (take(loseWithoutState)) return false;
        boolean r = d.compareAndSave(id, e, v);
        if (r && take(writeThenThrowWithoutState)) throw new IllegalStateException("response lost");
        return r;
    }
    @Override public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) { return d.findWithDueTimers(now, limit); }
    @Override public void delete(String id) { d.delete(id); }
    @Override public boolean hold(String id, HeldEvent e) {
        point("beforeHold");
        if (take(failHold)) throw new IllegalStateException("cannot hold");
        boolean r = d.hold(id, e);
        point("afterHold");
        return r;
    }
    @Override public void failing(String id, String key, Instant at) {
        point("beforeMark");
        if (take(failMark)) throw new IllegalStateException("cannot mark");
        d.failing(id, key, at);
        point("afterMark");
    }
    @Override public List<HeldEventRef> heldIndex(String id) {
        List<HeldEventRef> r = d.heldIndex(id);
        point("afterHeldIndex");
        return r;
    }
    @Override public List<HeldEvent> held(String id, Collection<String> keys) {
        if (take(failHeldRead)) throw new IllegalStateException("cannot read held");
        List<HeldEvent> r = d.held(id, keys);
        point("afterHeld");
        return r;
    }
    @Override public void release(String id, String key) {
        point("beforeRelease");
        if (take(failRelease)) throw new IllegalStateException("cannot release");
        d.release(id, key);
        point("afterRelease");
    }
}
