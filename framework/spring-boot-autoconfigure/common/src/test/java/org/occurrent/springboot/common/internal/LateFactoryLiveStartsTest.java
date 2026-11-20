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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class LateFactoryLiveStartsTest {

    private static final long AWAIT_SECONDS = 20;
    private static final long GC_DEADLINE_NANOS = 10_000_000_000L;

    private final LateFactoryLiveStarts liveStarts = new LateFactoryLiveStarts();

    @Test
    void two_factories_under_one_name_each_keep_the_boundary_they_were_created_with() {
        Object first = new Object();
        Object second = new Object();
        liveStarts.factoryCreated("factory", first, false, "one");
        liveStarts.factoryCreated("factory", second, false, "two");

        assertThat(liveStarts.liveStartOf(first)).isEqualTo("one");
        assertThat(liveStarts.liveStartOf(second)).isEqualTo("two");
    }

    @Test
    void a_factory_created_on_another_thread_keeps_its_boundary_where_this_thread_looks_it_up() throws Exception {
        Object factory = new Object();
        onAnotherThread(() -> {
            liveStarts.factoryCreated("factory", factory, false, "boundary");
            return null;
        });

        assertThat(liveStarts.liveStartOf(factory)).isEqualTo("boundary");
    }

    @Test
    void reading_the_boundary_of_a_factory_takes_none_away() {
        Object factory = new Object();
        liveStarts.factoryCreated("factory", factory, false, "boundary");

        assertThat(liveStarts.liveStartOf(factory)).isEqualTo("boundary");
        assertThat(liveStarts.liveStartOf(factory)).isEqualTo("boundary");
    }

    @Test
    void a_factory_that_was_never_created_has_no_boundary() {
        liveStarts.factoryCreated("factory", new Object(), false, "boundary");

        assertThat(liveStarts.liveStartOf(new Object())).isNull();
    }

    @Test
    void a_factory_created_with_no_boundary_has_none_and_leaves_the_others_theirs() {
        Object first = new Object();
        Object second = new Object();
        liveStarts.factoryCreated("factory", first, false, "one");
        liveStarts.factoryCreated("factory", second, false, null);

        assertThat(liveStarts.liveStartOf(second)).isNull();
        assertThat(liveStarts.liveStartOf(first)).isEqualTo("one");
    }

    @Test
    void a_prototype_claim_returns_the_boundary_of_the_oldest_unclaimed_factory_and_removes_the_most_recent() {
        Object first = new Object();
        Object second = new Object();
        Object third = new Object();
        liveStarts.factoryCreated("factory", first, true, "one");
        liveStarts.factoryCreated("factory", second, true, "two");
        liveStarts.factoryCreated("factory", third, true, "three");

        assertThat(liveStarts.claimPrototypeFactory("factory")).isEqualTo("one");
        assertThat(liveStarts.claimPrototypeFactory("factory")).isEqualTo("one");
        assertThat(liveStarts.claimPrototypeFactory("factory")).isEqualTo("one");
        assertThat(liveStarts.claimPrototypeFactory("factory")).isNull();
    }

    @Test
    void a_prototype_claim_removes_the_most_recent_factory_so_the_oldest_is_the_one_left() {
        Object first = new Object();
        Object second = new Object();
        liveStarts.factoryCreated("factory", first, true, "one");
        liveStarts.factoryCreated("factory", second, true, "two");

        liveStarts.claimPrototypeFactory("factory");
        Object third = new Object();
        liveStarts.factoryCreated("factory", third, true, "three");

        assertThat(liveStarts.claimPrototypeFactory("factory")).isEqualTo("one");
        assertThat(liveStarts.claimPrototypeFactory("factory")).isEqualTo("one");
        assertThat(liveStarts.claimPrototypeFactory("factory")).isNull();
    }

    @Test
    void a_prototype_claim_on_a_thread_that_created_no_factory_returns_null() throws Exception {
        liveStarts.factoryCreated("factory", new Object(), true, "boundary");

        assertThat(onAnotherThread(() -> liveStarts.claimPrototypeFactory("factory"))).isNull();
    }

    @Test
    void a_prototype_claim_under_a_name_no_factory_was_created_under_returns_null() {
        liveStarts.factoryCreated("factory", new Object(), true, "boundary");

        assertThat(liveStarts.claimPrototypeFactory("other")).isNull();
    }

    @Test
    void a_factory_that_is_not_a_prototype_is_not_claimed() {
        liveStarts.factoryCreated("factory", new Object(), false, "boundary");

        assertThat(liveStarts.claimPrototypeFactory("factory")).isNull();
    }

    @Test
    void a_prototype_claim_keeps_the_names_apart() {
        liveStarts.factoryCreated("factory", new Object(), true, "one");
        liveStarts.factoryCreated("other", new Object(), true, "two");

        assertThat(liveStarts.claimPrototypeFactory("other")).isEqualTo("two");
        assertThat(liveStarts.claimPrototypeFactory("factory")).isEqualTo("one");
    }

    @Test
    void a_prototype_factory_created_with_no_boundary_is_claimed_with_none() {
        liveStarts.factoryCreated("factory", new Object(), true, null);

        assertThat(liveStarts.claimPrototypeFactory("factory")).isNull();
    }

    @Test
    void a_prototype_claim_skips_a_factory_that_was_collected() {
        Object kept = new Object();
        collectPrototypeFactoryHoldingABoundary("factory");
        liveStarts.factoryCreated("factory", kept, true, "kept");

        assertThat(liveStarts.claimPrototypeFactory("factory")).isEqualTo("kept");
    }

    @Test
    void the_boundary_of_a_factory_that_was_collected_is_dropped_by_the_next_factory_created() {
        WeakReference<String> boundary = collectFactoryHoldingABoundary();

        assertThat(uncollectedAfterGc(() -> {
            liveStarts.factoryCreated("other", new Object(), false, "other boundary");
            return boundary.get() != null;
        })).isZero();
    }

    @Test
    void the_boundary_of_a_factory_that_was_collected_is_dropped_by_the_next_boundary_looked_up() {
        WeakReference<String> boundary = collectFactoryHoldingABoundary();

        assertThat(uncollectedAfterGc(() -> {
            liveStarts.liveStartOf(new Object());
            return boundary.get() != null;
        })).isZero();
    }

    @Test
    void the_boundary_of_a_factory_that_is_still_referenced_is_kept_after_gc() {
        Object factory = new Object();
        liveStarts.factoryCreated("factory", factory, false, new String("boundary"));

        System.gc();
        liveStarts.factoryCreated("other", new Object(), false, "other boundary");

        assertThat(liveStarts.liveStartOf(factory)).isEqualTo("boundary");
    }

    @Test
    void the_newest_unclaimed_boundary_is_null_when_the_thread_created_no_factory_under_the_name() {
        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isNull();
    }

    @Test
    void the_newest_unclaimed_boundary_is_the_one_of_the_factory_created_last_and_reading_it_takes_none_away() {
        liveStarts.factoryCreated("factory", new Object(), true, "one");
        liveStarts.factoryCreated("factory", new Object(), true, "two");
        liveStarts.factoryCreated("factory", new Object(), true, "three");

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isEqualTo("three");
        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isEqualTo("three");
        assertThat(liveStarts.claimPrototypeFactory("factory")).isEqualTo("one");
    }

    @Test
    void the_newest_unclaimed_boundary_skips_a_factory_that_was_claimed() {
        liveStarts.factoryCreated("factory", new Object(), true, "one");
        liveStarts.factoryCreated("factory", new Object(), true, "two");
        liveStarts.claimPrototypeFactory("factory");

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isEqualTo("one");
    }

    @Test
    void the_newest_unclaimed_boundary_is_null_once_every_factory_was_claimed() {
        liveStarts.factoryCreated("factory", new Object(), true, "one");
        liveStarts.claimPrototypeFactory("factory");

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isNull();
    }

    @Test
    void the_newest_unclaimed_boundary_skips_a_factory_that_read_none() {
        liveStarts.factoryCreated("factory", new Object(), true, "one");
        liveStarts.factoryCreated("factory", new Object(), true, null);

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isEqualTo("one");
    }

    @Test
    void the_newest_unclaimed_boundary_is_null_when_no_unclaimed_factory_read_one() {
        liveStarts.factoryCreated("factory", new Object(), true, null);

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isNull();
    }

    @Test
    void the_newest_unclaimed_boundary_skips_a_factory_that_was_collected() {
        Object kept = new Object();
        liveStarts.factoryCreated("factory", kept, true, "kept");
        collectPrototypeFactoryHoldingABoundary("factory");

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isEqualTo("kept");
    }

    @Test
    void the_newest_unclaimed_boundary_is_null_when_the_only_factory_was_collected() {
        collectPrototypeFactoryHoldingABoundary("factory");

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isNull();
    }

    @Test
    void the_newest_unclaimed_boundary_ignores_a_factory_that_is_not_a_prototype() {
        liveStarts.factoryCreated("factory", new Object(), false, "boundary");

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isNull();
    }

    @Test
    void the_newest_unclaimed_boundary_does_not_see_a_factory_another_thread_created() throws Exception {
        onAnotherThread(() -> {
            liveStarts.factoryCreated("factory", new Object(), true, "other thread");
            return null;
        });
        liveStarts.factoryCreated("factory", new Object(), true, "this thread");

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isEqualTo("this thread");
        assertThat(onAnotherThread(() -> liveStarts.newestUnclaimedLiveStart("factory"))).isNull();
    }

    @Test
    void the_newest_unclaimed_boundary_is_null_on_a_thread_that_created_no_factory() throws Exception {
        liveStarts.factoryCreated("factory", new Object(), true, "boundary");

        assertThat(onAnotherThread(() -> liveStarts.newestUnclaimedLiveStart("factory"))).isNull();
    }

    @Test
    void the_newest_unclaimed_boundary_keeps_the_names_apart() {
        liveStarts.factoryCreated("factory", new Object(), true, "one");
        liveStarts.factoryCreated("other", new Object(), true, "two");

        assertThat(liveStarts.newestUnclaimedLiveStart("factory")).isEqualTo("one");
        assertThat(liveStarts.newestUnclaimedLiveStart("other")).isEqualTo("two");
        assertThat(liveStarts.newestUnclaimedLiveStart("third")).isNull();
    }

    private void collectPrototypeFactoryHoldingABoundary(String beanName) {
        Object factory = new Object();
        liveStarts.factoryCreated(beanName, factory, true, new String("boundary"));
        WeakReference<Object> factoryReference = new WeakReference<>(factory);
        factory = null;
        assertThat(uncollectedAfterGc(() -> factoryReference.get() != null)).describedAs("factory still reachable after gc").isZero();
    }

    private WeakReference<String> collectFactoryHoldingABoundary() {
        Object factory = new Object();
        String boundary = new String("boundary");
        liveStarts.factoryCreated("factory", factory, false, boundary);
        WeakReference<Object> factoryReference = new WeakReference<>(factory);
        WeakReference<String> boundaryReference = new WeakReference<>(boundary);
        factory = null;
        boundary = null;
        assertThat(uncollectedAfterGc(() -> factoryReference.get() != null)).describedAs("factory still reachable after gc").isZero();
        return boundaryReference;
    }

    private static long uncollectedAfterGc(Callable<Boolean> stillReachable) {
        long deadline = System.nanoTime() + GC_DEADLINE_NANOS;
        boolean reachable;
        try {
            while ((reachable = stillReachable.call()) && System.nanoTime() < deadline) {
                System.gc();
                byte[] pressure = new byte[1024 * 1024];
                pressure[0] = 1;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return reachable ? 1 : 0;
    }

    private static <T extends @Nullable Object> T onAnotherThread(Callable<T> work) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            return executor.submit(work).get(AWAIT_SECONDS, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }
}
