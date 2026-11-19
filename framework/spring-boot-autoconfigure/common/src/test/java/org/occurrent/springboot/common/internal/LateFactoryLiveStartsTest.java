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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class LateFactoryLiveStartsTest {

    private static final boolean SINGLETON = true;
    private static final boolean NOT_SINGLETON = false;
    private static final long AWAIT_SECONDS = 20;

    private final LateFactoryLiveStarts liveStarts = new LateFactoryLiveStarts();

    @Test
    void a_non_singleton_entry_put_on_one_thread_is_not_visible_on_another() throws Exception {
        liveStarts.put("factory", NOT_SINGLETON, "boundary");

        assertThat(onAnotherThread(() -> liveStarts.remove("factory", NOT_SINGLETON))).isNull();
        assertThat(liveStarts.remove("factory", NOT_SINGLETON)).isEqualTo("boundary");
    }

    @Test
    void a_non_singleton_entry_put_on_another_thread_is_not_visible_here() throws Exception {
        onAnotherThread(() -> {
            liveStarts.put("factory", NOT_SINGLETON, "boundary");
            return null;
        });

        assertThat(liveStarts.remove("factory", NOT_SINGLETON)).isNull();
    }

    @Test
    void two_threads_putting_a_non_singleton_entry_under_the_same_name_each_take_their_own() throws Exception {
        liveStarts.put("factory", NOT_SINGLETON, "first");
        onAnotherThread(() -> {
            liveStarts.put("factory", NOT_SINGLETON, "second");
            return null;
        });

        assertThat(liveStarts.remove("factory", NOT_SINGLETON)).isEqualTo("first");
        assertThat(onAnotherThread(() -> liveStarts.remove("factory", NOT_SINGLETON))).isNull();
    }

    @Test
    void a_singleton_entry_put_on_one_thread_is_visible_on_another() throws Exception {
        liveStarts.put("factory", SINGLETON, "boundary");

        assertThat(onAnotherThread(() -> liveStarts.remove("factory", SINGLETON))).isEqualTo("boundary");
        assertThat(liveStarts.remove("factory", SINGLETON)).isNull();
    }

    @Test
    void a_null_live_start_drops_an_earlier_non_singleton_entry() {
        liveStarts.put("factory", NOT_SINGLETON, "boundary");
        liveStarts.put("factory", NOT_SINGLETON, null);

        assertThat(liveStarts.remove("factory", NOT_SINGLETON)).isNull();
    }

    @Test
    void a_null_live_start_drops_an_earlier_singleton_entry() {
        liveStarts.put("factory", SINGLETON, "boundary");
        liveStarts.put("factory", SINGLETON, null);

        assertThat(liveStarts.remove("factory", SINGLETON)).isNull();
    }

    @Test
    void an_entry_is_removed_with_its_value_once() {
        liveStarts.put("singleton", SINGLETON, "one");
        liveStarts.put("other", NOT_SINGLETON, "two");

        assertThat(liveStarts.remove("singleton", SINGLETON)).isEqualTo("one");
        assertThat(liveStarts.remove("singleton", SINGLETON)).isNull();
        assertThat(liveStarts.remove("other", NOT_SINGLETON)).isEqualTo("two");
        assertThat(liveStarts.remove("other", NOT_SINGLETON)).isNull();
    }

    @Test
    void an_entry_is_found_by_its_bean_name_only() {
        liveStarts.put("factory", SINGLETON, "boundary");
        liveStarts.put("other", NOT_SINGLETON, "other boundary");

        assertThat(liveStarts.remove("unknown", SINGLETON)).isNull();
        assertThat(liveStarts.remove("unknown", NOT_SINGLETON)).isNull();
        assertThat(liveStarts.remove("factory", SINGLETON)).isEqualTo("boundary");
        assertThat(liveStarts.remove("other", NOT_SINGLETON)).isEqualTo("other boundary");
    }

    private static @Nullable String onAnotherThread(Callable<@Nullable String> work) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            return executor.submit(work).get(AWAIT_SECONDS, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }
}
