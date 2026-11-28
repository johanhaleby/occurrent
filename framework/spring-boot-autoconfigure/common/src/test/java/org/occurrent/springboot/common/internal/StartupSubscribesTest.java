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
package org.occurrent.springboot.common.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class StartupSubscribesTest {

    @Test
    void a_start_registered_before_start_does_not_run_until_start() {
        StartupSubscribes startupSubscribes = new StartupSubscribes();
        List<String> ran = new ArrayList<>();

        startupSubscribes.run(() -> ran.add("held back"));

        assertThat(ran).as("[runs before start()]").isEmpty();

        startupSubscribes.start();

        assertThat(ran).as("[runs after start()]").containsExactly("held back");
    }

    @Test
    void the_held_back_starts_run_in_the_order_they_were_registered_each_finishing_before_the_next() {
        StartupSubscribes startupSubscribes = new StartupSubscribes();
        List<String> ran = new ArrayList<>();
        startupSubscribes.run(() -> {
            ran.add("first begins");
            ran.add("first ends");
        });
        startupSubscribes.run(() -> {
            ran.add("second begins");
            ran.add("second ends");
        });
        startupSubscribes.run(() -> ran.add("third"));

        startupSubscribes.start();

        assertThat(ran).as("[order of the held back starts]").containsExactly("first begins", "first ends", "second begins", "second ends", "third");
    }

    @Test
    void the_held_back_starts_run_on_the_thread_that_calls_start() {
        StartupSubscribes startupSubscribes = new StartupSubscribes();
        List<Thread> ranOn = new ArrayList<>();
        startupSubscribes.run(() -> ranOn.add(Thread.currentThread()));

        startupSubscribes.start();

        assertThat(ranOn).as("[threads the held back start ran on]").containsExactly(Thread.currentThread());
    }

    @Test
    void a_start_registered_after_start_runs_at_once() {
        StartupSubscribes startupSubscribes = new StartupSubscribes();
        startupSubscribes.start();
        List<String> ran = new ArrayList<>();

        startupSubscribes.run(() -> ran.add("at once"));

        assertThat(ran).as("[runs when registered after start()]").containsExactly("at once");
    }

    @Test
    void a_start_registered_by_a_held_back_start_runs_at_once_and_nothing_runs_twice() {
        StartupSubscribes startupSubscribes = new StartupSubscribes();
        List<String> ran = new ArrayList<>();
        startupSubscribes.run(() -> {
            ran.add("outer begins");
            startupSubscribes.run(() -> ran.add("inner"));
            ran.add("outer ends");
        });

        startupSubscribes.start();
        startupSubscribes.start();

        assertThat(ran).as("[order and count of runs]").containsExactly("outer begins", "inner", "outer ends");
    }
}
