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
class StartupGapTest {

    @Test
    void nothing_starts_while_one_of_the_gaps_is_closed() {
        List<String> started = new ArrayList<>();
        FakeGap first = new FakeGap("first", started);
        FakeGap second = new FakeGap("second", started);
        first.open();

        StartupGap.startHeldBackOnceEveryGapAdmitsWrites(first, List.of(first, second));

        assertThat(started).as("[held-back starts that ran while the second gap was closed]").isEmpty();
    }

    @Test
    void every_gap_starts_in_the_given_order_once_all_of_them_admit_writes() {
        List<String> started = new ArrayList<>();
        FakeGap first = new FakeGap("first", started);
        FakeGap second = new FakeGap("second", started);
        first.open();
        second.open();

        StartupGap.startHeldBackOnceEveryGapAdmitsWrites(second, List.of(first, second));

        assertThat(started).as("[held-back starts in the order the gaps were given]").containsExactly("first", "second");
    }

    @Test
    void the_gap_that_opens_last_starts_the_held_back_subscriptions_of_all_of_them() {
        List<String> started = new ArrayList<>();
        FakeGap first = new FakeGap("first", started);
        FakeGap second = new FakeGap("second", started);
        List<StartupGap> gaps = List.of(first, second);

        first.open();
        StartupGap.startHeldBackOnceEveryGapAdmitsWrites(first, gaps);
        second.open();
        StartupGap.startHeldBackOnceEveryGapAdmitsWrites(second, gaps);

        assertThat(started).as("[held-back starts, none before the second gap opened]").containsExactly("first", "second");
    }

    @Test
    void a_gap_missing_from_the_list_is_still_started() {
        List<String> started = new ArrayList<>();
        FakeGap other = new FakeGap("other", started);
        FakeGap self = new FakeGap("self", started);
        other.open();
        self.open();

        StartupGap.startHeldBackOnceEveryGapAdmitsWrites(self, List.of(other));

        assertThat(started).as("[held-back starts when self is not in the list]").containsExactly("other", "self");
    }

    @Test
    void a_gap_missing_from_the_list_that_is_closed_holds_back_the_others() {
        List<String> started = new ArrayList<>();
        FakeGap other = new FakeGap("other", started);
        FakeGap self = new FakeGap("self", started);
        other.open();

        StartupGap.startHeldBackOnceEveryGapAdmitsWrites(self, List.of(other));

        assertThat(started).as("[held-back starts while self, which is not in the list, is closed]").isEmpty();
    }

    @Test
    void a_second_call_starts_nothing_again() {
        StartupSubscribes first = new StartupSubscribes();
        StartupSubscribes second = new StartupSubscribes();
        List<String> started = new ArrayList<>();
        first.run(() -> started.add("first"));
        second.run(() -> started.add("second"));
        List<StartupGap> gaps = List.of(new BackedGap(first), new BackedGap(second));

        StartupGap.startHeldBackOnceEveryGapAdmitsWrites(gaps.get(0), gaps);
        StartupGap.startHeldBackOnceEveryGapAdmitsWrites(gaps.get(1), gaps);

        assertThat(started).as("[held-back starts after two calls]").containsExactly("first", "second");
    }

    private static final class FakeGap implements StartupGap {
        private final String name;
        private final List<String> started;
        private boolean open;

        FakeGap(String name, List<String> started) {
            this.name = name;
            this.started = started;
        }

        void open() {
            open = true;
        }

        @Override
        public boolean admitsWrite() {
            return open;
        }

        @Override
        public void startHeldBack() {
            started.add(name);
        }

        @Override
        public boolean registrationStarts() {
            return !open;
        }

        @Override
        public void registrationEnds() {
        }
    }

    // Starts what StartupSubscribes held back, like the post-processors, and is open from the start
    private record BackedGap(StartupSubscribes startupSubscribes) implements StartupGap {
        @Override
        public boolean admitsWrite() {
            return true;
        }

        @Override
        public void startHeldBack() {
            startupSubscribes.start();
        }

        @Override
        public boolean registrationStarts() {
            return false;
        }

        @Override
        public void registrationEnds() {
        }
    }
}
