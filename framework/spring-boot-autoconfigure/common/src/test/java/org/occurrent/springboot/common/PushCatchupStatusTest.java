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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.springboot.common.PushCatchupStatus.CatchingUp;
import org.occurrent.springboot.common.PushCatchupStatus.Failed;
import org.occurrent.springboot.common.PushCatchupStatus.Live;
import org.occurrent.springboot.common.PushCatchupStatus.NotStarted;
import org.occurrent.springboot.common.PushCatchupStatus.Unknown;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class PushCatchupStatusTest {

    private final PushCatchupStatus status = new PushCatchupStatus();

    @Nested
    class An_id_backed_by_a_subscription_model {

        @Test
        void is_catching_up_while_the_model_says_its_replay_is_in_flight() {
            AtomicBoolean replaying = new AtomicBoolean(true);
            status.register("orders", replaying::get, () -> true);

            assertThat(status.of("orders")).isEqualTo(new CatchingUp("orders"));
            assertThat(status.isCaughtUp("orders")).isFalse();
        }

        @Test
        void is_live_once_the_model_says_the_replay_handed_over() {
            AtomicBoolean replaying = new AtomicBoolean(true);
            status.register("orders", replaying::get, () -> true);

            replaying.set(false);

            assertThat(status.of("orders")).isEqualTo(new Live("orders"));
            assertThat(status.isCaughtUp("orders")).isTrue();
        }

        @Test
        void reports_catching_up_again_when_the_model_replays_a_second_time() {
            AtomicBoolean replaying = new AtomicBoolean(true);
            status.register("orders", replaying::get, () -> true);
            replaying.set(false);

            // What a stop() followed by start(true) does: the model relaunches the replay. Derived rather than
            // recorded is what makes this readable at all, since nothing tells this bean the replay restarted.
            replaying.set(true);

            assertThat(status.of("orders")).isEqualTo(new CatchingUp("orders"));
        }
    }

    @Nested
    class An_id_whose_subscription_has_not_started {

        @Test
        void is_not_started_rather_than_live() {
            // What occurrent.subscription.mode = manual leaves a push projection as. Nothing is replaying, so asking
            // isCatchingUp alone would report it live and tell a readiness probe it is ready to serve.
            status.register("orders", () -> false, () -> false);

            assertThat(status.of("orders")).isEqualTo(new NotStarted("orders"));
            assertThat(status.isCaughtUp("orders")).isFalse();
        }

        @Test
        void becomes_live_once_the_application_starts_it() {
            AtomicBoolean running = new AtomicBoolean(false);
            status.register("orders", () -> false, running::get);

            running.set(true);

            assertThat(status.of("orders")).isEqualTo(new Live("orders"));
        }

        @Test
        void goes_back_to_not_started_when_the_model_is_stopped() {
            AtomicBoolean running = new AtomicBoolean(true);
            status.register("orders", () -> false, running::get);

            running.set(false);

            assertThat(status.of("orders")).isEqualTo(new NotStarted("orders"));
            assertThat(status.isCaughtUp("orders")).isFalse();
        }

        @Test
        void is_still_catching_up_while_replaying_even_though_a_replay_also_reports_running() {
            // A model reports a replay as running, so resolving running before catching up would call a replay in
            // flight live.
            status.register("orders", () -> true, () -> true);

            assertThat(status.of("orders")).isEqualTo(new CatchingUp("orders"));
        }
    }

    @Nested
    class An_id_with_no_model_to_ask {

        @Test
        void carries_the_state_that_was_recorded_for_it() {
            status.recordCatchingUp("domain-feed");

            assertThat(status.of("domain-feed")).isEqualTo(new CatchingUp("domain-feed"));

            status.recordLive("domain-feed");

            assertThat(status.of("domain-feed")).isEqualTo(new Live("domain-feed"));
        }
    }

    @Nested
    class A_failure {

        @Test
        void carries_the_cause_that_ended_the_replay() {
            RuntimeException cause = new RuntimeException("replay boom");

            status.recordFailure("orders", cause);

            assertThat(status.of("orders")).isEqualTo(new Failed("orders", cause));
            assertThat(status.isCaughtUp("orders")).isFalse();
        }

        @Test
        void wins_over_what_the_model_would_say() {
            // The load-bearing case. A model forgets a replay that failed, so its isCatchingUp answers false
            // afterwards, and resolving the model first would report a broken projection as ready to serve.
            status.register("orders", () -> false, () -> true);

            status.recordFailure("orders", new RuntimeException("replay boom"));

            assertThat(status.of("orders")).isInstanceOf(Failed.class);
            assertThat(status.isCaughtUp("orders")).isFalse();
        }

        @Test
        void replaces_an_earlier_failure_for_the_same_id() {
            status.recordFailure("orders", new RuntimeException("first"));

            status.recordFailure("orders", new RuntimeException("second"));

            assertThat(status.of("orders")).isInstanceOfSatisfying(Failed.class,
                    failed -> assertThat(failed.cause()).hasMessage("second"));
        }
    }

    @Nested
    class An_id_nothing_here_knows {

        @Test
        void is_unknown_rather_than_live() {
            assertThat(status.of("never-registered")).isEqualTo(new Unknown("never-registered"));
        }

        @Test
        void is_not_reported_as_caught_up() {
            // A readiness probe asking about a name nothing recognises has not been told yes, which is the whole
            // distinction the old failures-only bean could not express.
            assertThat(status.isCaughtUp("never-registered")).isFalse();
        }
    }

    @Nested
    class Every_id_at_once {

        @Test
        void is_reported_in_registration_order() {
            status.register("first", () -> true, () -> true);
            status.recordLive("second");
            status.recordFailure("third", new RuntimeException("boom"));

            assertThat(status.all()).containsExactly(
                    org.assertj.core.api.Assertions.entry("first", new CatchingUp("first")),
                    org.assertj.core.api.Assertions.entry("second", new Live("second")),
                    org.assertj.core.api.Assertions.entry("third", status.of("third")));
        }

        @Test
        void reflects_a_model_that_has_since_handed_over() {
            AtomicBoolean replaying = new AtomicBoolean(true);
            status.register("orders", replaying::get, () -> true);

            replaying.set(false);

            assertThat(status.all()).containsExactly(
                    org.assertj.core.api.Assertions.entry("orders", new Live("orders")));
        }

        @Test
        void is_empty_before_anything_registers() {
            assertThat(status.all()).isEmpty();
        }
    }

    @Nested
    class Invalid_arguments {

        @Test
        void are_refused_eagerly() {
            assertThatThrownBy(() -> status.register(null, () -> true, () -> true)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> status.register("orders", null, () -> true)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> status.register("orders", () -> true, null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> status.recordFailure("orders", null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> status.of(null)).isInstanceOf(NullPointerException.class);
        }
    }
}
