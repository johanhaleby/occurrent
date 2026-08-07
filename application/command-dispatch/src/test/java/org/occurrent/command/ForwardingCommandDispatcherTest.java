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

package org.occurrent.command;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ForwardingCommandDispatcher")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ForwardingCommandDispatcherTest {

    @Test
    void a_decorator_overriding_only_dispatch_still_calls_the_delegates_dispatchAll_for_a_batch() {
        // Given a delegate whose dispatchAll is atomic: it fails the whole batch rather than writing commands one by one
        List<String> delegateBatchCalls = new ArrayList<>();
        CommandDispatcher<String> atomicDelegate = new CommandDispatcher<>() {
            @Override
            public void dispatch(String command) {
                throw new UnsupportedOperationException("only dispatchAll is atomic here");
            }

            @Override
            public void dispatchAll(List<String> commands) {
                delegateBatchCalls.addAll(commands);
            }
        };

        // And a decorator that overrides only dispatch, extending ForwardingCommandDispatcher instead of implementing
        // CommandDispatcher directly
        List<String> observed = new ArrayList<>();
        CommandDispatcher<String> decorator = new ForwardingCommandDispatcher<>(atomicDelegate) {
            @Override
            public void dispatch(String command) {
                observed.add(command);
                super.dispatch(command);
            }
        };

        // When
        decorator.dispatchAll(List.of("a", "b", "c"));

        // Then the batch went to the delegate's own dispatchAll, not to a per-command loop over the decorator's dispatch
        assertThat(delegateBatchCalls).containsExactly("a", "b", "c");
        assertThat(observed).isEmpty();
    }

    @Test
    void dispatch_forwards_to_the_delegate() {
        List<String> dispatched = new ArrayList<>();
        CommandDispatcher<String> delegate = dispatched::add;
        CommandDispatcher<String> decorator = new ForwardingCommandDispatcher<>(delegate) {
        };

        decorator.dispatch("a");

        assertThat(dispatched).containsExactly("a");
    }

    @Test
    void a_null_delegate_is_rejected_at_construction() {
        assertThatThrownBy(() -> new ForwardingCommandDispatcher<String>(null) {
        }).isInstanceOf(NullPointerException.class);
    }
}
