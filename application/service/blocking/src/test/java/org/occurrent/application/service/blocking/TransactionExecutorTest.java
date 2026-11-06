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

package org.occurrent.application.service.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class TransactionExecutorTest {

    @Test
    void the_action_is_told_that_this_call_opened_a_transaction_when_one_is_in_force_only_inside() {
        TransactionExecutor executor = new TransactionExecutor() {
            private boolean inside;

            @Override
            public <T> T inTransaction(Supplier<T> action) {
                inside = true;
                try {
                    return action.get();
                } finally {
                    inside = false;
                }
            }

            @Override
            public boolean isTransactional() {
                return inside;
            }
        };

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(openedTransaction -> openedTransaction)).isTrue();
    }

    @Test
    void the_action_is_told_that_this_call_opened_no_transaction_when_one_was_already_in_force() {
        TransactionExecutor executor = new TransactionExecutor() {
            @Override
            public <T> T inTransaction(Supplier<T> action) {
                return action.get();
            }

            @Override
            public boolean isTransactional() {
                return true;
            }
        };

        assertThat(executor.<Boolean>inTransactionTellingIfOpened(openedTransaction -> openedTransaction)).isFalse();
    }

    @Test
    void the_action_is_told_that_this_call_opened_no_transaction_when_none_is_in_force() {
        assertThat(TransactionExecutor.noTransaction().<Boolean>inTransactionTellingIfOpened(openedTransaction -> openedTransaction)).isFalse();
    }
}
