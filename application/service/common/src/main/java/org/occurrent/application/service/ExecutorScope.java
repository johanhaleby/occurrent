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

package org.occurrent.application.service;

import org.jspecify.annotations.NullMarked;

/**
 * What a transaction executor set up for one attempt of the action it runs. The blocking {@code TransactionExecutor} and
 * the reactive {@code ReactiveTransactionExecutor} give it to the action in {@code inTransactionTellingIfOpened}.
 *
 * @param openedTransaction      {@code true} if the executor opened the transaction the attempt runs in. {@code false} if
 *                               the attempt joined a transaction someone else opened, or runs in none.
 * @param startedSynchronization {@code true} if the executor started the transaction synchronization the attempt runs
 *                               in, such as Spring's, so a resource the attempt binds to it is released when the executor
 *                               finishes the attempt. {@code false} if the attempt runs in a synchronization the caller
 *                               started, or in none.
 */
@NullMarked
public record ExecutorScope(boolean openedTransaction, boolean startedSynchronization) {
}
