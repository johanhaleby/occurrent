/*
 *
 *  Copyright 2025 Johan Haleby
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

package org.occurrent.dsl.view

import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.OptimisticLockingFailureException
import java.time.Duration

private val DEFAULT_INITIAL = Duration.ofMillis(100)
private val DEFAULT_MAX = Duration.ofSeconds(5)
private const val DEFAULT_MULTIPLIER = 2.0
private val DEFAULT_ON_ERROR: (Throwable) -> Unit = {}
private val DEFAULT_ON_DUPLICATE_KEY_EXCEPTION: (DuplicateKeyException) -> Unit = { }

/**
 * Determines how [DuplicateKeyException]'s are handled
 */
sealed interface DuplicateKeyHandling {
    data class Ignore(val onDuplicateKeyException: (DuplicateKeyException) -> Unit = DEFAULT_ON_DUPLICATE_KEY_EXCEPTION) : DuplicateKeyHandling
    data object Rethrow : DuplicateKeyHandling

    companion object {
        fun ignore(onDuplicateKeyException: (DuplicateKeyException) -> Unit = DEFAULT_ON_DUPLICATE_KEY_EXCEPTION): DuplicateKeyHandling = Ignore(onDuplicateKeyException)
        fun rethrow(): DuplicateKeyHandling = Rethrow
    }
}

/**
 * Determines how [OptimisticLockingFailureException]'s are handled
 */
sealed interface OptimisticLockingHandling {
    data class Ignore(val onOptimisticLockingFailureException: (OptimisticLockingFailureException) -> Unit = { }) : OptimisticLockingHandling


    data class Retry(
        val initial: Duration = DEFAULT_INITIAL,
        val max: Duration = DEFAULT_MAX,
        val multiplier: Double = DEFAULT_MULTIPLIER,
        val onOptimisticLockingFailureException: (OptimisticLockingFailureException) -> Unit = DEFAULT_ON_ERROR,
    ) : OptimisticLockingHandling

    data object Rethrow : OptimisticLockingHandling

    companion object {
        fun ignore(onOptimisticLockingFailureException: (OptimisticLockingFailureException) -> Unit = { }): OptimisticLockingHandling = Ignore(onOptimisticLockingFailureException)
        fun rethrow(): OptimisticLockingHandling = Rethrow
        fun retry(
            initial: Duration = DEFAULT_INITIAL,
            max: Duration = DEFAULT_MAX,
            multiplier: Double = DEFAULT_MULTIPLIER,
            onError: (Throwable) -> Unit = DEFAULT_ON_ERROR,
        ): OptimisticLockingHandling = Retry(initial, max, multiplier, onError)
    }
}

data class SpringMongoViewConfig(val duplicateKeyHandling: DuplicateKeyHandling, val optimisticLockingHandling: OptimisticLockingHandling) {
    companion object {
        /**
         * Create a default SpringMongoViewConfig configured with:
         * 1. duplicateKeyHandling => ignore DuplicateKeyException
         * 2. optimisticLockingHandler => Retry forever with exponential backoff between 100 ms to 5s
         */
        fun config(
            duplicateKeyHandling: DuplicateKeyHandling = DuplicateKeyHandling.ignore(),
            optimisticLockingHandling: OptimisticLockingHandling = OptimisticLockingHandling.retry()
        ) = SpringMongoViewConfig(duplicateKeyHandling, optimisticLockingHandling)
    }
}