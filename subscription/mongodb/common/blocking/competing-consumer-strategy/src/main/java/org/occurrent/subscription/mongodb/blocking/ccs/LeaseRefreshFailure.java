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

package org.occurrent.subscription.mongodb.blocking.ccs;

import org.jspecify.annotations.NullMarked;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Why the scheduled lease refresh of a MongoDB lease strategy fails, and since when.
 *
 * @param cause The failure of the latest refresh attempt.
 * @param since When the first attempt of this run of failures failed. It stays the same until an attempt refreshes
 *              every lease, so a refresh that keeps failing has one {@code since} and a changing {@code cause}.
 */
@NullMarked
public record LeaseRefreshFailure(Throwable cause, Instant since) {
    public LeaseRefreshFailure {
        requireNonNull(cause, "cause cannot be null");
        requireNonNull(since, "since cannot be null");
    }
}
