/*
 *
 *  Copyright 2023 Johan Haleby
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

package org.occurrent.example.domain.mastermind


typealias NumberOfGuesses = Int
typealias CurrentNumberOfGuesses = Int

sealed interface MasterMindState {
    data object NotStarted : MasterMindState
    data class Started(
        val gameId: GameId, val timestamp: Timestamp, val codebreakerId: CodebreakerId, val codeMakerId: CodeMakerId, val secretCode: SecretCode,
        val currentNumberOfGuesses: CurrentNumberOfGuesses, val maxNumberOfGuesses: MaxNumberOfGuesses
    ) : MasterMindState

    sealed interface Ended : MasterMindState {
        data class Won(val numberOfGuesses: NumberOfGuesses) : Ended
        data class Lost(val secretCode: SecretCode) : Ended
    }
}