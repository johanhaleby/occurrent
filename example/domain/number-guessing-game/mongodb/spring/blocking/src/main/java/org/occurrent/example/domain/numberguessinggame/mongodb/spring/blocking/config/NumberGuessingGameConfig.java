/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "number-guessing-game")
public class NumberGuessingGameConfig {
    private int minNumberToGuess;
    private int maxNumberToGuess;
    private int maxNumberOfGuesses;

    public int getMinNumberToGuess() {
        return minNumberToGuess;
    }

    void setMinNumberToGuess(int minNumberToGuess) {
        this.minNumberToGuess = minNumberToGuess;
    }

    public int getMaxNumberToGuess() {
        return maxNumberToGuess;
    }

    void setMaxNumberToGuess(int maxNumberToGuess) {
        this.maxNumberToGuess = maxNumberToGuess;
    }

    public int getMaxNumberOfGuesses() {
        return maxNumberOfGuesses;
    }

    void setMaxNumberOfGuesses(int maxNumberOfGuesses) {
        this.maxNumberOfGuesses = maxNumberOfGuesses;
    }
}