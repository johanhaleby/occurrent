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

package org.occurrent.example.domain.numberguessinggame.model;

import java.util.Random;

public class SecretNumberToGuess {

    public final int value;

    public SecretNumberToGuess(int value) {
        if (value < 1 || value > 1000) {
            throw new IllegalArgumentException("Value to guess must be between 1 and 1000");
        }
        this.value = value;
    }

    public static SecretNumberToGuess randomBetween(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("min must be < max");
        }
        Random rand = new Random();
        int random = rand.nextInt((max - min)) + min;
        return new SecretNumberToGuess(random);
    }
}