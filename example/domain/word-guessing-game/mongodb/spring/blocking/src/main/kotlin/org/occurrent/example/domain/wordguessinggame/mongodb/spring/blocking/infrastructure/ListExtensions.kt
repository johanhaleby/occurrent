package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure


inline fun <reified T> List<Any>.first(): T = find { it is T } as T