package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.slf4j.Logger
import org.slf4j.LoggerFactory

inline fun <reified T : Any> loggerFor(): Logger = LoggerFactory.getLogger(T::class.java)