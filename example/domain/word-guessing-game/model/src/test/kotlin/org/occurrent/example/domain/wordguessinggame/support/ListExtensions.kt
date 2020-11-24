package org.occurrent.example.domain.wordguessinggame.support

import org.occurrent.example.domain.wordguessinggame.event.GameEvent


inline fun <reified T : GameEvent> List<GameEvent>.find(): T = first { it is T } as T
inline fun <reified T : GameEvent> Sequence<GameEvent>.find(): T = first { it is T } as T