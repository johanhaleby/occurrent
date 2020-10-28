package org.occurrent.example.domain.wordguessinggame.support

import org.occurrent.example.domain.wordguessinggame.event.DomainEvent


inline fun <reified T : DomainEvent> List<DomainEvent>.find(): T = first { it is T } as T
inline fun <reified T : DomainEvent> Sequence<DomainEvent>.find(): T = first { it is T } as T