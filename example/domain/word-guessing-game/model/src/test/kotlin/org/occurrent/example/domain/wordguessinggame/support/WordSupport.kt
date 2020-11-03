package org.occurrent.example.domain.wordguessinggame.support

import org.occurrent.example.domain.wordguessinggame.writemodel.Word

fun wordsOf(vararg words: String) = listOf(*words).map(::Word)