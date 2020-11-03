package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy

import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.Policies
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.loggerFor
import org.occurrent.example.domain.wordguessinggame.policy.WhenGameWasWonThenSendEmailToWinnerPolicy
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SendEmailToWinnerPolicy {
    private val log = loggerFor<SendEmailToWinnerPolicy>()

    @Autowired
    lateinit var policies: Policies

    @Bean
    fun whenGameWasWonThenSendEmailToWinner() = policies.newPolicy<GameWasWon>(WhenGameWasWonThenSendEmailToWinnerPolicy::class.simpleName!!) { gameWasWon ->
        log.info("Sending email to player ${gameWasWon.winnerId} since he/she was a winner of game ${gameWasWon.gameId}")
    }
}