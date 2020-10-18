package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.queries

import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.OngoingGameOverviewMongoDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.toDomain
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameOverview
import org.springframework.data.domain.Sort
import org.springframework.data.domain.Sort.Direction.DESC
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.stream
import org.springframework.stereotype.Component


@Component
class OngoingGamesQuery(private val mongo: MongoOperations) {

    fun execute(numberOfGames: Int): Sequence<OngoingGameOverview> =
        mongo.stream<OngoingGameOverviewMongoDTO>(Query().with(Sort.by(DESC, "startedAt")).limit(numberOfGames))
                .asSequence()
                .map(OngoingGameOverviewMongoDTO::toDomain)
}