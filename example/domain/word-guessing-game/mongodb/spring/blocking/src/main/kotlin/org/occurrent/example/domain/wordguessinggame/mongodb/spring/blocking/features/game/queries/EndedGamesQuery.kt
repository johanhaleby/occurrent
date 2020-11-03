package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.queries

import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.EndedGameOverviewMongoDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.toDomain
import org.occurrent.example.domain.wordguessinggame.readmodel.EndedGameOverview
import org.springframework.data.domain.Sort
import org.springframework.data.domain.Sort.Direction.DESC
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.stream
import org.springframework.stereotype.Component


@Component
class EndedGamesQuery(private val mongo: MongoOperations) {

    fun execute(numberOfGames: Int): Sequence<EndedGameOverview> =
            mongo.stream<EndedGameOverviewMongoDTO>(Query().with(Sort.by(DESC, "endedAt")).limit(numberOfGames))
                    .asSequence()
                    .map(EndedGameOverviewMongoDTO::toDomain)
}